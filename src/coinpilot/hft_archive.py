from __future__ import annotations

import gzip
import hashlib
import json
import math
import os
import random
import re
import stat
import time
import uuid
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from coinpilot.hft_audit_journal import (
    AUDIT_JOURNAL_SCHEMA_VERSION,
    AuditJournalCoordinator,
    DurableAuditJournal,
    HFTAuditJournalError,
    durable_file_sync,
)
from coinpilot.hft_data import (
    MAX_ORDERBOOK_DEPTH,
    UPBIT_PUBLIC_WEBSOCKET_URL,
    HFTDataValidationError,
    normalize_upbit_message,
)


ARCHIVE_SCHEMA_VERSION = 1
DEFAULT_PARTITION_SECONDS = 3_600
_MARKET_PATTERN = re.compile(r"^[A-Z0-9]+-[A-Z0-9]+$")
_CAPTURE_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


class HFTArchiveError(RuntimeError):
    """Raised when a public market-data archive cannot be written safely."""


class HFTArchiveProtocolError(HFTArchiveError):
    """Raised when Upbit explicitly rejects the public WebSocket request."""

    def __init__(self, error_name: str, error_message: str) -> None:
        self.error_name = error_name
        self.error_message = error_message
        super().__init__(
            "Upbit public WebSocket protocol error "
            f"{error_name}: {error_message}"
        )


@dataclass(frozen=True, slots=True)
class HFTArchiveResult:
    capture_id: str
    market: str
    requested_duration_seconds: float
    elapsed_monotonic_seconds: float
    raw_messages: int
    written_events: int
    rejected_messages: int
    connection_count: int
    reconnect_count: int
    gap_count: int
    pings_sent: int
    heartbeats_received: int
    error_counts: dict[str, int]
    data_paths: tuple[Path, ...]
    manifest_paths: tuple[Path, ...]

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["data_paths"] = [str(path) for path in self.data_paths]
        result["manifest_paths"] = [
            str(path) for path in self.manifest_paths
        ]
        return result


@dataclass(frozen=True, slots=True)
class _FinalizedPartition:
    data_path: Path
    manifest_path: Path


def _validate_market(market: str) -> str:
    if not isinstance(market, str):
        raise HFTArchiveError("market must be a string")
    normalized = market.strip().upper()
    if not _MARKET_PATTERN.fullmatch(normalized):
        raise HFTArchiveError(
            "market must use Upbit's safe QUOTE-BASE form, for example KRW-BTC"
        )
    return normalized


def validate_hft_capture_id(capture_id: str | None) -> str:
    """Return a filesystem-safe, non-empty archive capture identifier."""

    result = uuid.uuid4().hex if capture_id is None else capture_id
    if (
        not isinstance(result, str)
        or not _CAPTURE_ID_PATTERN.fullmatch(result)
        or result in {".", ".."}
    ):
        raise HFTArchiveError(
            "capture_id must be a safe 1-128 character identifier"
        )
    return result


def _validate_positive_float(value: Any, name: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise HFTArchiveError(f"{name} must be numeric") from exc
    if not math.isfinite(result) or result <= 0:
        raise HFTArchiveError(f"{name} must be finite and positive")
    return result


def _validate_nonnegative_float(value: Any, name: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise HFTArchiveError(f"{name} must be numeric") from exc
    if not math.isfinite(result) or result < 0:
        raise HFTArchiveError(f"{name} must be finite and non-negative")
    return result


def _validate_positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool):
        raise HFTArchiveError(f"{name} must be an integer")
    try:
        result = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise HFTArchiveError(f"{name} must be an integer") from exc
    if result <= 0 or result != value:
        raise HFTArchiveError(f"{name} must be a positive integer")
    return result


def validate_hft_archive_request(
    *,
    market: str,
    duration_seconds: float,
    max_events: int | None,
    partition_seconds: int,
    max_depth: int,
    capture_id: str,
) -> str:
    """Validate recorder arguments before a durable capture-ID reservation."""

    _validate_market(market)
    _validate_positive_float(duration_seconds, "duration_seconds")
    if max_events is not None:
        _validate_positive_int(max_events, "max_events")
    _validate_positive_int(partition_seconds, "partition_seconds")
    depth = _validate_positive_int(max_depth, "max_depth")
    if depth > MAX_ORDERBOOK_DEPTH:
        raise HFTArchiveError(
            f"max_depth cannot exceed {MAX_ORDERBOOK_DEPTH}"
        )
    return validate_hft_capture_id(capture_id)


def _ensure_safe_descendant(root: Path, path: Path) -> None:
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise HFTArchiveError(
            f"archive path escaped the output root: {path}"
        ) from exc

    current = root
    relative_parent = path.parent.relative_to(root)
    for component in relative_parent.parts:
        current = current / component
        if current.exists() and current.is_symlink():
            raise HFTArchiveError(
                f"refusing to traverse archive symlink: {current}"
            )
    if path.exists() and path.is_symlink():
        raise HFTArchiveError(
            f"refusing to overwrite archive symlink: {path}"
        )


def _secure_mkdirs(root: Path, directory: Path) -> None:
    _ensure_safe_descendant(root, directory / "_sentinel")
    current = root
    for component in directory.relative_to(root).parts:
        current = current / component
        if current.exists():
            if current.is_symlink() or not current.is_dir():
                raise HFTArchiveError(
                    f"archive parent is not a safe directory: {current}"
                )
            continue
        current.mkdir(mode=0o700)
        _fsync_directory(current.parent, required=True)


def _exclusive_binary_file(path: Path) -> Any:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags, 0o600)
    except OSError as exc:
        raise HFTArchiveError(
            f"could not create archive partial file {path}: {exc}"
        ) from exc
    return os.fdopen(descriptor, "wb")


def _fsync_directory(path: Path, *, required: bool = False) -> None:
    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError as exc:
        if required:
            raise HFTArchiveError(
                f"could not open archive directory for fsync {path}: {exc}"
            ) from exc
        return
    try:
        try:
            os.fsync(descriptor)
        except OSError as exc:
            if required:
                raise HFTArchiveError(
                    f"could not fsync archive directory {path}: {exc}"
                ) from exc
    finally:
        os.close(descriptor)


def _sha256_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    total_bytes = 0
    with path.open("rb") as handle:
        while block := handle.read(1024 * 1024):
            digest.update(block)
            total_bytes += len(block)
    return digest.hexdigest(), total_bytes


def _write_json_partial(
    root: Path,
    partial: Path,
    payload: Mapping[str, Any],
) -> None:
    _ensure_safe_descendant(root, partial)
    if partial.exists() or partial.is_symlink():
        raise HFTArchiveError(
            f"manifest partial already exists: {partial}"
        )
    encoded = (
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    handle: Any | None = None
    try:
        handle = _exclusive_binary_file(partial)
        handle.write(encoded)
        handle.flush()
        durable_file_sync(handle.fileno())
        handle.close()
        handle = None
    except BaseException:
        if handle is not None:
            try:
                handle.close()
            except BaseException:
                pass
        partial.unlink(missing_ok=True)
        raise


def _partition_identity(
    received_wall_ns: int,
    partition_seconds: int,
) -> tuple[int, datetime, datetime]:
    partition_ns = partition_seconds * 1_000_000_000
    start_ns = received_wall_ns - (received_wall_ns % partition_ns)
    start_seconds = start_ns // 1_000_000_000
    try:
        start = datetime.fromtimestamp(start_seconds, tz=timezone.utc)
        end = datetime.fromtimestamp(
            start_seconds + partition_seconds,
            tz=timezone.utc,
        )
    except (OverflowError, OSError, ValueError) as exc:
        raise HFTArchiveError(
            "wall clock is outside the supported UTC archive range"
        ) from exc
    return start_ns, start, end


class _PartitionWriter:
    def __init__(
        self,
        *,
        root: Path,
        market: str,
        capture_id: str,
        partition_seconds: int,
        received_wall_ns: int,
        segment: int,
        counter_baseline: Mapping[str, int],
        durable_consumer: bool = False,
    ) -> None:
        start_ns, start, end = _partition_identity(
            received_wall_ns,
            partition_seconds,
        )
        self.root = root
        self.market = market
        self.capture_id = capture_id
        self.partition_start_ns = start_ns
        self.partition_start = start
        self.partition_end = end
        self.counter_baseline = dict(counter_baseline)

        directory = (
            root
            / f"market={market}"
            / f"date={start:%Y-%m-%d}"
            / f"hour={start:%H}"
        )
        _secure_mkdirs(root, directory)
        stem = (
            f"{capture_id}-{start:%Y%m%dT%H%M%SZ}"
            f"-segment-{segment:04d}"
        )
        self.data_path = directory / f"{stem}.jsonl.gz"
        self.partial_path = directory / f"{stem}.jsonl.gz.partial"
        self.manifest_path = directory / f"{stem}.manifest.json"
        for path in (
            self.data_path,
            self.partial_path,
            self.manifest_path,
            self.manifest_path.with_name(self.manifest_path.name + ".partial"),
        ):
            _ensure_safe_descendant(root, path)
            if path.exists() or path.is_symlink():
                raise HFTArchiveError(
                    f"refusing to overwrite archive path: {path}"
                )

        self._raw_handle = _exclusive_binary_file(self.partial_path)
        self._gzip_handle = gzip.GzipFile(
            filename="",
            mode="wb",
            fileobj=self._raw_handle,
            mtime=0,
        )
        self._audit_journal: DurableAuditJournal | None = None
        if durable_consumer:
            try:
                self._audit_journal = DurableAuditJournal.create(
                    root=root,
                    market=market,
                    capture_id=capture_id,
                    partition_start_ns=start_ns,
                    partition_start_utc=(
                        start.isoformat().replace("+00:00", "Z")
                    ),
                    partition_end_utc_exclusive=(
                        end.isoformat().replace("+00:00", "Z")
                    ),
                    segment=segment,
                    data_path=self.data_path,
                    partial_data_path=self.partial_path,
                    manifest_path=self.manifest_path,
                )
            except BaseException:
                try:
                    self._gzip_handle.close()
                finally:
                    self._raw_handle.close()
                self.partial_path.unlink(missing_ok=True)
                raise
        self._closed = False
        self._tainted = False
        self.records = 0
        self.first_ordinal: int | None = None
        self.last_ordinal: int | None = None
        self.first_wall_ns: str | None = None
        self.last_wall_ns: str | None = None
        self.first_monotonic_ns: str | None = None
        self.last_monotonic_ns: str | None = None
        self.connection_ids: set[str] = set()
        self.gap_count = 0
        self.event_type_counts = {"orderbook": 0, "trade": 0}

    def contains(self, received_wall_ns: int) -> bool:
        return (
            self.partition_start_ns
            <= received_wall_ns
            < self.partition_start_ns
            + (
                int(
                    (self.partition_end - self.partition_start).total_seconds()
                )
                * 1_000_000_000
            )
        )

    def write(self, envelope: Mapping[str, Any]) -> None:
        if self._closed:
            raise HFTArchiveError("cannot write to a closed archive partition")
        if self._tainted:
            raise HFTArchiveError(
                "cannot write to a tainted archive partition"
            )
        try:
            encoded = (
                json.dumps(
                    envelope,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                )
                + "\n"
            ).encode("utf-8")
            self._gzip_handle.write(encoded)
            if self._audit_journal is not None:
                # Keep gzip buffered for throughput, but make the exact source
                # event recoverable before the shadow consumer can commit it.
                self._audit_journal.append_durable(envelope)
            ordinal = int(envelope["ordinal"])
            wall_ns = str(envelope["received_wall_ns"])
            monotonic_ns = str(envelope["received_monotonic_ns"])
            if self.records == 0:
                self.first_ordinal = ordinal
                self.first_wall_ns = wall_ns
                self.first_monotonic_ns = monotonic_ns
            self.records += 1
            self.last_ordinal = ordinal
            self.last_wall_ns = wall_ns
            self.last_monotonic_ns = monotonic_ns
            self.connection_ids.add(str(envelope["connection_id"]))
            if bool(envelope["gap_before"]):
                self.gap_count += 1
            event_type = str(envelope["event"]["event_type"])
            self.event_type_counts[event_type] = (
                self.event_type_counts.get(event_type, 0) + 1
            )
        except BaseException as exc:
            # A signal can interrupt gzip.write after it emitted only part of
            # a JSON line.  Never finalize that partition as healthy.
            self._tainted = True
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            raise HFTArchiveError(
                f"failed to write archive partition: {exc}"
            ) from exc

    def _close_partial(self) -> None:
        if self._closed:
            return
        try:
            self._gzip_handle.close()
            self._raw_handle.flush()
            durable_file_sync(self._raw_handle.fileno())
        finally:
            self._raw_handle.close()
            self._closed = True

    def finalize(
        self,
        *,
        counters: Mapping[str, int],
    ) -> _FinalizedPartition:
        if self._tainted:
            self.abort()
            raise HFTArchiveError(
                "cannot finalize a tainted archive partition"
            )
        if self.records <= 0:
            self.abort()
            raise HFTArchiveError("cannot finalize an empty archive partition")
        manifest_partial = self.manifest_path.with_name(
            self.manifest_path.name + ".partial"
        )
        data_renamed = False
        manifest_renamed = False
        data_promotion_attempted = False
        manifest_promotion_attempted = False
        try:
            self._close_partial()
            digest, compressed_bytes = _sha256_file(self.partial_path)
            counter_deltas = {
                name: max(
                    0,
                    int(value)
                    - int(self.counter_baseline.get(name, 0)),
                )
                for name, value in counters.items()
            }
            relative_data_path = self.data_path.relative_to(
                self.root
            ).as_posix()
            manifest = {
                "schema_version": ARCHIVE_SCHEMA_VERSION,
                "capture_id": self.capture_id,
                "market": self.market,
                "partition_start_utc": (
                    self.partition_start.isoformat().replace("+00:00", "Z")
                ),
                "partition_end_utc_exclusive": (
                    self.partition_end.isoformat().replace("+00:00", "Z")
                ),
                "data_file": relative_data_path,
                "compression": "gzip",
                "sha256": digest,
                "sha256_scope": "compressed_file_bytes",
                "compressed_bytes": compressed_bytes,
                "records": self.records,
                "first_ordinal": self.first_ordinal,
                "last_ordinal": self.last_ordinal,
                "first_received_wall_ns": self.first_wall_ns,
                "last_received_wall_ns": self.last_wall_ns,
                "first_received_monotonic_ns": self.first_monotonic_ns,
                "last_received_monotonic_ns": self.last_monotonic_ns,
                "connection_ids": sorted(self.connection_ids),
                "connection_count": len(self.connection_ids),
                "gap_count": self.gap_count,
                "event_type_counts": dict(
                    sorted(self.event_type_counts.items())
                ),
                "counter_deltas": dict(sorted(counter_deltas.items())),
                "counters_cumulative": {
                    name: int(value)
                    for name, value in sorted(counters.items())
                },
                "audit_journal_schema_version": (
                    AUDIT_JOURNAL_SCHEMA_VERSION
                    if self._audit_journal is not None
                    else None
                ),
                "audit_durability": (
                    "fsync_before_consumer"
                    if self._audit_journal is not None
                    else "not_applicable_no_consumer"
                ),
            }
            _write_json_partial(self.root, manifest_partial, manifest)
            # The data filename is the bundle commit point: promote the
            # manifest first so a crash can never expose final data without
            # its verifier.
            manifest_promotion_attempted = True
            os.replace(manifest_partial, self.manifest_path)
            manifest_renamed = True
            _fsync_directory(self.data_path.parent, required=True)
            data_promotion_attempted = True
            os.replace(self.partial_path, self.data_path)
            data_renamed = True
            _fsync_directory(self.data_path.parent, required=True)
        except BaseException:
            self._tainted = True
            data_owned = data_renamed or (
                data_promotion_attempted
                and not self.partial_path.exists()
                and self.data_path.is_file()
                and not self.data_path.is_symlink()
            )
            manifest_owned = manifest_renamed or (
                manifest_promotion_attempted
                and not manifest_partial.exists()
                and self.manifest_path.is_file()
                and not self.manifest_path.is_symlink()
            )
            manifest_partial.unlink(missing_ok=True)
            self.partial_path.unlink(missing_ok=True)
            if data_owned:
                self.data_path.unlink(missing_ok=True)
            if manifest_owned:
                self.manifest_path.unlink(missing_ok=True)
            _fsync_directory(self.data_path.parent)
            raise
        if self._audit_journal is not None:
            # If cleanup loses a race with a power failure, startup verifies
            # this committed bundle and removes the redundant WAL.
            self._audit_journal.discard_after_archive_commit()
        return _FinalizedPartition(
            data_path=self.data_path,
            manifest_path=self.manifest_path,
        )

    def abort(self) -> None:
        if not self._closed:
            try:
                self._gzip_handle.close()
            except Exception:
                pass
            try:
                self._raw_handle.close()
            except Exception:
                pass
            self._closed = True
        if self._audit_journal is not None:
            self._audit_journal.close_preserving()
        self.partial_path.unlink(missing_ok=True)
        self.manifest_path.with_name(
            self.manifest_path.name + ".partial"
        ).unlink(missing_ok=True)


def cleanup_stale_partials(
    output_root: str | Path,
    *,
    older_than_seconds: float = 86_400.0,
    now_seconds: Callable[[], float] = time.time,
) -> tuple[Path, ...]:
    """Delete old regular ``*.partial`` files without following symlinks."""

    age = _validate_nonnegative_float(
        older_than_seconds,
        "older_than_seconds",
    )
    root = Path(output_root).expanduser()
    if not root.exists():
        return ()
    if root.is_symlink() or not root.is_dir():
        raise HFTArchiveError("output_root must be a non-symlink directory")
    resolved_root = root.resolve()
    cutoff = now_seconds() - age
    removed: list[Path] = []
    for candidate in resolved_root.rglob("*.partial"):
        try:
            metadata = candidate.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
            continue
        _ensure_safe_descendant(resolved_root, candidate)
        if metadata.st_mtime <= cutoff:
            candidate.unlink(missing_ok=True)
            removed.append(candidate)
    return tuple(sorted(removed))


def _default_socket_components() -> tuple[
    Callable[..., Any],
    tuple[type[BaseException], ...],
]:
    try:
        import websocket  # type: ignore[import-not-found]
    except ImportError as exc:
        raise HFTArchiveError(
            "continuous capture requires the optional 'websocket-client' package"
        ) from exc
    return websocket.create_connection, (
        TimeoutError,
        websocket.WebSocketTimeoutException,
    )


def _json_message_object(message: Any) -> Mapping[str, Any] | None:
    candidate = message
    if isinstance(candidate, Mapping):
        return candidate
    if isinstance(candidate, bytes):
        try:
            candidate = candidate.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if not isinstance(candidate, str):
        return None
    try:
        payload = json.loads(candidate)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, Mapping) else None


def _upbit_protocol_error(message: Any) -> tuple[str, str] | None:
    payload = _json_message_object(message)
    if payload is None or "error" not in payload:
        return None
    error = payload.get("error")
    if isinstance(error, Mapping):
        raw_name = error.get("name")
        raw_message = error.get("message")
        name = (
            str(raw_name).strip()
            if raw_name is not None and str(raw_name).strip()
            else "UNKNOWN_ERROR"
        )
        detail = (
            str(raw_message).strip()
            if raw_message is not None and str(raw_message).strip()
            else "Upbit rejected the WebSocket request"
        )
    else:
        name = "UNKNOWN_ERROR"
        detail = (
            str(error).strip()
            if error is not None and str(error).strip()
            else "Upbit returned a malformed protocol error"
        )
    return name[:128], detail[:500]


def _is_heartbeat(message: Any) -> bool:
    if message == "PONG" or message == b"PONG":
        return True
    payload = _json_message_object(message)
    return payload is not None and payload.get("status") == "UP"


class UpbitHFTArchiveRecorder:
    """Reconnect-capable, public-only Upbit event recorder."""

    def __init__(
        self,
        *,
        output_root: str | Path,
        partition_seconds: int = DEFAULT_PARTITION_SECONDS,
        max_depth: int = MAX_ORDERBOOK_DEPTH,
        socket_factory: Callable[..., Any] | None = None,
        timeout_exceptions: tuple[type[BaseException], ...] | None = None,
        wall_time_ns: Callable[[], int] = time.time_ns,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
        sleep: Callable[[float], None] = time.sleep,
        random_float: Callable[[], float] = random.random,
        socket_timeout_seconds: float = 1.0,
        idle_ping_seconds: float = 15.0,
        idle_reconnect_seconds: float = 45.0,
        gap_threshold_seconds: float = 2.0,
        backoff_initial_seconds: float = 0.25,
        backoff_cap_seconds: float = 10.0,
        backoff_jitter_ratio: float = 0.20,
    ) -> None:
        root_input = Path(output_root).expanduser()
        if root_input.is_symlink() or (
            root_input.exists() and not root_input.is_dir()
        ):
            raise HFTArchiveError(
                "output_root must be a non-symlink directory"
            )
        root_input.mkdir(parents=True, exist_ok=True, mode=0o700)
        if root_input.is_symlink():
            raise HFTArchiveError(
                "output_root must not be a symlink"
            )
        self.output_root = root_input.resolve()
        self.partition_seconds = _validate_positive_int(
            partition_seconds,
            "partition_seconds",
        )
        self.max_depth = _validate_positive_int(max_depth, "max_depth")
        if self.max_depth > MAX_ORDERBOOK_DEPTH:
            raise HFTArchiveError(
                f"max_depth cannot exceed {MAX_ORDERBOOK_DEPTH}"
            )

        if socket_factory is None:
            socket_factory, default_timeout_exceptions = (
                _default_socket_components()
            )
            if timeout_exceptions is None:
                timeout_exceptions = default_timeout_exceptions
        self.socket_factory = socket_factory
        self.timeout_exceptions = timeout_exceptions or (TimeoutError,)
        self.wall_time_ns = wall_time_ns
        self.monotonic_ns = monotonic_ns
        self.sleep = sleep
        self.random_float = random_float
        self.socket_timeout_seconds = _validate_positive_float(
            socket_timeout_seconds,
            "socket_timeout_seconds",
        )
        self.idle_ping_seconds = _validate_positive_float(
            idle_ping_seconds,
            "idle_ping_seconds",
        )
        self.idle_reconnect_seconds = _validate_positive_float(
            idle_reconnect_seconds,
            "idle_reconnect_seconds",
        )
        if self.idle_reconnect_seconds <= self.idle_ping_seconds:
            raise HFTArchiveError(
                "idle_reconnect_seconds must exceed idle_ping_seconds"
            )
        self.gap_threshold_ns = int(
            _validate_positive_float(
                gap_threshold_seconds,
                "gap_threshold_seconds",
            )
            * 1_000_000_000
        )
        self.backoff_initial_seconds = _validate_positive_float(
            backoff_initial_seconds,
            "backoff_initial_seconds",
        )
        self.backoff_cap_seconds = _validate_positive_float(
            backoff_cap_seconds,
            "backoff_cap_seconds",
        )
        if self.backoff_cap_seconds < self.backoff_initial_seconds:
            raise HFTArchiveError(
                "backoff_cap_seconds cannot be below the initial delay"
            )
        self.backoff_jitter_ratio = _validate_nonnegative_float(
            backoff_jitter_ratio,
            "backoff_jitter_ratio",
        )
        if self.backoff_jitter_ratio > 1:
            raise HFTArchiveError(
                "backoff_jitter_ratio cannot exceed 1"
            )

    def _subscription(self, market: str, capture_id: str) -> str:
        request = [
            {"ticket": f"coinpilot-archive-{capture_id}"},
            {
                "type": "orderbook",
                "codes": [market],
                "is_only_realtime": True,
            },
            {
                "type": "trade",
                "codes": [market],
                "is_only_realtime": True,
            },
            {"format": "DEFAULT"},
        ]
        return json.dumps(
            request,
            ensure_ascii=False,
            separators=(",", ":"),
        )

    def _backoff_delay(self, failures: int) -> float:
        exponent = max(0, min(failures - 1, 30))
        base = min(
            self.backoff_cap_seconds,
            self.backoff_initial_seconds * (2**exponent),
        )
        random_value = float(self.random_float())
        if not math.isfinite(random_value):
            random_value = 0.5
        random_value = min(1.0, max(0.0, random_value))
        factor = 1.0 + self.backoff_jitter_ratio * (
            2.0 * random_value - 1.0
        )
        return min(self.backoff_cap_seconds, max(0.0, base * factor))

    def _bounded_sleep(self, delay: float, deadline_ns: int) -> None:
        remaining = max(0.0, (deadline_ns - self.monotonic_ns()) / 1e9)
        if remaining > 0:
            self.sleep(min(delay, remaining))

    @staticmethod
    def _close_socket(connection: Any | None) -> None:
        if connection is None:
            return
        try:
            connection.close()
        except Exception:
            pass

    @staticmethod
    def _ping(connection: Any) -> None:
        # Upbit documents the application-level "PING" text message and
        # responds with {"status":"UP"}.  Fall back to a WebSocket control
        # ping only for clients without a normal send method.
        if hasattr(connection, "send"):
            connection.send("PING")
        else:
            connection.ping()

    def record(
        self,
        *,
        market: str,
        duration_seconds: float,
        max_events: int | None = None,
        capture_id: str | None = None,
        stop_requested: Callable[[], bool] | None = None,
        on_record: Callable[[Mapping[str, Any]], None] | None = None,
    ) -> HFTArchiveResult:
        normalized_market = _validate_market(market)
        duration = _validate_positive_float(
            duration_seconds,
            "duration_seconds",
        )
        maximum_events = (
            None
            if max_events is None
            else _validate_positive_int(max_events, "max_events")
        )
        identifier = validate_hft_capture_id(capture_id)

        audit_coordinator: AuditJournalCoordinator | None = None
        if on_record is not None:
            try:
                audit_coordinator = AuditJournalCoordinator(self.output_root)
                audit_coordinator.acquire()
                audit_coordinator.recover()
            except (HFTAuditJournalError, OSError) as exc:
                if audit_coordinator is not None:
                    audit_coordinator.release()
                raise HFTArchiveError(
                    f"could not establish durable consumer audit: {exc}"
                ) from exc

        try:
            start_ns = int(self.monotonic_ns())
            if start_ns < 0:
                raise HFTArchiveError("monotonic clock cannot be negative")
        except BaseException:
            if audit_coordinator is not None:
                audit_coordinator.release()
            raise
        duration_ns = int(duration * 1_000_000_000)
        deadline_ns = start_ns + duration_ns
        counters: dict[str, int] = {
            "connection_errors": 0,
            "receive_errors": 0,
            "normalization_errors": 0,
            "protocol_errors": 0,
            "idle_reconnects": 0,
            "write_errors": 0,
            "consumer_errors": 0,
            "monotonic_regressions": 0,
            "wall_clock_regressions": 0,
            "reconnects": 0,
            "gaps": 0,
            "pings_sent": 0,
            "heartbeats_received": 0,
        }
        raw_messages = 0
        written_events = 0
        rejected_messages = 0
        connection_count = 0
        consecutive_failures = 0
        pending_gap_reason: str | None = None
        connection: Any | None = None
        connection_id: str | None = None
        last_activity_ns: int | None = None
        last_ping_ns: int | None = None
        last_event_monotonic_ns: int | None = None
        last_event_wall_ns: int | None = None
        active_writer: _PartitionWriter | None = None
        segment = 0
        finalized: list[_FinalizedPartition] = []

        def finalize_active() -> None:
            nonlocal active_writer
            if active_writer is None:
                return
            finalized.append(active_writer.finalize(counters=counters))
            active_writer = None

        try:
            while self.monotonic_ns() < deadline_ns:
                if stop_requested is not None and stop_requested():
                    break
                if maximum_events is not None and written_events >= maximum_events:
                    break

                if connection is None:
                    try:
                        remaining_seconds = max(
                            0.001,
                            (deadline_ns - self.monotonic_ns()) / 1e9,
                        )
                        connection = self.socket_factory(
                            UPBIT_PUBLIC_WEBSOCKET_URL,
                            timeout=min(5.0, remaining_seconds),
                        )
                        connection.send(
                            self._subscription(normalized_market, identifier)
                        )
                        connection_count += 1
                        if connection_count > 1:
                            counters["reconnects"] += 1
                            if last_event_monotonic_ns is not None:
                                pending_gap_reason = (
                                    pending_gap_reason or "reconnect"
                                )
                        connection_id = (
                            f"{identifier}-connection-{connection_count:06d}"
                        )
                        last_activity_ns = int(self.monotonic_ns())
                        last_ping_ns = None
                    except Exception:
                        counters["connection_errors"] += 1
                        consecutive_failures += 1
                        self._close_socket(connection)
                        connection = None
                        self._bounded_sleep(
                            self._backoff_delay(consecutive_failures),
                            deadline_ns,
                        )
                        continue

                now_ns = int(self.monotonic_ns())
                remaining_seconds = (deadline_ns - now_ns) / 1e9
                if remaining_seconds <= 0:
                    break
                if hasattr(connection, "settimeout"):
                    connection.settimeout(
                        max(
                            0.001,
                            min(
                                self.socket_timeout_seconds,
                                remaining_seconds,
                            ),
                        )
                    )
                try:
                    message = connection.recv()
                except self.timeout_exceptions:
                    timeout_now_ns = int(self.monotonic_ns())
                    idle_ns = timeout_now_ns - int(
                        last_activity_ns
                        if last_activity_ns is not None
                        else timeout_now_ns
                    )
                    if idle_ns >= int(self.idle_reconnect_seconds * 1e9):
                        counters["idle_reconnects"] += 1
                        consecutive_failures += 1
                        pending_gap_reason = (
                            pending_gap_reason or "idle_reconnect"
                        )
                        self._close_socket(connection)
                        connection = None
                        self._bounded_sleep(
                            self._backoff_delay(consecutive_failures),
                            deadline_ns,
                        )
                        continue
                    if (
                        idle_ns >= int(self.idle_ping_seconds * 1e9)
                        and (
                            last_ping_ns is None
                            or timeout_now_ns - last_ping_ns
                            >= int(self.idle_ping_seconds * 1e9)
                        )
                    ):
                        try:
                            self._ping(connection)
                            counters["pings_sent"] += 1
                            last_ping_ns = timeout_now_ns
                        except Exception:
                            counters["receive_errors"] += 1
                            consecutive_failures += 1
                            pending_gap_reason = (
                                pending_gap_reason or "ping_error"
                            )
                            self._close_socket(connection)
                            connection = None
                            self._bounded_sleep(
                                self._backoff_delay(consecutive_failures),
                                deadline_ns,
                            )
                    continue
                except Exception:
                    counters["receive_errors"] += 1
                    consecutive_failures += 1
                    pending_gap_reason = (
                        pending_gap_reason or "receive_error"
                    )
                    self._close_socket(connection)
                    connection = None
                    self._bounded_sleep(
                        self._backoff_delay(consecutive_failures),
                        deadline_ns,
                    )
                    continue

                receive_monotonic_ns = int(self.monotonic_ns())
                receive_wall_ns = int(self.wall_time_ns())
                if receive_monotonic_ns < 0 or receive_wall_ns <= 0:
                    raise HFTArchiveError(
                        "capture clocks returned invalid nanosecond values"
                    )
                last_activity_ns = receive_monotonic_ns
                last_ping_ns = None
                raw_messages += 1

                if message is None or message == "" or message == b"":
                    counters["receive_errors"] += 1
                    consecutive_failures += 1
                    pending_gap_reason = (
                        pending_gap_reason or "connection_closed"
                    )
                    self._close_socket(connection)
                    connection = None
                    self._bounded_sleep(
                        self._backoff_delay(consecutive_failures),
                        deadline_ns,
                    )
                    continue
                protocol_error = _upbit_protocol_error(message)
                if protocol_error is not None:
                    counters["protocol_errors"] += 1
                    # Every previously written record is healthy at this point;
                    # preserve it before surfacing the terminal subscription
                    # error to the caller.
                    finalize_active()
                    raise HFTArchiveProtocolError(*protocol_error)
                if _is_heartbeat(message):
                    counters["heartbeats_received"] += 1
                    continue

                try:
                    event = normalize_upbit_message(
                        message,
                        received_at_ns=receive_wall_ns,
                        max_depth=self.max_depth,
                        expected_market=normalized_market,
                    )
                except HFTDataValidationError:
                    counters["normalization_errors"] += 1
                    rejected_messages += 1
                    pending_gap_reason = (
                        pending_gap_reason or "normalization_error"
                    )
                    continue

                delta_ns: int | None = None
                monotonic_regression = False
                gap_before = False
                gap_reason: str | None = None
                gap_duration_ns: int | None = None
                if last_event_monotonic_ns is not None:
                    delta_ns = receive_monotonic_ns - last_event_monotonic_ns
                    if delta_ns < 0:
                        counters["monotonic_regressions"] += 1
                        monotonic_regression = True
                if pending_gap_reason is not None:
                    gap_before = True
                    gap_reason = pending_gap_reason
                    gap_duration_ns = delta_ns
                elif delta_ns is not None and delta_ns > self.gap_threshold_ns:
                    gap_before = True
                    gap_reason = "receive_interval"
                    gap_duration_ns = delta_ns
                if (
                    last_event_wall_ns is not None
                    and receive_wall_ns < last_event_wall_ns
                ):
                    counters["wall_clock_regressions"] += 1
                if gap_before:
                    counters["gaps"] += 1
                pending_gap_reason = None

                ordinal = written_events + 1
                if connection_id is None:
                    raise HFTArchiveError(
                        "internal error: event has no connection identifier"
                    )
                envelope = {
                    "schema_version": ARCHIVE_SCHEMA_VERSION,
                    "capture_id": identifier,
                    "connection_id": connection_id,
                    "ordinal": ordinal,
                    "received_wall_ns": str(receive_wall_ns),
                    "received_monotonic_ns": str(receive_monotonic_ns),
                    "receive_delta_monotonic_ns": (
                        None if delta_ns is None else str(delta_ns)
                    ),
                    "monotonic_regression": monotonic_regression,
                    "gap_before": gap_before,
                    "gap_reason": gap_reason,
                    "gap_duration_monotonic_ns": (
                        None
                        if gap_duration_ns is None
                        else str(gap_duration_ns)
                    ),
                    "event": event,
                }

                try:
                    if (
                        active_writer is None
                        or not active_writer.contains(receive_wall_ns)
                    ):
                        finalize_active()
                        segment += 1
                        active_writer = _PartitionWriter(
                            root=self.output_root,
                            market=normalized_market,
                            capture_id=identifier,
                            partition_seconds=self.partition_seconds,
                            received_wall_ns=receive_wall_ns,
                            segment=segment,
                            counter_baseline=counters,
                            durable_consumer=on_record is not None,
                        )
                    active_writer.write(envelope)
                except Exception:
                    counters["write_errors"] += 1
                    raise

                if on_record is not None:
                    try:
                        on_record(envelope)
                    except Exception as exc:
                        # Preserve every public record already acknowledged by
                        # the archive before stopping the shadow consumer.  A
                        # normal outer exception path would abort the whole
                        # active partition and leave ledger rows without their
                        # audit source.
                        counters["consumer_errors"] += 1
                        finalize_active()
                        raise HFTArchiveError(
                            "archive event consumer failed; capture stopped"
                        ) from exc

                written_events += 1
                last_event_monotonic_ns = receive_monotonic_ns
                last_event_wall_ns = receive_wall_ns
                consecutive_failures = 0

            finalize_active()
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                if active_writer is not None:
                    try:
                        finalize_active()
                    except BaseException:
                        if active_writer is not None:
                            active_writer.abort()
                raise
            if active_writer is not None:
                active_writer.abort()
            if isinstance(exc, HFTArchiveError):
                raise
            if isinstance(exc, Exception):
                raise HFTArchiveError(
                    f"archive recorder failed: {exc}"
                ) from exc
            raise
        finally:
            self._close_socket(connection)
            if audit_coordinator is not None:
                audit_coordinator.release()

        end_ns = int(self.monotonic_ns())
        return HFTArchiveResult(
            capture_id=identifier,
            market=normalized_market,
            requested_duration_seconds=duration,
            elapsed_monotonic_seconds=max(0.0, (end_ns - start_ns) / 1e9),
            raw_messages=raw_messages,
            written_events=written_events,
            rejected_messages=rejected_messages,
            connection_count=connection_count,
            reconnect_count=counters["reconnects"],
            gap_count=counters["gaps"],
            pings_sent=counters["pings_sent"],
            heartbeats_received=counters["heartbeats_received"],
            error_counts={
                name: value
                for name, value in sorted(counters.items())
                if name
                not in {
                    "reconnects",
                    "gaps",
                    "pings_sent",
                    "heartbeats_received",
                }
            },
            data_paths=tuple(item.data_path for item in finalized),
            manifest_paths=tuple(item.manifest_path for item in finalized),
        )


def record_upbit_public_archive(
    *,
    market: str,
    duration_seconds: float,
    output_root: str | Path,
    max_events: int | None = None,
    partition_seconds: int = DEFAULT_PARTITION_SECONDS,
    max_depth: int = MAX_ORDERBOOK_DEPTH,
    capture_id: str | None = None,
    stop_requested: Callable[[], bool] | None = None,
    on_record: Callable[[Mapping[str, Any]], None] | None = None,
) -> HFTArchiveResult:
    """Convenience entry point using the real public Upbit WebSocket."""

    recorder = UpbitHFTArchiveRecorder(
        output_root=output_root,
        partition_seconds=partition_seconds,
        max_depth=max_depth,
    )
    return recorder.record(
        market=market,
        duration_seconds=duration_seconds,
        max_events=max_events,
        capture_id=capture_id,
        stop_requested=stop_requested,
        on_record=on_record,
    )
