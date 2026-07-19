"""Crash-recoverable audit journal for the HFT public-feed archive.

The gzip archive is intentionally buffered for throughput.  Before an event is
handed to a shadow consumer, this module appends the exact normalized envelope
to a separate WAL and fsyncs it.  A hard crash can therefore lose the buffered
gzip tail, but it cannot leave a durable shadow-ledger row without a recoverable
public-data source record.
"""

from __future__ import annotations

import fcntl
import gzip
import hashlib
import json
import os
import stat
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


AUDIT_JOURNAL_SCHEMA_VERSION = 1
_JOURNAL_DIRECTORY = ".audit-journal"
_QUARANTINE_DIRECTORY = ".audit-quarantine"
_HEADER_KIND = "coinpilot_hft_audit_header"
_RECORD_KIND = "coinpilot_hft_audit_record"


class HFTAuditJournalError(RuntimeError):
    """Raised when the durable audit boundary cannot be guaranteed."""


class _EmptyAuditJournal(HFTAuditJournalError):
    """A valid header with no consumer-visible durable records."""


@dataclass(frozen=True, slots=True)
class AuditRecoveryResult:
    journal_path: Path
    disposition: str
    records: int
    data_path: Path | None
    manifest_path: Path | None
    quarantined_path: Path | None = None


def _canonical_json(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise HFTAuditJournalError(
            "audit journal payload must be strict JSON"
        ) from exc


def _write_all(descriptor: int, payload: bytes) -> None:
    view = memoryview(payload)
    while view:
        try:
            written = os.write(descriptor, view)
        except OSError as exc:
            raise HFTAuditJournalError(
                f"could not append audit journal: {exc}"
            ) from exc
        if written <= 0:
            raise HFTAuditJournalError(
                "audit journal append made no forward progress"
            )
        view = view[written:]


def durable_file_sync(descriptor: int) -> None:
    """Request a power-loss durability barrier on the current platform."""

    full_fsync = getattr(fcntl, "F_FULLFSYNC", None)
    if full_fsync is not None:
        # macOS fsync may stop at the drive cache.  F_FULLFSYNC asks the
        # device to report completion only after its own durable flush.
        fcntl.fcntl(descriptor, full_fsync)
    else:
        os.fsync(descriptor)


def _fsync_directory(path: Path) -> None:
    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError as exc:
        raise HFTAuditJournalError(
            f"could not open audit directory for fsync {path}: {exc}"
        ) from exc
    try:
        os.fsync(descriptor)
    except OSError as exc:
        raise HFTAuditJournalError(
            f"could not fsync audit directory {path}: {exc}"
        ) from exc
    finally:
        os.close(descriptor)


def _ensure_directory(path: Path) -> None:
    if path.exists():
        if path.is_symlink() or not path.is_dir():
            raise HFTAuditJournalError(
                f"audit path must be a non-symlink directory: {path}"
            )
        return
    path.mkdir(mode=0o700)
    _fsync_directory(path.parent)


def _ensure_durable_descendant_directory(root: Path, directory: Path) -> None:
    try:
        relative = directory.relative_to(root)
    except ValueError as exc:
        raise HFTAuditJournalError(
            f"audit recovery directory escaped its root: {directory}"
        ) from exc
    current = root
    for component in relative.parts:
        parent = current
        current = current / component
        if current.exists():
            if current.is_symlink() or not current.is_dir():
                raise HFTAuditJournalError(
                    f"unsafe audit recovery directory: {current}"
                )
            continue
        current.mkdir(mode=0o700)
        _fsync_directory(parent)


def _safe_relative(root: Path, path: Path) -> str:
    try:
        relative = path.relative_to(root)
    except ValueError as exc:
        raise HFTAuditJournalError(
            f"audit archive path escaped its root: {path}"
        ) from exc
    current = root
    for component in relative.parts[:-1]:
        current = current / component
        if current.is_symlink():
            raise HFTAuditJournalError(
                f"audit archive path traverses a symlink: {current}"
            )
    if path.is_symlink():
        raise HFTAuditJournalError(
            f"audit archive target is a symlink: {path}"
        )
    return relative.as_posix()


def _path_from_header(root: Path, value: Any, field: str) -> Path:
    if not isinstance(value, str) or not value or value.startswith("/"):
        raise HFTAuditJournalError(
            f"audit journal {field} must be a relative path"
        )
    parts = Path(value).parts
    if any(part in ("", ".", "..") for part in parts):
        raise HFTAuditJournalError(
            f"audit journal {field} contains an unsafe component"
        )
    candidate = root.joinpath(*parts)
    _safe_relative(root, candidate)
    return candidate


class AuditJournalCoordinator:
    """Hold the single-recorder lock while recovering and recording."""

    def __init__(self, output_root: str | Path) -> None:
        root = Path(output_root).expanduser()
        if root.is_symlink() or not root.is_dir():
            raise HFTAuditJournalError(
                "audit output root must be a non-symlink directory"
            )
        self.root = root.resolve()
        self.directory = self.root / _JOURNAL_DIRECTORY
        _ensure_directory(self.directory)
        self._descriptor: int | None = None

    def acquire(self) -> None:
        if self._descriptor is not None:
            raise HFTAuditJournalError("audit coordinator is already locked")
        lock_path = self.directory / "recorder.lock"
        flags = os.O_RDWR | os.O_CREAT
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor: int | None = None
        try:
            descriptor = os.open(lock_path, flags, 0o600)
            os.chmod(lock_path, 0o600)
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if descriptor is not None:
                os.close(descriptor)
            raise HFTAuditJournalError(
                "another archive recorder holds the audit-journal lock"
            ) from exc
        self._descriptor = descriptor

    def release(self) -> None:
        if self._descriptor is None:
            return
        descriptor = self._descriptor
        self._descriptor = None
        try:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            except OSError:
                pass
        finally:
            try:
                os.close(descriptor)
            except OSError:
                pass

    def recover(self) -> tuple[AuditRecoveryResult, ...]:
        if self._descriptor is None:
            raise HFTAuditJournalError(
                "audit recovery requires the recorder lock"
            )
        results: list[AuditRecoveryResult] = []
        for journal_path in sorted(self.directory.glob("*.audit.wal")):
            metadata = journal_path.lstat()
            if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(
                metadata.st_mode
            ):
                raise HFTAuditJournalError(
                    f"unsafe audit journal entry: {journal_path}"
                )
            results.append(_recover_one(self.root, journal_path))
        return tuple(results)


class DurableAuditJournal:
    """Append and fsync one exact event before consumer acknowledgement."""

    def __init__(
        self,
        *,
        root: Path,
        path: Path,
        descriptor: int,
        header: Mapping[str, Any],
    ) -> None:
        self.root = root
        self.path = path
        self._descriptor: int | None = descriptor
        self.header = dict(header)

    @classmethod
    def create(
        cls,
        *,
        root: str | Path,
        market: str,
        capture_id: str,
        partition_start_ns: int,
        partition_start_utc: str,
        partition_end_utc_exclusive: str,
        segment: int,
        data_path: Path,
        partial_data_path: Path,
        manifest_path: Path,
    ) -> DurableAuditJournal:
        selected_root = Path(root).resolve()
        data_path = data_path.resolve(strict=False)
        partial_data_path = partial_data_path.resolve(strict=False)
        manifest_path = manifest_path.resolve(strict=False)
        directory = selected_root / _JOURNAL_DIRECTORY
        _ensure_directory(directory)
        safe_capture = "".join(
            character
            for character in capture_id
            if character.isalnum() or character in "._-"
        )
        if safe_capture != capture_id or not safe_capture:
            raise HFTAuditJournalError("unsafe audit capture identifier")
        filename = (
            f"{capture_id}-{partition_start_ns}"
            f"-segment-{segment:04d}.audit.wal"
        )
        path = directory / filename
        header = {
            "kind": _HEADER_KIND,
            "schema_version": AUDIT_JOURNAL_SCHEMA_VERSION,
            "market": market,
            "capture_id": capture_id,
            "partition_start_ns": str(partition_start_ns),
            "partition_start_utc": partition_start_utc,
            "partition_end_utc_exclusive": (
                partition_end_utc_exclusive
            ),
            "segment": segment,
            "data_path": _safe_relative(selected_root, data_path),
            "partial_data_path": _safe_relative(
                selected_root, partial_data_path
            ),
            "manifest_path": _safe_relative(
                selected_root, manifest_path
            ),
        }
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(path, flags, 0o600)
        except OSError as exc:
            raise HFTAuditJournalError(
                f"could not create audit journal {path}: {exc}"
            ) from exc
        try:
            _write_all(descriptor, _canonical_json(header) + b"\n")
            durable_file_sync(descriptor)
            _fsync_directory(directory)
        except BaseException:
            os.close(descriptor)
            path.unlink(missing_ok=True)
            _fsync_directory(directory)
            raise
        return cls(
            root=selected_root,
            path=path,
            descriptor=descriptor,
            header=header,
        )

    def append_durable(self, envelope: Mapping[str, Any]) -> str:
        """Return the record digest only after the frame is on durable media."""

        if self._descriptor is None:
            raise HFTAuditJournalError("audit journal is closed")
        payload = _canonical_json(envelope)
        digest = hashlib.sha256(payload).hexdigest()
        frame = {
            "kind": _RECORD_KIND,
            "schema_version": AUDIT_JOURNAL_SCHEMA_VERSION,
            "sha256": digest,
            "payload": dict(envelope),
        }
        encoded = _canonical_json(frame) + b"\n"
        try:
            _write_all(self._descriptor, encoded)
            durable_file_sync(self._descriptor)
        except BaseException:
            # Keep the complete, fsynced prefix.  Recovery treats an
            # incomplete final line as an unacknowledged crash tail.
            raise
        return digest

    def close_preserving(self) -> None:
        if self._descriptor is None:
            return
        descriptor = self._descriptor
        self._descriptor = None
        os.close(descriptor)

    def discard_after_archive_commit(self) -> bool:
        """Best-effort WAL cleanup after the gzip bundle is fully durable."""

        try:
            self.close_preserving()
            self.path.unlink(missing_ok=True)
            _fsync_directory(self.path.parent)
        except (OSError, HFTAuditJournalError):
            # A surviving WAL is safe: next startup verifies the committed
            # archive and removes it idempotently.
            return False
        return True


@dataclass(frozen=True, slots=True)
class _JournalScan:
    header: dict[str, Any]
    valid_end: int
    records: int
    first_ordinal: int
    last_ordinal: int
    first_wall_ns: str
    last_wall_ns: str
    first_monotonic_ns: str
    last_monotonic_ns: str
    connection_ids: tuple[str, ...]
    gap_count: int
    event_type_counts: dict[str, int]
    has_incomplete_tail: bool


def _validated_frame(
    raw_line: bytes,
    *,
    header: Mapping[str, Any],
    previous_ordinal: int | None,
) -> tuple[dict[str, Any], int]:
    try:
        frame = json.loads(raw_line)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HFTAuditJournalError("invalid complete audit record frame") from exc
    if (
        not isinstance(frame, dict)
        or frame.get("kind") != _RECORD_KIND
        or frame.get("schema_version") != AUDIT_JOURNAL_SCHEMA_VERSION
        or not isinstance(frame.get("payload"), dict)
    ):
        raise HFTAuditJournalError("invalid complete audit record structure")
    payload = frame["payload"]
    digest = hashlib.sha256(_canonical_json(payload)).hexdigest()
    if frame.get("sha256") != digest:
        raise HFTAuditJournalError("audit record checksum mismatch")
    if payload.get("capture_id") != header["capture_id"]:
        raise HFTAuditJournalError("audit record capture ID mismatch")
    event = payload.get("event")
    if not isinstance(event, dict) or event.get("market") != header["market"]:
        raise HFTAuditJournalError("audit record market mismatch")
    ordinal = payload.get("ordinal")
    if isinstance(ordinal, bool) or not isinstance(ordinal, int) or ordinal <= 0:
        raise HFTAuditJournalError("audit record ordinal is invalid")
    if previous_ordinal is not None and ordinal != previous_ordinal + 1:
        raise HFTAuditJournalError("audit record ordinals are not contiguous")
    for field in ("connection_id", "received_wall_ns", "received_monotonic_ns"):
        value = payload.get(field)
        if not isinstance(value, str) or not value:
            raise HFTAuditJournalError(
                f"audit record {field} is invalid"
            )
    return payload, ordinal


def _scan_journal(path: Path) -> _JournalScan:
    with path.open("rb") as handle:
        header_line = handle.readline()
        if not header_line.endswith(b"\n"):
            raise HFTAuditJournalError("audit journal header is incomplete")
        try:
            header = json.loads(header_line)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise HFTAuditJournalError("audit journal header is corrupt") from exc
        required = {
            "kind": _HEADER_KIND,
            "schema_version": AUDIT_JOURNAL_SCHEMA_VERSION,
        }
        if not isinstance(header, dict) or any(
            header.get(key) != value for key, value in required.items()
        ):
            raise HFTAuditJournalError("audit journal header is invalid")
        for field in (
            "market",
            "capture_id",
            "partition_start_utc",
            "partition_end_utc_exclusive",
            "data_path",
            "partial_data_path",
            "manifest_path",
        ):
            if not isinstance(header.get(field), str) or not header[field]:
                raise HFTAuditJournalError(
                    f"audit journal header {field} is invalid"
                )

        records = 0
        first_ordinal: int | None = None
        last_ordinal: int | None = None
        first_wall_ns: str | None = None
        last_wall_ns: str | None = None
        first_monotonic_ns: str | None = None
        last_monotonic_ns: str | None = None
        connection_ids: set[str] = set()
        gap_count = 0
        event_type_counts: dict[str, int] = {}
        valid_end = handle.tell()
        has_incomplete_tail = False
        while True:
            line = handle.readline()
            if not line:
                break
            if not line.endswith(b"\n"):
                has_incomplete_tail = True
                break
            payload, ordinal = _validated_frame(
                line,
                header=header,
                previous_ordinal=last_ordinal,
            )
            if first_ordinal is None:
                first_ordinal = ordinal
                first_wall_ns = payload["received_wall_ns"]
                first_monotonic_ns = payload["received_monotonic_ns"]
            last_ordinal = ordinal
            last_wall_ns = payload["received_wall_ns"]
            last_monotonic_ns = payload["received_monotonic_ns"]
            connection_ids.add(payload["connection_id"])
            if payload.get("gap_before") is True:
                gap_count += 1
            event_type = str(payload["event"].get("event_type", "unknown"))
            event_type_counts[event_type] = (
                event_type_counts.get(event_type, 0) + 1
            )
            records += 1
            valid_end = handle.tell()
    if records == 0:
        raise _EmptyAuditJournal(
            "audit journal contains no recoverable event records"
        )
    assert first_ordinal is not None
    assert last_ordinal is not None
    assert first_wall_ns is not None
    assert last_wall_ns is not None
    assert first_monotonic_ns is not None
    assert last_monotonic_ns is not None
    return _JournalScan(
        header=header,
        valid_end=valid_end,
        records=records,
        first_ordinal=first_ordinal,
        last_ordinal=last_ordinal,
        first_wall_ns=first_wall_ns,
        last_wall_ns=last_wall_ns,
        first_monotonic_ns=first_monotonic_ns,
        last_monotonic_ns=last_monotonic_ns,
        connection_ids=tuple(sorted(connection_ids)),
        gap_count=gap_count,
        event_type_counts=dict(sorted(event_type_counts.items())),
        has_incomplete_tail=has_incomplete_tail,
    )


def _iter_payload_bytes(
    journal_path: Path,
    scan: _JournalScan,
) -> Iterator[bytes]:
    with journal_path.open("rb") as handle:
        handle.readline()
        previous_ordinal: int | None = None
        while handle.tell() < scan.valid_end:
            line = handle.readline()
            payload, previous_ordinal = _validated_frame(
                line,
                header=scan.header,
                previous_ordinal=previous_ordinal,
            )
            yield _canonical_json(payload)


def _sha256_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        while block := handle.read(1024 * 1024):
            digest.update(block)
            size += len(block)
    return digest.hexdigest(), size


def _archive_matches(
    *,
    journal_path: Path,
    scan: _JournalScan,
    data_path: Path,
    manifest_path: Path,
) -> bool:
    if (
        not data_path.is_file()
        or data_path.is_symlink()
        or not manifest_path.is_file()
        or manifest_path.is_symlink()
    ):
        return False
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        digest, size = _sha256_file(data_path)
        if (
            not isinstance(manifest, dict)
            or manifest.get("sha256") != digest
            or manifest.get("compressed_bytes") != size
            or manifest.get("records") != scan.records
            or manifest.get("capture_id") != scan.header["capture_id"]
        ):
            return False
        expected = _iter_payload_bytes(journal_path, scan)
        with gzip.open(data_path, "rb") as archive:
            for payload in expected:
                line = archive.readline()
                if not line or line.rstrip(b"\n") != payload:
                    return False
            if archive.readline():
                return False
    except (OSError, EOFError, ValueError, json.JSONDecodeError):
        return False
    return True


def _quarantine(
    root: Path,
    journal_path: Path,
    candidates: tuple[Path, ...],
) -> tuple[Path, ...]:
    directory = root / _QUARANTINE_DIRECTORY
    _ensure_directory(directory)
    moved: list[Path] = []
    for index, candidate in enumerate(candidates, start=1):
        if not candidate.exists() and not candidate.is_symlink():
            continue
        if candidate.is_symlink() or not candidate.is_file():
            raise HFTAuditJournalError(
                f"unsafe artifact blocks audit recovery: {candidate}"
            )
        destination = directory / (
            f"{journal_path.stem}-{index:02d}-{candidate.name}.quarantined"
        )
        suffix = 0
        while destination.exists() or destination.is_symlink():
            suffix += 1
            destination = directory / (
                f"{journal_path.stem}-{index:02d}-{suffix:04d}-"
                f"{candidate.name}.quarantined"
            )
        os.replace(candidate, destination)
        moved.append(destination)
    if moved:
        _fsync_directory(directory)
        for parent in {candidate.parent for candidate in candidates}:
            if parent.exists() and parent.is_dir() and not parent.is_symlink():
                _fsync_directory(parent)
    return tuple(moved)


def _write_recovered_bundle(
    *,
    root: Path,
    journal_path: Path,
    scan: _JournalScan,
    data_path: Path,
    manifest_path: Path,
) -> None:
    _ensure_durable_descendant_directory(root, data_path.parent)
    data_partial = data_path.with_name(data_path.name + ".recovery.partial")
    manifest_partial = manifest_path.with_name(
        manifest_path.name + ".recovery.partial"
    )
    for candidate in (data_partial, manifest_partial):
        if candidate.exists() or candidate.is_symlink():
            candidate.unlink(missing_ok=True)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    raw_descriptor = os.open(data_partial, flags, 0o600)
    raw_handle = os.fdopen(raw_descriptor, "wb")
    try:
        with gzip.GzipFile(
            filename="",
            mode="wb",
            fileobj=raw_handle,
            mtime=0,
        ) as compressed:
            for payload in _iter_payload_bytes(journal_path, scan):
                compressed.write(payload + b"\n")
        raw_handle.flush()
        durable_file_sync(raw_handle.fileno())
    finally:
        raw_handle.close()
    digest, compressed_bytes = _sha256_file(data_partial)
    manifest = {
        "schema_version": 1,
        "capture_id": scan.header["capture_id"],
        "market": scan.header["market"],
        "partition_start_utc": scan.header["partition_start_utc"],
        "partition_end_utc_exclusive": (
            scan.header["partition_end_utc_exclusive"]
        ),
        "data_file": data_path.relative_to(root).as_posix(),
        "compression": "gzip",
        "sha256": digest,
        "sha256_scope": "compressed_file_bytes",
        "compressed_bytes": compressed_bytes,
        "records": scan.records,
        "first_ordinal": scan.first_ordinal,
        "last_ordinal": scan.last_ordinal,
        "first_received_wall_ns": scan.first_wall_ns,
        "last_received_wall_ns": scan.last_wall_ns,
        "first_received_monotonic_ns": scan.first_monotonic_ns,
        "last_received_monotonic_ns": scan.last_monotonic_ns,
        "connection_ids": list(scan.connection_ids),
        "connection_count": len(scan.connection_ids),
        "gap_count": scan.gap_count,
        "event_type_counts": scan.event_type_counts,
        "counter_deltas": {},
        "counters_cumulative": {},
        "audit_journal_schema_version": AUDIT_JOURNAL_SCHEMA_VERSION,
        "audit_durability": "fsync_before_consumer",
        "recovered_from_audit_journal": True,
    }
    encoded_manifest = (
        json.dumps(
            manifest,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")
    manifest_descriptor = os.open(manifest_partial, flags, 0o600)
    try:
        _write_all(manifest_descriptor, encoded_manifest)
        durable_file_sync(manifest_descriptor)
    finally:
        os.close(manifest_descriptor)

    # Match the normal archive protocol: manifest first, data last as the
    # bundle commit point.  A second crash leaves the WAL for another retry.
    os.replace(manifest_partial, manifest_path)
    _fsync_directory(manifest_path.parent)
    os.replace(data_partial, data_path)
    _fsync_directory(data_path.parent)


def _recover_one(root: Path, journal_path: Path) -> AuditRecoveryResult:
    try:
        scan = _scan_journal(journal_path)
    except _EmptyAuditJournal:
        moved = _quarantine(root, journal_path, (journal_path,))
        return AuditRecoveryResult(
            journal_path=journal_path,
            disposition="empty_quarantined",
            records=0,
            data_path=None,
            manifest_path=None,
            quarantined_path=moved[0] if moved else None,
        )
    except HFTAuditJournalError:
        _quarantine(root, journal_path, (journal_path,))
        raise HFTAuditJournalError(
            f"unrecoverable audit journal quarantined: {journal_path}"
        )
    header = scan.header
    try:
        data_path = _path_from_header(root, header["data_path"], "data_path")
        partial_path = _path_from_header(
            root, header["partial_data_path"], "partial_data_path"
        )
        manifest_path = _path_from_header(
            root, header["manifest_path"], "manifest_path"
        )
    except HFTAuditJournalError as exc:
        _quarantine(root, journal_path, (journal_path,))
        raise HFTAuditJournalError(
            f"unsafe audit journal quarantined: {journal_path}"
        ) from exc
    manifest_partial = manifest_path.with_name(
        manifest_path.name + ".partial"
    )
    recovery_partials = (
        data_path.with_name(data_path.name + ".recovery.partial"),
        manifest_path.with_name(manifest_path.name + ".recovery.partial"),
    )

    if _archive_matches(
        journal_path=journal_path,
        scan=scan,
        data_path=data_path,
        manifest_path=manifest_path,
    ):
        _quarantine(
            root,
            journal_path,
            (partial_path, manifest_partial, *recovery_partials),
        )
        journal_path.unlink(missing_ok=True)
        _fsync_directory(journal_path.parent)
        return AuditRecoveryResult(
            journal_path=journal_path,
            disposition="already_committed",
            records=scan.records,
            data_path=data_path,
            manifest_path=manifest_path,
        )

    _quarantine(
        root,
        journal_path,
        (
            data_path,
            manifest_path,
            partial_path,
            manifest_partial,
            *recovery_partials,
        ),
    )
    _write_recovered_bundle(
        root=root,
        journal_path=journal_path,
        scan=scan,
        data_path=data_path,
        manifest_path=manifest_path,
    )
    quarantined_journal: Path | None = None
    if scan.has_incomplete_tail:
        moved = _quarantine(root, journal_path, (journal_path,))
        quarantined_journal = moved[0] if moved else None
    else:
        journal_path.unlink(missing_ok=True)
        _fsync_directory(journal_path.parent)
    return AuditRecoveryResult(
        journal_path=journal_path,
        disposition="recovered",
        records=scan.records,
        data_path=data_path,
        manifest_path=manifest_path,
        quarantined_path=quarantined_journal,
    )


def recover_stale_audit_journals(
    output_root: str | Path,
) -> tuple[AuditRecoveryResult, ...]:
    """Recover all crash-left WALs while excluding concurrent recorders."""

    coordinator = AuditJournalCoordinator(output_root)
    coordinator.acquire()
    try:
        return coordinator.recover()
    finally:
        coordinator.release()
