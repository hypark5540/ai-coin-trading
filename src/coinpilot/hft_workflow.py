"""Bounded, provenance-preserving I/O for HFT research workflows.

The long-running recorder stores gzip-compressed archive envelopes, while the
original bounded capture stores direct normalized JSONL events.  This module
provides one strict reader for both shapes and small atomic artifact writers.
It deliberately imposes a record limit: a month of public order-book traffic
must be processed as bounded partitions rather than accidentally materialized
as one unbounded Python list.
"""

from __future__ import annotations

import fcntl
import gzip
import hashlib
import io
import json
import os
import stat
import uuid
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, BinaryIO, TextIO


DEFAULT_MAX_RESEARCH_RECORDS = 1_000_000
_SUPPORTED_SUFFIXES = (".jsonl", ".jsonl.gz")


class HFTWorkflowError(ValueError):
    """Raised when an HFT research input or artifact path is unsafe or invalid."""


@dataclass(frozen=True, slots=True)
class HFTInputFile:
    path: Path
    sha256: str
    stored_bytes: int
    records: int
    manifest_path: Path | None
    manifest_verified: bool
    capture_id: str | None
    first_ordinal: int | None
    last_ordinal: int | None

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["path"] = str(self.path)
        value["manifest_path"] = (
            None
            if self.manifest_path is None
            else str(self.manifest_path)
        )
        return value


@dataclass(frozen=True, slots=True)
class HFTRecordBatch:
    source: Path
    records: tuple[dict[str, Any], ...]
    files: tuple[HFTInputFile, ...]
    combined_sha256: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": str(self.source),
            "record_count": len(self.records),
            "file_count": len(self.files),
            "combined_sha256": self.combined_sha256,
            "files": [item.to_dict() for item in self.files],
        }


class HFTRecordStream:
    """Single-pass verified reader whose file profile is available afterward."""

    def __init__(
        self,
        source: str | Path,
        *,
        max_records: int | None = None,
        allow_unmanifested_gzip: bool = False,
    ) -> None:
        self.source = Path(source)
        self.max_records = _validate_max_records(max_records)
        if not isinstance(allow_unmanifested_gzip, bool):
            raise HFTWorkflowError(
                "allow_unmanifested_gzip must be a boolean"
            )
        self.allow_unmanifested_gzip = allow_unmanifested_gzip
        self._paths = _safe_input_files(self.source)
        self._files: list[HFTInputFile] = []
        self._combined_sha256: str | None = None
        self._record_count = 0
        self._started = False
        self._completed = False

    @property
    def files(self) -> tuple[HFTInputFile, ...]:
        return tuple(self._files)

    @property
    def record_count(self) -> int:
        return self._record_count

    @property
    def combined_sha256(self) -> str:
        if self._combined_sha256 is None:
            raise HFTWorkflowError(
                "combined_sha256 is available only after the stream is consumed"
            )
        return self._combined_sha256

    @property
    def completed(self) -> bool:
        return self._completed

    def to_dict(self) -> dict[str, Any]:
        if not self._completed:
            raise HFTWorkflowError(
                "HFT stream profile is available only after complete consumption"
            )
        return {
            "source": str(self.source),
            "record_count": self._record_count,
            "file_count": len(self._files),
            "combined_sha256": self.combined_sha256,
            "files": [item.to_dict() for item in self._files],
        }

    def __iter__(self) -> Iterator[dict[str, Any]]:
        if self._started:
            raise HFTWorkflowError("HFT record stream is single-use")
        self._started = True
        combined = hashlib.sha256()
        last_ordinal_by_capture: dict[str, int] = {}

        for path in self._paths:
            relative = (
                str(path.relative_to(self.source))
                if self.source.is_dir()
                else path.name
            )
            try:
                raw_handle = _open_binary_nofollow(path)
                try:
                    sha256, stored_bytes = _sha256_handle(raw_handle)
                    manifest_path, manifest = _load_and_verify_manifest(
                        path,
                        sha256=sha256,
                        stored_bytes=stored_bytes,
                        allow_unmanifested_gzip=(
                            self.allow_unmanifested_gzip
                        ),
                    )
                    (
                        manifest_capture_id,
                        manifest_first_ordinal,
                        manifest_last_ordinal,
                        declared_records,
                    ) = _manifest_ordinal_profile(
                        manifest,
                        manifest_path=manifest_path,
                        last_ordinal_by_capture=last_ordinal_by_capture,
                    )
                    raw_handle.seek(0)
                    snapshot_handle = _duplicate_binary_handle(raw_handle)
                finally:
                    raw_handle.close()
            except HFTWorkflowError:
                raise
            except OSError as exc:
                raise HFTWorkflowError(
                    f"cannot open or hash HFT input {path}: {exc}"
                ) from exc
            combined.update(relative.encode("utf-8"))
            combined.update(b"\0")
            combined.update(sha256.encode("ascii"))
            combined.update(b"\n")

            file_records = 0
            try:
                with _open_text_snapshot(snapshot_handle, path) as handle:
                    for line_number, line in enumerate(handle, start=1):
                        if not line.strip():
                            continue
                        if (
                            self.max_records is not None
                            and self._record_count >= self.max_records
                        ):
                            raise HFTWorkflowError(
                                "HFT input exceeds max_records="
                                f"{self.max_records}; select fewer UTC "
                                "partitions or raise the limit explicitly "
                                "after checking capacity"
                            )
                        try:
                            decoded = json.loads(line)
                        except json.JSONDecodeError as exc:
                            raise HFTWorkflowError(
                                f"invalid JSON at {path}:{line_number}: {exc}"
                            ) from exc
                        if not isinstance(decoded, Mapping):
                            raise HFTWorkflowError(
                                "HFT record must be an object at "
                                f"{path}:{line_number}"
                            )
                        if manifest is not None:
                            if decoded.get("schema_version") != 1:
                                raise HFTWorkflowError(
                                    "archive envelope schema_version mismatch "
                                    f"at {path}:{line_number}"
                                )
                            if (
                                decoded.get("capture_id")
                                != manifest_capture_id
                            ):
                                raise HFTWorkflowError(
                                    "archive envelope capture_id disagrees "
                                    f"with manifest at {path}:{line_number}"
                                )
                            raw_ordinal = decoded.get("ordinal")
                            if type(raw_ordinal) is not int:
                                raise HFTWorkflowError(
                                    "archive envelope ordinal must be an "
                                    f"integer at {path}:{line_number}"
                                )
                            ordinal = raw_ordinal
                            expected_ordinal = (
                                manifest_first_ordinal + file_records
                            )
                            if ordinal != expected_ordinal:
                                raise HFTWorkflowError(
                                    "HFT archive ordinal discontinuity inside "
                                    f"{path}: expected {expected_ordinal}, "
                                    f"got {ordinal}"
                                )
                        file_records += 1
                        self._record_count += 1
                        yield dict(decoded)
            except HFTWorkflowError:
                raise
            except (OSError, EOFError, UnicodeError) as exc:
                raise HFTWorkflowError(
                    f"cannot read HFT input {path}: {exc}"
                ) from exc

            if manifest is not None:
                if declared_records != file_records:
                    raise HFTWorkflowError(
                        f"HFT manifest record-count mismatch for {path}"
                    )
                if (
                    file_records
                    and manifest_first_ordinal + file_records - 1
                    != manifest_last_ordinal
                ):
                    raise HFTWorkflowError(
                        f"HFT manifest last_ordinal mismatch for {path}"
                    )
                last_ordinal_by_capture[manifest_capture_id] = (
                    manifest_last_ordinal
                )

            self._files.append(
                HFTInputFile(
                    path=path,
                    sha256=sha256,
                    stored_bytes=stored_bytes,
                    records=file_records,
                    manifest_path=manifest_path,
                    manifest_verified=manifest is not None,
                    capture_id=manifest_capture_id,
                    first_ordinal=manifest_first_ordinal,
                    last_ordinal=manifest_last_ordinal,
                )
            )

        self._combined_sha256 = combined.hexdigest()
        self._completed = True


def stream_hft_records(
    source: str | Path,
    *,
    max_records: int | None = None,
    allow_unmanifested_gzip: bool = False,
) -> HFTRecordStream:
    """Return a verified single-pass stream over finalized archive records.

    Recorder gzip files require a finalized manifest by default.  The opt-out
    exists only for explicitly trusted legacy/research JSONL gzip artifacts;
    it must never be used to consume a recorder directory after a crash.
    """

    return HFTRecordStream(
        source,
        max_records=max_records,
        allow_unmanifested_gzip=allow_unmanifested_gzip,
    )


def _is_supported(path: Path) -> bool:
    name = path.name
    return any(name.endswith(suffix) for suffix in _SUPPORTED_SUFFIXES)


def _safe_input_files(source: Path) -> tuple[Path, ...]:
    if source.is_symlink():
        raise HFTWorkflowError("HFT input cannot be a symbolic link")
    if source.is_file():
        if not _is_supported(source):
            raise HFTWorkflowError("HFT input must be .jsonl or .jsonl.gz")
        return (source,)
    if not source.is_dir():
        raise HFTWorkflowError(f"HFT input does not exist: {source}")

    files = tuple(
        sorted(
            path
            for path in source.rglob("*")
            if path.is_file()
            and not path.is_symlink()
            and _is_supported(path)
            and ".partial" not in path.name
        )
    )
    if not files:
        raise HFTWorkflowError(
            f"HFT input directory has no finalized JSONL partitions: {source}"
        )
    return files


def _open_binary_nofollow(path: Path) -> BinaryIO:
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise HFTWorkflowError(
                f"HFT input must be a regular file: {path}"
            )
        return os.fdopen(descriptor, "rb")
    except BaseException:
        os.close(descriptor)
        raise


def _sha256_handle(handle: BinaryIO) -> tuple[str, int]:
    digest = hashlib.sha256()
    total = 0
    while chunk := handle.read(1024 * 1024):
        digest.update(chunk)
        total += len(chunk)
    return digest.hexdigest(), total


def _duplicate_binary_handle(handle: BinaryIO) -> BinaryIO:
    descriptor = os.dup(handle.fileno())
    try:
        return os.fdopen(descriptor, "rb")
    except BaseException:
        os.close(descriptor)
        raise


@contextmanager
def _open_text_snapshot(
    handle: BinaryIO,
    path: Path,
) -> Iterator[TextIO]:
    text_handle: TextIO | None = None
    try:
        if path.name.endswith(".gz"):
            compressed = gzip.GzipFile(
                filename="",
                mode="rb",
                fileobj=handle,
            )
            text_handle = io.TextIOWrapper(compressed, encoding="utf-8")
        else:
            text_handle = io.TextIOWrapper(handle, encoding="utf-8")
        yield text_handle
    finally:
        if text_handle is not None:
            try:
                text_handle.close()
            finally:
                handle.close()
        else:
            handle.close()


def _sha256_file(path: Path) -> tuple[str, int]:
    with path.open("rb") as handle:
        return _sha256_handle(handle)


def _manifest_path(data_path: Path) -> Path | None:
    if not data_path.name.endswith(".jsonl.gz"):
        return None
    candidate = data_path.with_name(
        data_path.name.removesuffix(".jsonl.gz") + ".manifest.json"
    )
    return candidate if candidate.exists() else None


def _load_and_verify_manifest(
    data_path: Path,
    *,
    sha256: str,
    stored_bytes: int,
    allow_unmanifested_gzip: bool,
) -> tuple[Path | None, Mapping[str, Any] | None]:
    if data_path.name.endswith(".jsonl.gz"):
        partial_manifest = data_path.with_name(
            data_path.name.removesuffix(".jsonl.gz")
            + ".manifest.json.partial"
        )
        if partial_manifest.exists() or partial_manifest.is_symlink():
            raise HFTWorkflowError(
                "compressed HFT archive has an uncommitted manifest partial: "
                f"{partial_manifest}"
            )
    manifest_path = _manifest_path(data_path)
    if manifest_path is None:
        if (
            data_path.name.endswith(".jsonl.gz")
            and not allow_unmanifested_gzip
        ):
            raise HFTWorkflowError(
                "compressed HFT archive has no finalized manifest: "
                f"{data_path}"
            )
        return None, None
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise HFTWorkflowError(
            f"HFT manifest must be a regular non-symlink file: {manifest_path}"
        )
    try:
        decoded = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise HFTWorkflowError(
            f"cannot read HFT manifest {manifest_path}: {exc}"
        ) from exc
    if not isinstance(decoded, Mapping):
        raise HFTWorkflowError(f"HFT manifest must be an object: {manifest_path}")
    if decoded.get("schema_version") != 1:
        raise HFTWorkflowError(
            f"unsupported HFT manifest schema_version: {manifest_path}"
        )
    if decoded.get("compression") != "gzip":
        raise HFTWorkflowError(
            f"unsupported HFT manifest compression: {manifest_path}"
        )
    declared_file = decoded.get("data_file")
    if not isinstance(declared_file, str) or not declared_file:
        raise HFTWorkflowError(
            f"invalid data_file in HFT manifest: {manifest_path}"
        )
    declared_path = Path(declared_file)
    if (
        declared_path.is_absolute()
        or ".." in declared_path.parts
        or len(declared_path.parts) > len(data_path.parts)
        or tuple(data_path.parts[-len(declared_path.parts) :])
        != declared_path.parts
    ):
        raise HFTWorkflowError(
            f"HFT manifest data_file does not match {data_path}"
        )
    if decoded.get("sha256_scope") != "compressed_file_bytes":
        raise HFTWorkflowError(
            f"unsupported SHA-256 scope in HFT manifest: {manifest_path}"
        )
    if decoded.get("sha256") != sha256:
        raise HFTWorkflowError(
            f"HFT manifest SHA-256 mismatch for {data_path}"
        )
    try:
        declared_bytes = int(decoded.get("compressed_bytes"))
    except (TypeError, ValueError, OverflowError) as exc:
        raise HFTWorkflowError(
            f"invalid compressed_bytes in HFT manifest: {manifest_path}"
        ) from exc
    if declared_bytes != stored_bytes:
        raise HFTWorkflowError(
            f"HFT manifest byte-count mismatch for {data_path}"
        )
    return manifest_path, decoded


def _manifest_ordinal_profile(
    manifest: Mapping[str, Any] | None,
    *,
    manifest_path: Path | None,
    last_ordinal_by_capture: Mapping[str, int],
) -> tuple[str | None, int | None, int | None, int | None]:
    if manifest is None:
        return None, None, None, None
    raw_capture_id = manifest.get("capture_id")
    if not isinstance(raw_capture_id, str) or not raw_capture_id.strip():
        raise HFTWorkflowError(
            f"invalid capture_id in HFT manifest: {manifest_path}"
        )
    capture_id = raw_capture_id.strip()
    raw_first = manifest.get("first_ordinal")
    raw_last = manifest.get("last_ordinal")
    raw_records = manifest.get("records")
    if any(type(value) is not int for value in (raw_first, raw_last, raw_records)):
        raise HFTWorkflowError(
            "invalid ordinal/records metadata in HFT manifest: "
            f"{manifest_path}"
        )
    first_ordinal = raw_first
    last_ordinal = raw_last
    records = raw_records
    if (
        first_ordinal < 1
        or last_ordinal < first_ordinal
        or records != last_ordinal - first_ordinal + 1
    ):
        raise HFTWorkflowError(
            f"inconsistent ordinal range in HFT manifest: {manifest_path}"
        )
    previous_ordinal = last_ordinal_by_capture.get(capture_id)
    if (
        previous_ordinal is not None
        and first_ordinal != previous_ordinal + 1
    ):
        raise HFTWorkflowError(
            "HFT archive ordinal discontinuity between partitions "
            f"for {capture_id}: {previous_ordinal} -> {first_ordinal}"
        )
    return capture_id, first_ordinal, last_ordinal, records


def _validate_max_records(max_records: int | None) -> int | None:
    if max_records is None:
        return None
    if (
        isinstance(max_records, bool)
        or not isinstance(max_records, int)
        or max_records < 1
    ):
        raise HFTWorkflowError("max_records must be a positive integer or None")
    return max_records


def load_hft_records(
    source: str | Path,
    *,
    max_records: int | None = DEFAULT_MAX_RESEARCH_RECORDS,
    allow_unmanifested_gzip: bool = False,
) -> HFTRecordBatch:
    """Load finalized direct events or archive envelopes with bounded memory.

    Files are ordered lexicographically, matching the recorder's UTC partition
    names.  The SHA-256 values cover stored bytes (compressed bytes for gzip)
    and let downstream artifacts identify their exact input.
    """

    source_path = Path(source)
    stream = stream_hft_records(
        source_path,
        max_records=max_records,
        allow_unmanifested_gzip=allow_unmanifested_gzip,
    )
    records = tuple(stream)

    return HFTRecordBatch(
        source=source_path,
        records=records,
        files=stream.files,
        combined_sha256=stream.combined_sha256,
    )


def _validate_output_path(path: str | Path, *, overwrite: bool) -> Path:
    destination = Path(path)
    if destination.is_symlink():
        raise HFTWorkflowError("HFT artifact output cannot be a symbolic link")
    if destination.exists() and not overwrite:
        raise HFTWorkflowError(
            f"HFT artifact already exists: {destination}; use overwrite=True"
        )
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.parent.is_symlink():
        raise HFTWorkflowError(
            "HFT artifact parent directory cannot be a symbolic link"
        )
    return destination


def _temporary_sibling(destination: Path) -> Path:
    return destination.with_name(
        f".{destination.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    )


def artifact_staging_path(path: str | Path) -> Path:
    """Return a unique sibling path that preserves the destination suffix."""

    destination = Path(path)
    return destination.with_name(
        f".{uuid.uuid4().hex}.stage.{destination.name}"
    )


@contextmanager
def artifact_bundle_lock(completion_path: str | Path) -> Iterator[Path]:
    """Serialize bundle replacement and completion-marker publication."""

    marker = Path(completion_path)
    marker.parent.mkdir(parents=True, exist_ok=True)
    if marker.parent.is_symlink():
        raise HFTWorkflowError(
            "HFT artifact lock parent cannot be a symbolic link"
        )
    lock_path = marker.with_name(f".{marker.name}.lock")
    flags = os.O_RDWR | os.O_CREAT
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(lock_path, flags, 0o600)
    except OSError as exc:
        raise HFTWorkflowError(
            f"cannot open HFT artifact bundle lock {lock_path}: {exc}"
        ) from exc
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise HFTWorkflowError(
                f"HFT artifact bundle lock is not a regular file: {lock_path}"
            )
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
        except OSError as exc:
            raise HFTWorkflowError(
                f"cannot acquire HFT artifact bundle lock {lock_path}: {exc}"
            ) from exc
        yield lock_path
    finally:
        os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        try:
            os.fsync(descriptor)
        except OSError:
            pass
    finally:
        os.close(descriptor)


def _promote_temporary(
    temporary: Path,
    destination: Path,
    *,
    overwrite: bool,
) -> None:
    if overwrite:
        os.replace(temporary, destination)
    else:
        try:
            os.link(temporary, destination)
        except FileExistsError as exc:
            raise HFTWorkflowError(
                "HFT artifact appeared during no-overwrite commit: "
                f"{destination}"
            ) from exc
        temporary.unlink()
    _fsync_directory(destination.parent)


def promote_staged_artifacts(
    staged_to_destination: Mapping[str | Path, str | Path],
    *,
    overwrite: bool,
) -> tuple[Path, ...]:
    """Publish fully written sibling staging files without a clobber race."""

    if not staged_to_destination:
        raise HFTWorkflowError("no staged HFT artifacts to promote")
    pairs: list[tuple[Path, Path]] = []
    destinations: set[Path] = set()
    for raw_staged, raw_destination in staged_to_destination.items():
        staged = Path(raw_staged)
        destination = Path(raw_destination)
        if staged.parent.absolute() != destination.parent.absolute():
            raise HFTWorkflowError(
                "HFT staging file must be a destination sibling"
            )
        if staged.is_symlink() or not staged.is_file():
            raise HFTWorkflowError(
                f"HFT staging file is missing or unsafe: {staged}"
            )
        if destination in destinations:
            raise HFTWorkflowError(
                f"duplicate HFT artifact destination: {destination}"
            )
        destinations.add(destination)
        _validate_output_path(destination, overwrite=overwrite)
        pairs.append((staged, destination))

    promoted: list[tuple[Path, Path]] = []
    try:
        for staged, destination in pairs:
            if overwrite:
                os.replace(staged, destination)
            else:
                try:
                    os.link(staged, destination)
                except FileExistsError as exc:
                    raise HFTWorkflowError(
                        "HFT artifact appeared during no-overwrite commit: "
                        f"{destination}"
                    ) from exc
            _fsync_directory(destination.parent)
            promoted.append((staged, destination))
        if not overwrite:
            for staged, _ in pairs:
                try:
                    staged.unlink()
                except OSError:
                    pass
            for parent in {
                destination.parent for _, destination in pairs
            }:
                _fsync_directory(parent)
    except BaseException:
        if not overwrite:
            for staged, destination in reversed(promoted):
                try:
                    if staged.exists() and os.path.samefile(
                        staged,
                        destination,
                    ):
                        destination.unlink(missing_ok=True)
                        _fsync_directory(destination.parent)
                except OSError:
                    pass
        raise
    return tuple(destination for _, destination in pairs)


def write_jsonl_atomic(
    rows: Iterable[Mapping[str, Any]],
    path: str | Path,
    *,
    overwrite: bool = False,
) -> Path:
    """Write JSONL (optionally gzip by suffix) and atomically promote it."""

    destination = _validate_output_path(path, overwrite=overwrite)
    temporary = _temporary_sibling(destination)
    try:
        if destination.name.endswith(".gz"):
            with temporary.open("wb") as raw:
                with gzip.GzipFile(
                    filename="",
                    mode="wb",
                    fileobj=raw,
                    mtime=0,
                ) as compressed:
                    for row in rows:
                        encoded = (
                            json.dumps(
                                dict(row),
                                ensure_ascii=False,
                                separators=(",", ":"),
                                sort_keys=True,
                                allow_nan=False,
                            )
                            + "\n"
                        ).encode("utf-8")
                        compressed.write(encoded)
                raw.flush()
                os.fsync(raw.fileno())
        else:
            with temporary.open("w", encoding="utf-8") as handle:
                for row in rows:
                    handle.write(
                        json.dumps(
                            dict(row),
                            ensure_ascii=False,
                            separators=(",", ":"),
                            sort_keys=True,
                            allow_nan=False,
                        )
                        + "\n"
                    )
                handle.flush()
                os.fsync(handle.fileno())
        _promote_temporary(
            temporary,
            destination,
            overwrite=overwrite,
        )
    except BaseException as exc:
        temporary.unlink(missing_ok=True)
        if isinstance(exc, HFTWorkflowError):
            raise
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        raise HFTWorkflowError(
            f"failed to write HFT JSONL artifact {destination}: {exc}"
        ) from exc
    return destination


def write_json_atomic(
    value: Mapping[str, Any],
    path: str | Path,
    *,
    overwrite: bool = False,
) -> Path:
    """Write a strict finite JSON object and atomically promote it."""

    destination = _validate_output_path(path, overwrite=overwrite)
    temporary = _temporary_sibling(destination)
    try:
        encoded = json.dumps(
            dict(value),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
        with temporary.open("w", encoding="utf-8") as handle:
            handle.write(encoded + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        _promote_temporary(
            temporary,
            destination,
            overwrite=overwrite,
        )
    except BaseException as exc:
        temporary.unlink(missing_ok=True)
        if isinstance(exc, HFTWorkflowError):
            raise
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        raise HFTWorkflowError(
            f"failed to write HFT JSON artifact {destination}: {exc}"
        ) from exc
    return destination


def invalidate_artifact_completion(path: str | Path) -> None:
    """Remove a prior bundle commit marker before replacing its members."""

    marker = Path(path)
    if marker.is_symlink():
        raise HFTWorkflowError(
            "HFT artifact completion marker cannot be a symbolic link"
        )
    if marker.exists():
        if not marker.is_file():
            raise HFTWorkflowError(
                f"HFT artifact completion marker is not a file: {marker}"
            )
        try:
            marker.unlink()
            _fsync_directory(marker.parent)
        except OSError as exc:
            raise HFTWorkflowError(
                f"cannot invalidate HFT artifact completion marker "
                f"{marker}: {exc}"
            ) from exc


def write_artifact_completion(
    artifacts: Mapping[str, str | Path],
    path: str | Path,
    *,
    metadata: Mapping[str, Any] | None = None,
    overwrite: bool = False,
) -> Path:
    """Commit a multi-file research artifact by writing its hash marker last.

    Consumers should treat all listed files as uncommitted unless this marker
    exists and every stored-byte hash matches.  Call
    :func:`invalidate_artifact_completion` before replacing bundle members.
    """

    if not artifacts:
        raise HFTWorkflowError("artifact completion requires at least one file")
    marker = Path(path)
    marker_parent = marker.parent.absolute()
    members: dict[str, dict[str, Any]] = {}
    seen_paths: set[Path] = set()
    for raw_name, raw_path in sorted(artifacts.items()):
        name = str(raw_name).strip()
        if not name:
            raise HFTWorkflowError(
                "artifact completion member names must be non-empty"
            )
        member = Path(raw_path)
        if member == marker:
            raise HFTWorkflowError(
                "artifact completion marker cannot list itself"
            )
        if member in seen_paths:
            raise HFTWorkflowError(
                f"artifact completion lists a duplicate path: {member}"
            )
        seen_paths.add(member)
        if member.is_symlink() or not member.is_file():
            raise HFTWorkflowError(
                "artifact completion member must be a regular non-symlink "
                f"file: {member}"
            )
        try:
            digest, stored_bytes = _sha256_file(member)
        except OSError as exc:
            raise HFTWorkflowError(
                f"cannot hash HFT artifact member {member}: {exc}"
            ) from exc
        member_absolute = member.absolute()
        try:
            stored_path = member_absolute.relative_to(marker_parent).as_posix()
            path_base = "completion_parent"
        except ValueError:
            stored_path = str(member_absolute)
            path_base = "absolute"
        members[name] = {
            "path": stored_path,
            "path_base": path_base,
            "sha256": digest,
            "sha256_scope": "stored_file_bytes",
            "stored_bytes": stored_bytes,
        }
    payload: dict[str, Any] = {
        "schema_version": 1,
        "status": "complete",
        "members": members,
    }
    if metadata is not None:
        payload["metadata"] = dict(metadata)
    return write_json_atomic(
        payload,
        marker,
        overwrite=overwrite,
    )


def verify_artifact_completion(path: str | Path) -> dict[str, Any]:
    """Verify every stored-byte hash in a completed research bundle."""

    marker = Path(path)
    if marker.is_symlink() or not marker.is_file():
        raise HFTWorkflowError(
            "HFT artifact completion marker must be a regular non-symlink "
            f"file: {marker}"
        )
    try:
        decoded = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise HFTWorkflowError(
            f"cannot read HFT artifact completion marker {marker}: {exc}"
        ) from exc
    if (
        not isinstance(decoded, Mapping)
        or decoded.get("schema_version") != 1
        or decoded.get("status") != "complete"
        or not isinstance(decoded.get("members"), Mapping)
        or not decoded["members"]
    ):
        raise HFTWorkflowError(
            f"invalid HFT artifact completion marker: {marker}"
        )
    for raw_name, raw_member in decoded["members"].items():
        if not isinstance(raw_name, str) or not isinstance(
            raw_member,
            Mapping,
        ):
            raise HFTWorkflowError(
                f"invalid HFT artifact member in {marker}"
            )
        raw_path = raw_member.get("path")
        path_base = raw_member.get("path_base")
        if not isinstance(raw_path, str) or not raw_path:
            raise HFTWorkflowError(
                f"invalid path for HFT artifact member {raw_name}"
            )
        declared = Path(raw_path)
        if path_base == "completion_parent":
            if declared.is_absolute() or ".." in declared.parts:
                raise HFTWorkflowError(
                    f"unsafe relative path for HFT artifact member {raw_name}"
                )
            member = marker.parent / declared
        elif path_base == "absolute" and declared.is_absolute():
            member = declared
        else:
            raise HFTWorkflowError(
                f"invalid path_base for HFT artifact member {raw_name}"
            )
        if member.is_symlink() or not member.is_file():
            raise HFTWorkflowError(
                f"HFT artifact member is missing or unsafe: {member}"
            )
        if raw_member.get("sha256_scope") != "stored_file_bytes":
            raise HFTWorkflowError(
                f"invalid SHA-256 scope for HFT artifact member {raw_name}"
            )
        try:
            declared_bytes = int(raw_member.get("stored_bytes"))
        except (TypeError, ValueError, OverflowError) as exc:
            raise HFTWorkflowError(
                f"invalid byte count for HFT artifact member {raw_name}"
            ) from exc
        try:
            digest, stored_bytes = _sha256_file(member)
        except OSError as exc:
            raise HFTWorkflowError(
                f"cannot hash HFT artifact member {member}: {exc}"
            ) from exc
        if declared_bytes != stored_bytes:
            raise HFTWorkflowError(
                f"byte-count mismatch for HFT artifact member {raw_name}"
            )
        if raw_member.get("sha256") != digest:
            raise HFTWorkflowError(
                f"SHA-256 mismatch for HFT artifact member {raw_name}"
            )
    return dict(decoded)
