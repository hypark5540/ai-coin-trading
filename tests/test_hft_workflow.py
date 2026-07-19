from __future__ import annotations

import gzip
import hashlib
import json
import multiprocessing
import os
from pathlib import Path

import pytest

import coinpilot.hft_workflow as hft_workflow
from coinpilot.hft_workflow import (
    HFTWorkflowError,
    artifact_bundle_lock,
    artifact_staging_path,
    invalidate_artifact_completion,
    load_hft_records,
    promote_staged_artifacts,
    stream_hft_records,
    verify_artifact_completion,
    write_artifact_completion,
    write_json_atomic,
    write_jsonl_atomic,
)


def _acquire_bundle_lock_in_child(
    marker: str,
    started,
    acquired,
) -> None:
    started.set()
    with artifact_bundle_lock(marker):
        acquired.set()


def _write_manifest(
    path: Path,
    records: int,
    *,
    capture_id: str = "capture-test",
    first_ordinal: int = 1,
) -> Path:
    manifest = path.with_name(
        path.name.removesuffix(".jsonl.gz") + ".manifest.json"
    )
    manifest.write_text(
        json.dumps(
            {
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "sha256_scope": "compressed_file_bytes",
                "schema_version": 1,
                "compression": "gzip",
                "data_file": path.name,
                "compressed_bytes": path.stat().st_size,
                "records": records,
                "capture_id": capture_id,
                "first_ordinal": first_ordinal,
                "last_ordinal": first_ordinal + records - 1,
            }
        ),
        encoding="utf-8",
    )
    return manifest


def _archive_record(
    ordinal: int = 1,
    *,
    capture_id: str = "capture-test",
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "capture_id": capture_id,
        "ordinal": ordinal,
        "event": {"event_type": "trade"},
    }


def test_loads_direct_and_gzip_archive_records_in_partition_order(
    tmp_path: Path,
) -> None:
    root = tmp_path / "archive"
    first = root / "2026" / "07" / "19" / "00.jsonl.gz"
    second = root / "2026" / "07" / "19" / "01.jsonl"
    first.parent.mkdir(parents=True)
    with gzip.open(first, "wt", encoding="utf-8") as handle:
        handle.write(json.dumps(_archive_record()))
        handle.write("\n")
    _write_manifest(first, 1)
    second.write_text(
        json.dumps({"ordinal": 2, "event_type": "orderbook"}) + "\n",
        encoding="utf-8",
    )
    (first.parent / "ignored.jsonl.gz.partial").write_bytes(b"not gzip")

    batch = load_hft_records(root)

    assert [row["ordinal"] for row in batch.records] == [1, 2]
    assert [item.records for item in batch.files] == [1, 1]
    assert all(len(item.sha256) == 64 for item in batch.files)
    assert len(batch.combined_sha256) == 64


def test_input_hash_covers_stored_compressed_bytes(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl.gz"
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        handle.write(json.dumps(_archive_record()) + "\n")
    _write_manifest(path, 1)

    batch = load_hft_records(path)

    assert batch.files[0].sha256 == hashlib.sha256(path.read_bytes()).hexdigest()
    assert batch.files[0].stored_bytes == path.stat().st_size
    assert batch.files[0].manifest_verified is True


def test_recorder_manifest_is_verified_before_reading(tmp_path: Path) -> None:
    path = tmp_path / "capture.jsonl.gz"
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        handle.write(json.dumps(_archive_record()) + "\n")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    manifest = _write_manifest(path, 1)

    batch = load_hft_records(path)

    assert batch.files[0].manifest_verified is True
    assert batch.files[0].manifest_path == manifest
    manifest.write_text(
        manifest.read_text(encoding="utf-8").replace(digest, "0" * 64),
        encoding="utf-8",
    )
    with pytest.raises(HFTWorkflowError, match="SHA-256 mismatch"):
        load_hft_records(path)


def test_unmanifested_gzip_is_never_treated_as_finalized_archive(
    tmp_path: Path,
) -> None:
    path = tmp_path / "orphan.jsonl.gz"
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        handle.write('{"value":1}\n')

    with pytest.raises(HFTWorkflowError, match="no finalized manifest"):
        load_hft_records(path)

    trusted = load_hft_records(path, allow_unmanifested_gzip=True)
    assert trusted.records == ({"value": 1},)
    assert trusted.files[0].manifest_verified is False


def test_manifest_partial_marks_gzip_as_uncommitted(tmp_path: Path) -> None:
    path = tmp_path / "capture.jsonl.gz"
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        handle.write(json.dumps(_archive_record()) + "\n")
    _write_manifest(path, 1)
    partial = tmp_path / "capture.manifest.json.partial"
    partial.write_text("unfinished", encoding="utf-8")

    with pytest.raises(HFTWorkflowError, match="uncommitted manifest partial"):
        load_hft_records(path)


def test_manifested_archive_rejects_ordinal_discontinuity(
    tmp_path: Path,
) -> None:
    first = tmp_path / "00.jsonl.gz"
    second = tmp_path / "01.jsonl.gz"
    with gzip.open(first, "wt", encoding="utf-8") as handle:
        handle.write(json.dumps(_archive_record(1)) + "\n")
    _write_manifest(first, 1, first_ordinal=1)
    with gzip.open(second, "wt", encoding="utf-8") as handle:
        handle.write(json.dumps(_archive_record(3)) + "\n")
    _write_manifest(second, 1, first_ordinal=3)

    with pytest.raises(
        HFTWorkflowError,
        match="ordinal discontinuity between partitions",
    ):
        load_hft_records(tmp_path)

    with gzip.open(second, "wt", encoding="utf-8") as handle:
        handle.write(json.dumps(_archive_record(2)) + "\n")
        handle.write(json.dumps(_archive_record(4)) + "\n")
    _write_manifest(second, 2, first_ordinal=2)
    with pytest.raises(
        HFTWorkflowError,
        match="ordinal discontinuity inside",
    ):
        load_hft_records(tmp_path)


def test_manifest_and_envelope_ordinals_require_json_integers(
    tmp_path: Path,
) -> None:
    path = tmp_path / "capture.jsonl.gz"
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        record = _archive_record()
        record["ordinal"] = 1.0
        handle.write(json.dumps(record) + "\n")
    manifest = _write_manifest(path, 1)

    with pytest.raises(HFTWorkflowError, match="ordinal must be an integer"):
        load_hft_records(path)

    with gzip.open(path, "wt", encoding="utf-8") as handle:
        handle.write(json.dumps(_archive_record()) + "\n")
    manifest = _write_manifest(path, 1)
    decoded = json.loads(manifest.read_text(encoding="utf-8"))
    decoded["first_ordinal"] = "1"
    manifest.write_text(json.dumps(decoded), encoding="utf-8")
    with pytest.raises(
        HFTWorkflowError,
        match="invalid ordinal/records metadata",
    ):
        load_hft_records(path)


def test_manifest_data_file_must_match_archive_relative_path(
    tmp_path: Path,
) -> None:
    path = tmp_path / "actual" / "capture.jsonl.gz"
    path.parent.mkdir()
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        handle.write(json.dumps(_archive_record()) + "\n")
    manifest = _write_manifest(path, 1)
    decoded = json.loads(manifest.read_text(encoding="utf-8"))
    decoded["data_file"] = "wrong/capture.jsonl.gz"
    manifest.write_text(json.dumps(decoded), encoding="utf-8")

    with pytest.raises(HFTWorkflowError, match="data_file does not match"):
        load_hft_records(path)


def test_reader_wraps_invalid_utf8_as_workflow_error(tmp_path: Path) -> None:
    path = tmp_path / "bad.jsonl"
    path.write_bytes(b'{"value":"\\xff"}\n'.replace(b"\\xff", b"\xff"))

    with pytest.raises(HFTWorkflowError, match="cannot read HFT input"):
        load_hft_records(path)


def test_record_limit_fails_instead_of_silently_truncating(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    path.write_text('{"value":1}\n{"value":2}\n', encoding="utf-8")

    with pytest.raises(HFTWorkflowError, match="exceeds max_records=1"):
        load_hft_records(path, max_records=1)


def test_single_pass_stream_profiles_only_after_complete_consumption(
    tmp_path: Path,
) -> None:
    path = tmp_path / "events.jsonl"
    path.write_text('{"value":1}\n{"value":2}\n', encoding="utf-8")
    stream = stream_hft_records(path)
    with pytest.raises(HFTWorkflowError, match="complete consumption"):
        stream.to_dict()

    assert [row["value"] for row in stream] == [1, 2]
    assert stream.completed
    assert stream.record_count == 2
    assert stream.to_dict()["files"][0]["records"] == 2
    with pytest.raises(HFTWorkflowError, match="single-use"):
        list(stream)


def test_hash_and_parse_use_one_open_file_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "events.jsonl"
    replacement = tmp_path / "replacement.jsonl"
    original_bytes = b'{"value":"original"}\n'
    path.write_bytes(original_bytes)
    replacement.write_text('{"value":"replacement"}\n', encoding="utf-8")
    real_verify = hft_workflow._load_and_verify_manifest

    def replace_path_after_hash(*args, **kwargs):
        result = real_verify(*args, **kwargs)
        os.replace(replacement, path)
        return result

    monkeypatch.setattr(
        hft_workflow,
        "_load_and_verify_manifest",
        replace_path_after_hash,
    )

    batch = load_hft_records(path)

    assert batch.records == ({"value": "original"},)
    assert batch.files[0].sha256 == hashlib.sha256(
        original_bytes
    ).hexdigest()
    assert json.loads(path.read_text(encoding="utf-8")) == {
        "value": "replacement"
    }


def test_reader_rejects_bad_json_and_symlink(tmp_path: Path) -> None:
    bad = tmp_path / "bad.jsonl"
    bad.write_text("{bad}\n", encoding="utf-8")
    with pytest.raises(HFTWorkflowError, match="invalid JSON"):
        load_hft_records(bad)

    target = tmp_path / "target.jsonl"
    target.write_text("{}\n", encoding="utf-8")
    link = tmp_path / "link.jsonl"
    link.symlink_to(target)
    with pytest.raises(HFTWorkflowError, match="symbolic link"):
        load_hft_records(link)


def test_atomic_writers_support_deterministic_gzip_and_strict_json(
    tmp_path: Path,
) -> None:
    output = tmp_path / "nested" / "rows.jsonl.gz"
    write_jsonl_atomic([{"b": 2, "a": 1}], output)
    with gzip.open(output, "rt", encoding="utf-8") as handle:
        assert json.loads(handle.readline()) == {"a": 1, "b": 2}
    assert not list(output.parent.glob("*.tmp"))

    summary = tmp_path / "summary.json"
    write_json_atomic({"rows": 1}, summary)
    assert json.loads(summary.read_text(encoding="utf-8")) == {"rows": 1}
    with pytest.raises(HFTWorkflowError, match="already exists"):
        write_json_atomic({"rows": 2}, summary)
    with pytest.raises(HFTWorkflowError, match="failed to write"):
        write_json_atomic({"bad": float("nan")}, tmp_path / "bad.json")


def test_atomic_writer_does_not_clobber_a_racing_destination(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination = tmp_path / "summary.json"
    real_link = hft_workflow.os.link

    def racing_link(source: Path, target: Path) -> None:
        destination.write_text('{"owner":"racer"}\n', encoding="utf-8")
        real_link(source, target)

    monkeypatch.setattr(hft_workflow.os, "link", racing_link)

    with pytest.raises(HFTWorkflowError, match="appeared during"):
        write_json_atomic({"owner": "coinpilot"}, destination)
    assert json.loads(destination.read_text(encoding="utf-8")) == {
        "owner": "racer"
    }


def test_staged_bundle_promotes_only_complete_sibling_files(
    tmp_path: Path,
) -> None:
    first = tmp_path / "first.json"
    second = tmp_path / "second.json"
    staged_first = artifact_staging_path(first)
    staged_second = artifact_staging_path(second)
    write_json_atomic({"value": 1}, staged_first)
    write_json_atomic({"value": 2}, staged_second)

    promoted = promote_staged_artifacts(
        {
            staged_first: first,
            staged_second: second,
        },
        overwrite=False,
    )

    assert promoted == (first, second)
    assert json.loads(first.read_text(encoding="utf-8")) == {"value": 1}
    assert json.loads(second.read_text(encoding="utf-8")) == {"value": 2}
    assert not staged_first.exists()
    assert not staged_second.exists()


def test_artifact_bundle_lock_serializes_separate_processes(
    tmp_path: Path,
) -> None:
    marker = tmp_path / "run.complete.json"
    context = multiprocessing.get_context("spawn")
    started = context.Event()
    acquired = context.Event()
    process = context.Process(
        target=_acquire_bundle_lock_in_child,
        args=(str(marker), started, acquired),
    )

    with artifact_bundle_lock(marker):
        process.start()
        assert started.wait(timeout=3)
        assert not acquired.wait(timeout=0.2)

    try:
        assert acquired.wait(timeout=3)
    finally:
        process.join(timeout=3)
        if process.is_alive():
            process.terminate()
            process.join(timeout=3)
    assert process.exitcode == 0


def test_completion_marker_commits_member_hashes_last(
    tmp_path: Path,
) -> None:
    rows = tmp_path / "rows.jsonl.gz"
    summary = tmp_path / "summary.json"
    marker = tmp_path / "run.complete.json"
    write_jsonl_atomic([{"value": 1}], rows)
    write_json_atomic({"rows": 1}, summary)

    write_artifact_completion(
        {"dataset": rows, "summary": summary},
        marker,
        metadata={"mode": "test"},
    )

    payload = json.loads(marker.read_text(encoding="utf-8"))
    assert payload["status"] == "complete"
    assert payload["metadata"]["mode"] == "test"
    assert payload["members"]["dataset"]["path"] == "rows.jsonl.gz"
    assert (
        payload["members"]["dataset"]["path_base"]
        == "completion_parent"
    )
    assert payload["members"]["dataset"]["sha256"] == hashlib.sha256(
        rows.read_bytes()
    ).hexdigest()
    assert verify_artifact_completion(marker)["status"] == "complete"

    rows.write_bytes(rows.read_bytes() + b"tampered")
    with pytest.raises(HFTWorkflowError, match="byte-count mismatch"):
        verify_artifact_completion(marker)
    invalidate_artifact_completion(marker)
    assert not marker.exists()
