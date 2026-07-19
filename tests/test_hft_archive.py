from __future__ import annotations

import gzip
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

import coinpilot.hft_archive as hft_archive
from coinpilot.hft_audit_journal import (
    DurableAuditJournal,
    HFTAuditJournalError,
    recover_stale_audit_journals,
)
from coinpilot.hft_archive import (
    HFTArchiveError,
    HFTArchiveProtocolError,
    UPBIT_PUBLIC_WEBSOCKET_URL,
    UpbitHFTArchiveRecorder,
    cleanup_stale_partials,
)


def _orderbook(timestamp_ms: int) -> str:
    return json.dumps(
        {
            "type": "orderbook",
            "code": "KRW-BTC",
            "timestamp": timestamp_ms,
            "total_ask_size": 3.0,
            "total_bid_size": 4.0,
            "orderbook_units": [
                {
                    "ask_price": 101.0,
                    "ask_size": 1.0,
                    "bid_price": 100.0,
                    "bid_size": 1.5,
                }
            ],
        }
    )


def _trade(timestamp_ms: int, sequence_id: int) -> str:
    return json.dumps(
        {
            "type": "trade",
            "code": "KRW-BTC",
            "trade_timestamp": timestamp_ms,
            "sequential_id": sequence_id,
            "trade_price": 100.5,
            "trade_volume": 0.01,
            "ask_bid": "BID",
            "best_ask_price": 101.0,
            "best_ask_size": 1.0,
            "best_bid_price": 100.0,
            "best_bid_size": 1.5,
        }
    )


class _Clock:
    def __init__(self, *, wall_ns: int, monotonic_ns: int = 0) -> None:
        self.wall = wall_ns
        self.monotonic = monotonic_ns
        self.sleeps: list[float] = []

    def wall_time_ns(self) -> int:
        return self.wall

    def monotonic_time_ns(self) -> int:
        return self.monotonic

    def advance(self, seconds: float) -> None:
        elapsed_ns = int(seconds * 1_000_000_000)
        self.wall += elapsed_ns
        self.monotonic += elapsed_ns

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.advance(seconds)


class _Advance:
    def __init__(self, clock: _Clock, seconds: float, value: Any) -> None:
        self.clock = clock
        self.seconds = seconds
        self.value = value

    def execute(self) -> Any:
        self.clock.advance(self.seconds)
        if isinstance(self.value, BaseException):
            raise self.value
        return self.value


class _FakeSocket:
    def __init__(self, actions: list[_Advance]) -> None:
        self.actions = list(actions)
        self.sent: list[str] = []
        self.timeouts: list[float] = []
        self.ping_count = 0
        self.closed = False

    def send(self, payload: str) -> None:
        self.sent.append(payload)
        if payload == "PING":
            self.ping_count += 1

    def settimeout(self, timeout: float) -> None:
        self.timeouts.append(timeout)

    def recv(self) -> Any:
        if not self.actions:
            raise AssertionError("fake socket ran out of receive actions")
        return self.actions.pop(0).execute()

    def close(self) -> None:
        self.closed = True


class _Factory:
    def __init__(self, sockets: list[_FakeSocket]) -> None:
        self.sockets = list(sockets)
        self.calls: list[tuple[str, float]] = []

    def __call__(self, url: str, *, timeout: float) -> _FakeSocket:
        self.calls.append((url, timeout))
        if not self.sockets:
            raise AssertionError("fake factory ran out of sockets")
        return self.sockets.pop(0)


def _read_envelopes(paths: tuple[Path, ...]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for path in paths:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            result.extend(json.loads(line) for line in handle)
    return result


def test_archive_fans_out_the_exact_persisted_envelope(tmp_path: Path) -> None:
    epoch_ns = int(
        datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1e9
    )
    clock = _Clock(wall_ns=epoch_ns)
    socket = _FakeSocket(
        [_Advance(clock, 0.1, _orderbook(epoch_ns // 1_000_000))]
    )
    recorder = UpbitHFTArchiveRecorder(
        output_root=tmp_path,
        socket_factory=_Factory([socket]),
        wall_time_ns=clock.wall_time_ns,
        monotonic_ns=clock.monotonic_time_ns,
        sleep=clock.sleep,
        random_float=lambda: 0.5,
    )
    consumed: list[dict[str, Any]] = []

    result = recorder.record(
        market="KRW-BTC",
        duration_seconds=5,
        max_events=1,
        capture_id="fanout-test",
        on_record=lambda envelope: consumed.append(dict(envelope)),
    )

    assert consumed == _read_envelopes(result.data_paths)
    assert consumed[0]["ordinal"] == 1
    assert consumed[0]["event"]["event_type"] == "orderbook"


def test_consumer_runs_only_after_audit_wal_fsync(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    epoch_ns = int(
        datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1e9
    )
    clock = _Clock(wall_ns=epoch_ns)
    socket = _FakeSocket(
        [_Advance(clock, 0.1, _orderbook(epoch_ns // 1_000_000))]
    )
    recorder = UpbitHFTArchiveRecorder(
        output_root=tmp_path,
        socket_factory=_Factory([socket]),
        wall_time_ns=clock.wall_time_ns,
        monotonic_ns=clock.monotonic_time_ns,
        sleep=clock.sleep,
        random_float=lambda: 0.5,
    )
    ordering: list[str] = []
    real_append = DurableAuditJournal.append_durable

    def observed_append(
        journal: DurableAuditJournal,
        envelope: dict[str, Any],
    ) -> str:
        ordering.append("wal_append")
        result = real_append(journal, envelope)
        ordering.append("wal_fsync_returned")
        return result

    monkeypatch.setattr(
        DurableAuditJournal,
        "append_durable",
        observed_append,
    )

    def consume(envelope: dict[str, Any]) -> None:
        ordering.append("consumer")
        journals = list((tmp_path / ".audit-journal").glob("*.audit.wal"))
        assert len(journals) == 1
        lines = journals[0].read_bytes().splitlines()
        assert len(lines) == 2
        frame = json.loads(lines[1])
        assert frame["payload"] == envelope
        assert frame["sha256"] == hashlib.sha256(
            json.dumps(
                envelope,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        ).hexdigest()
        # The final gzip bundle deliberately does not exist yet.
        assert not list(tmp_path.rglob("*.jsonl.gz"))

    result = recorder.record(
        market="KRW-BTC",
        duration_seconds=5,
        max_events=1,
        capture_id="durability-order-test",
        on_record=consume,
    )

    assert ordering == ["wal_append", "wal_fsync_returned", "consumer"]
    assert len(result.data_paths) == 1
    assert not list((tmp_path / ".audit-journal").glob("*.audit.wal"))
    manifest = json.loads(result.manifest_paths[0].read_text(encoding="utf-8"))
    assert manifest["audit_durability"] == "fsync_before_consumer"


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires POSIX fork")
def test_hard_exit_after_ledger_commit_recovers_exact_audit_record(
    tmp_path: Path,
) -> None:
    epoch_ns = int(
        datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1e9
    )
    ledger_marker = tmp_path / "durable-ledger-marker.json"
    process_id = os.fork()
    if process_id == 0:
        clock = _Clock(wall_ns=epoch_ns)
        socket = _FakeSocket(
            [_Advance(clock, 0.1, _orderbook(epoch_ns // 1_000_000))]
        )
        recorder = UpbitHFTArchiveRecorder(
            output_root=tmp_path,
            socket_factory=_Factory([socket]),
            wall_time_ns=clock.wall_time_ns,
            monotonic_ns=clock.monotonic_time_ns,
            sleep=clock.sleep,
            random_float=lambda: 0.5,
        )

        def commit_then_power_loss(envelope: dict[str, Any]) -> None:
            marker = json.dumps(
                {
                    "capture_id": envelope["capture_id"],
                    "connection_id": envelope["connection_id"],
                    "book_ordinal": envelope["ordinal"],
                },
                sort_keys=True,
            ).encode("utf-8")
            descriptor = os.open(
                ledger_marker,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
            )
            os.write(descriptor, marker)
            os.fsync(descriptor)
            os.close(descriptor)
            os._exit(37)

        recorder.record(
            market="KRW-BTC",
            duration_seconds=5,
            max_events=1,
            capture_id="hard-exit-test",
            on_record=commit_then_power_loss,
        )
        os._exit(99)

    _, wait_status = os.waitpid(process_id, 0)
    assert os.waitstatus_to_exitcode(wait_status) == 37
    assert ledger_marker.is_file()
    assert not list(tmp_path.rglob("*.jsonl.gz"))
    assert len(
        list((tmp_path / ".audit-journal").glob("*.audit.wal"))
    ) == 1

    recovery = recover_stale_audit_journals(tmp_path)

    assert len(recovery) == 1
    assert recovery[0].disposition == "recovered"
    assert recovery[0].records == 1
    assert recovery[0].data_path is not None
    envelopes = _read_envelopes((recovery[0].data_path,))
    marker = json.loads(ledger_marker.read_text(encoding="utf-8"))
    assert (
        envelopes[0]["capture_id"],
        envelopes[0]["connection_id"],
        envelopes[0]["ordinal"],
    ) == (
        marker["capture_id"],
        marker["connection_id"],
        marker["book_ordinal"],
    )
    manifest = json.loads(
        recovery[0].manifest_path.read_text(encoding="utf-8")
    )
    assert manifest["recovered_from_audit_journal"] is True
    assert manifest["audit_durability"] == "fsync_before_consumer"
    assert not list((tmp_path / ".audit-journal").glob("*.audit.wal"))


def test_recovery_ignores_unacknowledged_partial_wal_tail(
    tmp_path: Path,
) -> None:
    epoch_ns = int(
        datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1e9
    )
    writer = hft_archive._PartitionWriter(
        root=tmp_path,
        market="KRW-BTC",
        capture_id="partial-tail-test",
        partition_seconds=3_600,
        received_wall_ns=epoch_ns,
        segment=1,
        counter_baseline={},
        durable_consumer=True,
    )
    envelope = {
        "schema_version": 1,
        "capture_id": "partial-tail-test",
        "connection_id": "partial-tail-test-connection-000001",
        "ordinal": 1,
        "received_wall_ns": str(epoch_ns),
        "received_monotonic_ns": "100",
        "receive_delta_monotonic_ns": None,
        "monotonic_regression": False,
        "gap_before": False,
        "gap_reason": None,
        "gap_duration_monotonic_ns": None,
        "event": {
            "event_type": "orderbook",
            "market": "KRW-BTC",
        },
    }
    writer.write(envelope)
    writer.abort()
    journal_path = next(
        (tmp_path / ".audit-journal").glob("*.audit.wal")
    )
    with journal_path.open("ab", buffering=0) as handle:
        handle.write(b'{"kind":"coinpilot_hft_audit_record"')

    recovery = recover_stale_audit_journals(tmp_path)

    assert recovery[0].disposition == "recovered"
    assert recovery[0].records == 1
    assert recovery[0].quarantined_path is not None
    assert recovery[0].quarantined_path.is_file()
    assert _read_envelopes((recovery[0].data_path,)) == [envelope]


def test_recovery_accepts_already_committed_archive_without_duplicate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    epoch_ns = int(
        datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1e9
    )
    clock = _Clock(wall_ns=epoch_ns)
    socket = _FakeSocket(
        [_Advance(clock, 0.1, _orderbook(epoch_ns // 1_000_000))]
    )
    recorder = UpbitHFTArchiveRecorder(
        output_root=tmp_path,
        socket_factory=_Factory([socket]),
        wall_time_ns=clock.wall_time_ns,
        monotonic_ns=clock.monotonic_time_ns,
        sleep=clock.sleep,
        random_float=lambda: 0.5,
    )

    def preserve_after_commit(journal: DurableAuditJournal) -> bool:
        journal.close_preserving()
        return False

    monkeypatch.setattr(
        DurableAuditJournal,
        "discard_after_archive_commit",
        preserve_after_commit,
    )
    archive = recorder.record(
        market="KRW-BTC",
        duration_seconds=5,
        max_events=1,
        capture_id="already-committed-test",
        on_record=lambda envelope: None,
    )
    original_data = archive.data_paths[0].read_bytes()
    assert len(
        list((tmp_path / ".audit-journal").glob("*.audit.wal"))
    ) == 1

    recovery = recover_stale_audit_journals(tmp_path)

    assert recovery[0].disposition == "already_committed"
    assert archive.data_paths[0].read_bytes() == original_data
    assert len(list(tmp_path.rglob("*.jsonl.gz"))) == 1
    assert not list((tmp_path / ".audit-journal").glob("*.audit.wal"))


def test_complete_wal_checksum_corruption_is_quarantined_fail_closed(
    tmp_path: Path,
) -> None:
    epoch_ns = int(
        datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1e9
    )
    writer = hft_archive._PartitionWriter(
        root=tmp_path,
        market="KRW-BTC",
        capture_id="corrupt-wal-test",
        partition_seconds=3_600,
        received_wall_ns=epoch_ns,
        segment=1,
        counter_baseline={},
        durable_consumer=True,
    )
    writer.write(
        {
            "schema_version": 1,
            "capture_id": "corrupt-wal-test",
            "connection_id": "corrupt-wal-test-connection-000001",
            "ordinal": 1,
            "received_wall_ns": str(epoch_ns),
            "received_monotonic_ns": "100",
            "receive_delta_monotonic_ns": None,
            "monotonic_regression": False,
            "gap_before": False,
            "gap_reason": None,
            "gap_duration_monotonic_ns": None,
            "event": {
                "event_type": "orderbook",
                "market": "KRW-BTC",
            },
        }
    )
    writer.abort()
    journal_path = next(
        (tmp_path / ".audit-journal").glob("*.audit.wal")
    )
    lines = journal_path.read_bytes().splitlines()
    frame = json.loads(lines[1])
    frame["sha256"] = "0" * 64
    journal_path.write_bytes(
        lines[0]
        + b"\n"
        + json.dumps(
            frame,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
    )

    with pytest.raises(HFTAuditJournalError, match="quarantined"):
        recover_stale_audit_journals(tmp_path)

    assert not list(tmp_path.rglob("*.jsonl.gz"))
    quarantined = list(
        (tmp_path / ".audit-quarantine").glob("*.quarantined")
    )
    assert len(quarantined) == 1
    assert not list((tmp_path / ".audit-journal").glob("*.audit.wal"))


def test_consumer_failure_stops_but_preserves_the_audit_record(
    tmp_path: Path,
) -> None:
    epoch_ns = int(
        datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1e9
    )
    clock = _Clock(wall_ns=epoch_ns)
    socket = _FakeSocket(
        [_Advance(clock, 0.1, _orderbook(epoch_ns // 1_000_000))]
    )
    recorder = UpbitHFTArchiveRecorder(
        output_root=tmp_path,
        socket_factory=_Factory([socket]),
        wall_time_ns=clock.wall_time_ns,
        monotonic_ns=clock.monotonic_time_ns,
        sleep=clock.sleep,
        random_float=lambda: 0.5,
    )

    def fail_consumer(envelope: dict[str, Any]) -> None:
        raise RuntimeError(f"injected failure at {envelope['ordinal']}")

    with pytest.raises(HFTArchiveError, match="consumer failed"):
        recorder.record(
            market="KRW-BTC",
            duration_seconds=5,
            max_events=1,
            capture_id="fanout-failure-test",
            on_record=fail_consumer,
        )

    data_paths = tuple(tmp_path.rglob("*.jsonl.gz"))
    assert len(data_paths) == 1
    assert _read_envelopes(data_paths)[0]["ordinal"] == 1
    assert not list(tmp_path.rglob("*.partial"))


def test_interrupted_partition_write_is_tainted_and_never_finalized(
    tmp_path: Path,
) -> None:
    epoch_ns = int(
        datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1e9
    )
    writer = hft_archive._PartitionWriter(
        root=tmp_path,
        market="KRW-BTC",
        capture_id="interrupt-write-test",
        partition_seconds=3_600,
        received_wall_ns=epoch_ns,
        segment=1,
        counter_baseline={},
    )
    original_gzip = writer._gzip_handle

    class _InterruptAfterPartialWrite:
        def write(self, payload: bytes) -> None:
            original_gzip.write(payload[: max(1, len(payload) // 2)])
            raise KeyboardInterrupt

        def close(self) -> None:
            original_gzip.close()

    writer._gzip_handle = _InterruptAfterPartialWrite()
    envelope = {
        "ordinal": 1,
        "received_wall_ns": str(epoch_ns),
        "received_monotonic_ns": "1",
        "connection_id": "interrupt-write-test-connection-000001",
        "gap_before": False,
        "event": {"event_type": "orderbook"},
    }

    with pytest.raises(KeyboardInterrupt):
        writer.write(envelope)
    with pytest.raises(HFTArchiveError, match="tainted"):
        writer.finalize(counters={})

    assert not list(tmp_path.rglob("*.jsonl.gz"))
    assert not list(tmp_path.rglob("*.manifest.json"))
    assert not list(tmp_path.rglob("*.partial"))


def test_interrupt_after_data_promotion_rolls_back_both_final_names(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    epoch_ns = int(
        datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1e9
    )
    writer = hft_archive._PartitionWriter(
        root=tmp_path,
        market="KRW-BTC",
        capture_id="interrupt-promote-test",
        partition_seconds=3_600,
        received_wall_ns=epoch_ns,
        segment=1,
        counter_baseline={},
    )
    writer.write(
        {
            "ordinal": 1,
            "received_wall_ns": str(epoch_ns),
            "received_monotonic_ns": "1",
            "connection_id": "interrupt-promote-test-connection-000001",
            "gap_before": False,
            "event": {"event_type": "orderbook"},
        }
    )
    real_replace = os.replace

    def interrupt_after_replace(source: Path, destination: Path) -> None:
        real_replace(source, destination)
        if str(destination).endswith(".jsonl.gz"):
            raise KeyboardInterrupt

    monkeypatch.setattr(hft_archive.os, "replace", interrupt_after_replace)

    with pytest.raises(KeyboardInterrupt):
        writer.finalize(counters={})

    assert not list(tmp_path.rglob("*.jsonl.gz"))
    assert not list(tmp_path.rglob("*.manifest.json"))
    assert not list(tmp_path.rglob("*.partial"))


def test_required_directory_fsync_failure_rolls_back_partition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    epoch_ns = int(
        datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1e9
    )
    writer = hft_archive._PartitionWriter(
        root=tmp_path,
        market="KRW-BTC",
        capture_id="fsync-failure-test",
        partition_seconds=3_600,
        received_wall_ns=epoch_ns,
        segment=1,
        counter_baseline={},
    )
    writer.write(
        {
            "ordinal": 1,
            "received_wall_ns": str(epoch_ns),
            "received_monotonic_ns": "1",
            "connection_id": "fsync-failure-test-connection-000001",
            "gap_before": False,
            "event": {"event_type": "orderbook"},
        }
    )
    real_fsync_directory = hft_archive._fsync_directory

    def fail_required_fsync(path: Path, *, required: bool = False) -> None:
        if required:
            raise HFTArchiveError("injected directory fsync failure")
        real_fsync_directory(path, required=required)

    monkeypatch.setattr(
        hft_archive,
        "_fsync_directory",
        fail_required_fsync,
    )

    with pytest.raises(HFTArchiveError, match="injected"):
        writer.finalize(counters={})

    assert not list(tmp_path.rglob("*.jsonl.gz"))
    assert not list(tmp_path.rglob("*.manifest.json"))
    assert not list(tmp_path.rglob("*.partial"))


def test_reconnect_creates_connection_and_gap_metadata(tmp_path: Path) -> None:
    epoch_ns = int(
        datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1e9
    )
    clock = _Clock(wall_ns=epoch_ns)
    first = _FakeSocket(
        [
            _Advance(clock, 0.1, _orderbook(epoch_ns // 1_000_000)),
            _Advance(clock, 0.2, ConnectionResetError("dropped")),
        ]
    )
    second = _FakeSocket(
        [
            _Advance(
                clock,
                0.1,
                _trade(epoch_ns // 1_000_000 + 400, 11),
            )
        ]
    )
    factory = _Factory([first, second])
    recorder = UpbitHFTArchiveRecorder(
        output_root=tmp_path,
        socket_factory=factory,
        wall_time_ns=clock.wall_time_ns,
        monotonic_ns=clock.monotonic_time_ns,
        sleep=clock.sleep,
        random_float=lambda: 0.5,
        backoff_initial_seconds=0.1,
        backoff_cap_seconds=0.2,
        gap_threshold_seconds=5,
    )

    result = recorder.record(
        market="KRW-BTC",
        duration_seconds=10,
        max_events=2,
        capture_id="reconnect-test",
    )
    envelopes = _read_envelopes(result.data_paths)

    assert result.connection_count == 2
    assert result.reconnect_count == 1
    assert result.error_counts["receive_errors"] == 1
    assert result.gap_count == 1
    assert len(envelopes) == 2
    assert envelopes[0]["ordinal"] == 1
    assert envelopes[1]["ordinal"] == 2
    assert envelopes[0]["connection_id"] != envelopes[1]["connection_id"]
    assert envelopes[0]["gap_before"] is False
    assert envelopes[1]["gap_before"] is True
    assert envelopes[1]["gap_reason"] == "receive_error"
    assert envelopes[1]["gap_duration_monotonic_ns"] == "400000000"
    assert envelopes[1]["receive_delta_monotonic_ns"] == "400000000"
    assert isinstance(envelopes[1]["received_wall_ns"], str)
    assert isinstance(envelopes[1]["received_monotonic_ns"], str)
    assert first.closed and second.closed
    assert all(call[0] == UPBIT_PUBLIC_WEBSOCKET_URL for call in factory.calls)
    subscription = json.loads(first.sent[0])
    assert {item.get("type") for item in subscription if "type" in item} == {
        "orderbook",
        "trade",
    }
    assert all("authorization" not in item for item in subscription)


def test_utc_partition_finalize_and_sha256_manifest(tmp_path: Path) -> None:
    start = datetime(2026, 1, 1, 0, 59, 59, 800_000, tzinfo=timezone.utc)
    epoch_ns = int(start.timestamp() * 1e9)
    clock = _Clock(wall_ns=epoch_ns)
    socket = _FakeSocket(
        [
            _Advance(clock, 0.1, _orderbook(epoch_ns // 1_000_000 + 100)),
            _Advance(clock, 0.2, _trade(epoch_ns // 1_000_000 + 300, 12)),
        ]
    )
    recorder = UpbitHFTArchiveRecorder(
        output_root=tmp_path,
        socket_factory=_Factory([socket]),
        wall_time_ns=clock.wall_time_ns,
        monotonic_ns=clock.monotonic_time_ns,
        sleep=clock.sleep,
        random_float=lambda: 0.5,
    )

    result = recorder.record(
        market="KRW-BTC",
        duration_seconds=5,
        max_events=2,
        capture_id="partition-test",
    )

    assert len(result.data_paths) == 2
    assert len(result.manifest_paths) == 2
    assert "hour=00" in result.data_paths[0].as_posix()
    assert "hour=01" in result.data_paths[1].as_posix()
    for data_path, manifest_path in zip(
        result.data_paths,
        result.manifest_paths,
    ):
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        digest = hashlib.sha256(data_path.read_bytes()).hexdigest()
        assert manifest["sha256"] == digest
        assert manifest["sha256_scope"] == "compressed_file_bytes"
        assert manifest["compressed_bytes"] == data_path.stat().st_size
        assert manifest["records"] == 1
        assert manifest["data_file"] == data_path.relative_to(tmp_path).as_posix()
        assert manifest["first_received_wall_ns"].isdigit()
        assert manifest["first_received_monotonic_ns"].isdigit()
    assert not list(tmp_path.rglob("*.partial"))


def test_idle_timeout_sends_ping_and_accepts_upbit_heartbeat(
    tmp_path: Path,
) -> None:
    epoch_ns = int(
        datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1e9
    )
    clock = _Clock(wall_ns=epoch_ns)
    socket = _FakeSocket(
        [
            _Advance(clock, 1.1, TimeoutError()),
            _Advance(clock, 0.1, json.dumps({"status": "UP"})),
            _Advance(clock, 0.1, _trade(epoch_ns // 1_000_000 + 1_300, 21)),
        ]
    )
    recorder = UpbitHFTArchiveRecorder(
        output_root=tmp_path,
        socket_factory=_Factory([socket]),
        wall_time_ns=clock.wall_time_ns,
        monotonic_ns=clock.monotonic_time_ns,
        sleep=clock.sleep,
        idle_ping_seconds=1,
        idle_reconnect_seconds=3,
        random_float=lambda: 0.5,
    )

    result = recorder.record(
        market="KRW-BTC",
        duration_seconds=5,
        max_events=1,
    )

    assert socket.ping_count == 1
    assert result.pings_sent == 1
    assert result.heartbeats_received == 1
    assert result.raw_messages == 2
    assert result.written_events == 1


def test_upbit_protocol_error_is_explicit_and_preserves_healthy_partition(
    tmp_path: Path,
) -> None:
    epoch_ns = int(
        datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1e9
    )
    clock = _Clock(wall_ns=epoch_ns)
    socket = _FakeSocket(
        [
            _Advance(clock, 0.1, _orderbook(epoch_ns // 1_000_000)),
            _Advance(
                clock,
                0.1,
                json.dumps(
                    {
                        "error": {
                            "name": "WRONG_FORMAT",
                            "message": "request format is invalid",
                        }
                    }
                ),
            ),
        ]
    )
    recorder = UpbitHFTArchiveRecorder(
        output_root=tmp_path,
        socket_factory=_Factory([socket]),
        wall_time_ns=clock.wall_time_ns,
        monotonic_ns=clock.monotonic_time_ns,
        sleep=clock.sleep,
    )

    with pytest.raises(
        HFTArchiveProtocolError,
        match="WRONG_FORMAT.*request format is invalid",
    ) as raised:
        recorder.record(
            market="KRW-BTC",
            duration_seconds=5,
            capture_id="protocol-test",
        )

    assert raised.value.error_name == "WRONG_FORMAT"
    assert socket.closed
    data_paths = tuple(tmp_path.rglob("*.jsonl.gz"))
    manifest_paths = tuple(tmp_path.rglob("*.manifest.json"))
    assert len(data_paths) == 1
    assert len(_read_envelopes(data_paths)) == 1
    manifest = json.loads(manifest_paths[0].read_text(encoding="utf-8"))
    assert manifest["counters_cumulative"]["protocol_errors"] == 1
    assert manifest["counter_deltas"]["protocol_errors"] == 1
    assert not list(tmp_path.rglob("*.partial"))


@pytest.mark.parametrize(
    "bad_message",
    [
        json.dumps({"type": "ticker", "code": "KRW-BTC"}),
        b"not-json",
    ],
)
def test_normalization_drop_marks_next_valid_event_as_gap(
    tmp_path: Path,
    bad_message: object,
) -> None:
    epoch_ns = int(
        datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1e9
    )
    clock = _Clock(wall_ns=epoch_ns)
    socket = _FakeSocket(
        [
            _Advance(clock, 0.1, bad_message),
            _Advance(clock, 0.1, _orderbook(epoch_ns // 1_000_000 + 200)),
        ]
    )
    recorder = UpbitHFTArchiveRecorder(
        output_root=tmp_path,
        socket_factory=_Factory([socket]),
        wall_time_ns=clock.wall_time_ns,
        monotonic_ns=clock.monotonic_time_ns,
        sleep=clock.sleep,
    )

    result = recorder.record(
        market="KRW-BTC",
        duration_seconds=5,
        max_events=1,
    )
    envelope = _read_envelopes(result.data_paths)[0]

    assert result.rejected_messages == 1
    assert result.error_counts["normalization_errors"] == 1
    assert result.gap_count == 1
    assert envelope["ordinal"] == 1
    assert envelope["gap_before"] is True
    assert envelope["gap_reason"] == "normalization_error"
    assert envelope["gap_duration_monotonic_ns"] is None


def test_exponential_backoff_is_jittered_and_capped(tmp_path: Path) -> None:
    recorder = UpbitHFTArchiveRecorder(
        output_root=tmp_path,
        socket_factory=lambda *_args, **_kwargs: None,
        random_float=lambda: 1.0,
        backoff_initial_seconds=1,
        backoff_cap_seconds=3,
        backoff_jitter_ratio=0.2,
    )

    assert recorder._backoff_delay(1) == pytest.approx(1.2)
    assert recorder._backoff_delay(2) == pytest.approx(2.4)
    assert recorder._backoff_delay(3) == pytest.approx(3.0)
    assert recorder._backoff_delay(20) == pytest.approx(3.0)


def test_write_failure_aborts_and_removes_partial(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    epoch_ns = int(
        datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1e9
    )
    clock = _Clock(wall_ns=epoch_ns)
    socket = _FakeSocket(
        [_Advance(clock, 0.1, _orderbook(epoch_ns // 1_000_000))]
    )
    original_write = hft_archive._PartitionWriter.write

    def failing_write(
        writer: hft_archive._PartitionWriter,
        envelope: dict[str, Any],
    ) -> None:
        original_write(writer, envelope)
        raise OSError("simulated disk fault")

    monkeypatch.setattr(
        hft_archive._PartitionWriter,
        "write",
        failing_write,
    )
    recorder = UpbitHFTArchiveRecorder(
        output_root=tmp_path,
        socket_factory=_Factory([socket]),
        wall_time_ns=clock.wall_time_ns,
        monotonic_ns=clock.monotonic_time_ns,
        sleep=clock.sleep,
    )

    with pytest.raises(HFTArchiveError, match="archive recorder failed"):
        recorder.record(
            market="KRW-BTC",
            duration_seconds=5,
            max_events=1,
        )

    assert not list(tmp_path.rglob("*.partial"))
    assert not list(tmp_path.rglob("*.jsonl.gz"))
    assert not list(tmp_path.rglob("*.manifest.json"))


def test_keyboard_interrupt_finalizes_healthy_nonempty_partition(
    tmp_path: Path,
) -> None:
    epoch_ns = int(
        datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1e9
    )
    clock = _Clock(wall_ns=epoch_ns)
    socket = _FakeSocket(
        [
            _Advance(clock, 0.1, _orderbook(epoch_ns // 1_000_000)),
            _Advance(clock, 0.1, KeyboardInterrupt()),
        ]
    )
    recorder = UpbitHFTArchiveRecorder(
        output_root=tmp_path,
        socket_factory=_Factory([socket]),
        wall_time_ns=clock.wall_time_ns,
        monotonic_ns=clock.monotonic_time_ns,
        sleep=clock.sleep,
    )

    with pytest.raises(KeyboardInterrupt):
        recorder.record(
            market="KRW-BTC",
            duration_seconds=5,
            capture_id="interrupt-test",
        )

    data_paths = tuple(tmp_path.rglob("*.jsonl.gz"))
    manifest_paths = tuple(tmp_path.rglob("*.manifest.json"))
    assert len(data_paths) == 1
    assert len(manifest_paths) == 1
    assert len(_read_envelopes(data_paths)) == 1
    manifest = json.loads(manifest_paths[0].read_text(encoding="utf-8"))
    assert manifest["sha256"] == hashlib.sha256(
        data_paths[0].read_bytes()
    ).hexdigest()
    assert not list(tmp_path.rglob("*.partial"))


def test_keyboard_interrupt_aborts_partial_if_finalize_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    epoch_ns = int(
        datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1e9
    )
    clock = _Clock(wall_ns=epoch_ns)
    socket = _FakeSocket(
        [
            _Advance(clock, 0.1, _orderbook(epoch_ns // 1_000_000)),
            _Advance(clock, 0.1, KeyboardInterrupt()),
        ]
    )

    def failing_finalize(
        _writer: hft_archive._PartitionWriter,
        *,
        counters: dict[str, int],
    ) -> hft_archive._FinalizedPartition:
        del counters
        raise OSError("simulated finalize fault")

    monkeypatch.setattr(
        hft_archive._PartitionWriter,
        "finalize",
        failing_finalize,
    )
    recorder = UpbitHFTArchiveRecorder(
        output_root=tmp_path,
        socket_factory=_Factory([socket]),
        wall_time_ns=clock.wall_time_ns,
        monotonic_ns=clock.monotonic_time_ns,
        sleep=clock.sleep,
    )

    with pytest.raises(KeyboardInterrupt):
        recorder.record(
            market="KRW-BTC",
            duration_seconds=5,
        )

    assert not list(tmp_path.rglob("*.partial"))
    assert not list(tmp_path.rglob("*.jsonl.gz"))
    assert not list(tmp_path.rglob("*.manifest.json"))


def test_stop_predicate_returns_after_finalizing_current_partition(
    tmp_path: Path,
) -> None:
    epoch_ns = int(
        datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1e9
    )
    clock = _Clock(wall_ns=epoch_ns)
    socket = _FakeSocket(
        [_Advance(clock, 0.1, _orderbook(epoch_ns // 1_000_000))]
    )
    checks = 0

    def stop_requested() -> bool:
        nonlocal checks
        checks += 1
        return checks >= 2

    recorder = UpbitHFTArchiveRecorder(
        output_root=tmp_path,
        socket_factory=_Factory([socket]),
        wall_time_ns=clock.wall_time_ns,
        monotonic_ns=clock.monotonic_time_ns,
        sleep=clock.sleep,
    )
    result = recorder.record(
        market="KRW-BTC",
        duration_seconds=5,
        stop_requested=stop_requested,
    )

    assert result.written_events == 1
    assert len(result.data_paths) == 1
    assert not list(tmp_path.rglob("*.partial"))


def test_cleanup_stale_partials_skips_symlinks(tmp_path: Path) -> None:
    old_partial = tmp_path / "old.jsonl.gz.partial"
    old_partial.write_bytes(b"partial")
    os.utime(old_partial, (10, 10))
    outside = tmp_path / "outside"
    outside.write_bytes(b"do-not-delete")
    link = tmp_path / "link.jsonl.gz.partial"
    try:
        link.symlink_to(outside)
    except OSError:
        pytest.skip("symlinks are unavailable")

    removed = cleanup_stale_partials(
        tmp_path,
        older_than_seconds=50,
        now_seconds=lambda: 100,
    )

    assert removed == (old_partial,)
    assert not old_partial.exists()
    assert link.is_symlink()
    assert outside.read_bytes() == b"do-not-delete"


def test_output_root_and_capture_id_reject_path_tricks(tmp_path: Path) -> None:
    symlink = tmp_path / "archive-link"
    try:
        symlink.symlink_to(tmp_path / "real", target_is_directory=True)
    except OSError:
        pytest.skip("symlinks are unavailable")

    with pytest.raises(HFTArchiveError, match="non-symlink"):
        UpbitHFTArchiveRecorder(
            output_root=symlink,
            socket_factory=lambda *_args, **_kwargs: None,
        )

    recorder = UpbitHFTArchiveRecorder(
        output_root=tmp_path / "safe",
        socket_factory=lambda *_args, **_kwargs: None,
    )
    with pytest.raises(HFTArchiveError, match="capture_id"):
        recorder.record(
            market="KRW-BTC",
            duration_seconds=1,
            capture_id="../escape",
        )
