from __future__ import annotations

import dataclasses
from pathlib import Path

from coinpilot.config import AppConfig
from coinpilot.hft_archive import HFTArchiveResult
from coinpilot import shadow_service


def _book(ordinal: int, *, imbalance: float) -> dict:
    ask_size, bid_size = (
        (1.0, 3.0) if imbalance > 0 else (3.0, 1.0)
    )
    wall_ns = 1_800_000_000_000_000_000 + ordinal * 100_000_000
    monotonic_ns = ordinal * 100_000_000
    return {
        "schema_version": 1,
        "capture_id": "service-smoke",
        "connection_id": "service-smoke-connection-000001",
        "ordinal": ordinal,
        "received_wall_ns": str(wall_ns),
        "received_monotonic_ns": str(monotonic_ns),
        "receive_delta_monotonic_ns": (
            None if ordinal == 1 else "100000000"
        ),
        "monotonic_regression": False,
        "gap_before": False,
        "gap_reason": None,
        "gap_duration_monotonic_ns": None,
        "event": {
            "schema_version": 1,
            "event_type": "orderbook",
            "market": "KRW-BTC",
            "exchange_timestamp_ms": 1_800_000_000_000 + ordinal * 100,
            "received_at_ns": str(wall_ns),
            "sequence_id": None,
            "best_ask_price": 100.1,
            "best_ask_size": ask_size,
            "best_bid_price": 100.0,
            "best_bid_size": bid_size,
            "total_ask_size": ask_size,
            "total_bid_size": bid_size,
            "depth": 1,
            "levels": [
                {
                    "level": 1,
                    "ask_price": 100.1,
                    "ask_size": ask_size,
                    "bid_price": 100.0,
                    "bid_size": bid_size,
                }
            ],
        },
    }


def _trade_with_gap(ordinal: int) -> dict:
    wall_ns = 1_800_000_000_000_000_000 + ordinal * 100_000_000
    return {
        "schema_version": 1,
        "capture_id": "service-smoke",
        "connection_id": "service-smoke-connection-000001",
        "ordinal": ordinal,
        "received_wall_ns": str(wall_ns),
        "received_monotonic_ns": str(ordinal * 100_000_000),
        "receive_delta_monotonic_ns": "100000000",
        "monotonic_regression": False,
        "gap_before": True,
        "gap_reason": "receive_interval",
        "gap_duration_monotonic_ns": "100000000",
        "event": {
            "schema_version": 1,
            "event_type": "trade",
            "market": "KRW-BTC",
            "trade_price": 100.0,
            "trade_volume": 0.1,
            "aggressor_side": "buy",
            "sequence_id": str(ordinal),
        },
    }


def _config(tmp_path: Path) -> AppConfig:
    base = AppConfig()
    return dataclasses.replace(
        base,
        shadow=dataclasses.replace(
            base.shadow,
            mode="diagnostic",
            database_path=str(tmp_path / "shadow.db"),
            archive_root=str(tmp_path / "archive"),
            warmup_books=1,
            entry_threshold=0.30,
            exit_threshold=0.0,
            max_spread_bps=20.0,
            latency_ms=0.0,
            session_seconds=10.0,
        ),
        operations=dataclasses.replace(
            base.operations,
            heartbeat_seconds=1,
            stale_after_seconds=2,
            backup_dir=str(tmp_path / "backups"),
        ),
    ).validate()


def test_service_archives_and_completes_a_zero_order_shadow_round_trip(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = _config(tmp_path)

    def fake_record(**kwargs):
        for record in (
            _book(1, imbalance=1),
            _book(2, imbalance=1),
            _book(3, imbalance=1),
            _book(4, imbalance=-1),
            _book(5, imbalance=-1),
        ):
            kwargs["on_record"](record)
        root = Path(kwargs["output_root"])
        data = root / "fake.jsonl.gz"
        manifest = root / "fake.manifest.json"
        data.write_bytes(b"public-feed-audit")
        manifest.write_text('{"schema_version":1}', encoding="utf-8")
        return HFTArchiveResult(
            capture_id=kwargs["capture_id"],
            market=kwargs["market"],
            requested_duration_seconds=kwargs["duration_seconds"],
            elapsed_monotonic_seconds=0.5,
            raw_messages=5,
            written_events=5,
            rejected_messages=0,
            connection_count=1,
            reconnect_count=0,
            gap_count=0,
            pings_sent=0,
            heartbeats_received=0,
            error_counts={},
            data_paths=(data,),
            manifest_paths=(manifest,),
        )

    monkeypatch.setattr(
        shadow_service,
        "record_upbit_public_archive",
        fake_record,
    )
    result = shadow_service.run_shadow_service(
        config,
        capture_id="service-smoke",
    )

    assert result.orders_sent == 0
    assert result.status["orders_sent"] == 0
    assert result.status["fills"] == 2
    assert result.status["base_quantity"] == 0
    assert result.run_completion_path.exists()
    assert result.run_summary_path.exists()


def test_service_daily_loss_halt_survives_gap_without_consumer_error(
    tmp_path: Path,
    monkeypatch,
) -> None:
    base = _config(tmp_path)
    config = dataclasses.replace(
        base,
        shadow=dataclasses.replace(
            base.shadow,
            max_daily_loss_pct=0.000000001,
        ),
    ).validate()

    def fake_record(**kwargs):
        records = (
            _book(1, imbalance=1),
            _book(2, imbalance=1),
            _book(3, imbalance=1),
            _book(4, imbalance=-1),
            _book(5, imbalance=-1),
            _trade_with_gap(6),
            _book(7, imbalance=1),
            _book(8, imbalance=1),
        )
        for record in records:
            kwargs["on_record"](record)
        root = Path(kwargs["output_root"])
        data = root / "fake.jsonl.gz"
        manifest = root / "fake.manifest.json"
        data.write_bytes(b"public-feed-halt-gap-audit")
        manifest.write_text('{"schema_version":1}', encoding="utf-8")
        return HFTArchiveResult(
            capture_id=kwargs["capture_id"],
            market=kwargs["market"],
            requested_duration_seconds=kwargs["duration_seconds"],
            elapsed_monotonic_seconds=0.8,
            raw_messages=len(records),
            written_events=len(records),
            rejected_messages=0,
            connection_count=1,
            reconnect_count=0,
            gap_count=1,
            pings_sent=0,
            heartbeats_received=0,
            error_counts={"consumer_errors": 0},
            data_paths=(data,),
            manifest_paths=(manifest,),
        )

    monkeypatch.setattr(
        shadow_service,
        "record_upbit_public_archive",
        fake_record,
    )
    result = shadow_service.run_shadow_service(
        config,
        capture_id="service-smoke",
    )

    assert result.archive.error_counts["consumer_errors"] == 0
    assert result.status["lifecycle_status"] == "stopped"
    assert result.status["halt_reason"] == "external_halt:daily_loss"
    assert result.status["orders_sent"] == 0


def test_gap_marker_on_intervening_trade_expires_pending_order(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = _config(tmp_path)

    def fake_record(**kwargs):
        for record in (
            _book(1, imbalance=1),
            _book(2, imbalance=1),
            _trade_with_gap(3),
            _book(4, imbalance=1),
        ):
            kwargs["on_record"](record)
        root = Path(kwargs["output_root"])
        data = root / "fake.jsonl.gz"
        manifest = root / "fake.manifest.json"
        data.write_bytes(b"public-feed-gap-audit")
        manifest.write_text('{"schema_version":1}', encoding="utf-8")
        return HFTArchiveResult(
            capture_id=kwargs["capture_id"],
            market=kwargs["market"],
            requested_duration_seconds=kwargs["duration_seconds"],
            elapsed_monotonic_seconds=0.4,
            raw_messages=4,
            written_events=4,
            rejected_messages=0,
            connection_count=1,
            reconnect_count=0,
            gap_count=1,
            pings_sent=0,
            heartbeats_received=0,
            error_counts={},
            data_paths=(data,),
            manifest_paths=(manifest,),
        )

    monkeypatch.setattr(
        shadow_service,
        "record_upbit_public_archive",
        fake_record,
    )
    result = shadow_service.run_shadow_service(
        config,
        capture_id="service-smoke",
    )

    assert result.status["fills"] == 0
    assert result.status["pending_orders"] == 0
    assert result.status["base_quantity"] == 0
