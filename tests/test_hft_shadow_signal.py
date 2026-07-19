from __future__ import annotations

import pytest

from coinpilot.hft_shadow_signal import OnlineDiagnosticSignal


def _book(
    ordinal: int,
    *,
    connection: str = "conn-1",
    gap: bool = False,
) -> dict:
    return {
        "schema_version": 1,
        "capture_id": "capture-1",
        "connection_id": connection,
        "ordinal": ordinal,
        "received_wall_ns": str(1_000_000_000 + ordinal),
        "received_monotonic_ns": str(ordinal * 100_000_000),
        "monotonic_regression": False,
        "gap_before": gap,
        "gap_reason": "reconnect" if gap else None,
        "event": {
            "event_type": "orderbook",
            "market": "KRW-BTC",
            "exchange_timestamp_ms": 1_000 + ordinal,
            "received_at_ns": str(1_000_000_000 + ordinal),
            "best_ask_price": 101.0,
            "best_ask_size": 1.0,
            "best_bid_price": 100.0,
            "best_bid_size": 3.0,
            "levels": [
                {
                    "level": 1,
                    "ask_price": 101.0,
                    "ask_size": 1.0,
                    "bid_price": 100.0,
                    "bid_size": 3.0,
                }
            ],
        },
    }


def _trade(ordinal: int, side: str) -> dict:
    record = _book(ordinal)
    record["event"] = {
        "event_type": "trade",
        "market": "KRW-BTC",
        "trade_price": 100.0,
        "trade_volume": 1.0,
        "aggressor_side": side,
        "sequence_id": str(ordinal),
    }
    return record


def test_snapshot_is_immediate_causal_and_warms_up() -> None:
    signal = OnlineDiagnosticSignal(
        warmup_books=2,
        trade_flow_window_ms=1_000,
        model_version="diagnostic-v0",
    )

    assert signal.feed(_trade(1, "buy")) is None
    first = signal.feed(_book(2))
    second = signal.feed(_book(3))

    assert first is not None and first.ready is False
    assert second is not None and second.ready is True
    assert first.book_imbalance_l5 == 0.5
    assert first.trade_flow == 1.0
    assert first.signal == pytest.approx(0.65)


def test_gap_resets_trade_window_and_book_warmup() -> None:
    signal = OnlineDiagnosticSignal(
        warmup_books=2,
        trade_flow_window_ms=1_000,
        model_version="diagnostic-v0",
    )
    signal.feed(_trade(1, "buy"))
    signal.feed(_book(2))
    ready = signal.feed(_book(3))
    after_gap = signal.feed(_book(4, gap=True))

    assert ready is not None and ready.ready is True
    assert after_gap is not None and after_gap.ready is False
    assert after_gap.trade_flow == 0.0
    assert after_gap.warmup_books_seen == 1


def test_duplicate_public_trade_id_is_not_double_counted() -> None:
    signal = OnlineDiagnosticSignal(
        warmup_books=1,
        trade_flow_window_ms=1_000,
        model_version="diagnostic-v0",
    )
    buy = _trade(1, "buy")
    duplicate = _trade(2, "buy")
    duplicate["event"]["sequence_id"] = buy["event"]["sequence_id"]
    sell = _trade(3, "sell")

    signal.feed(buy)
    signal.feed(duplicate)
    signal.feed(sell)
    snapshot = signal.feed(_book(4))

    assert snapshot is not None
    assert snapshot.trade_flow == 0.0
