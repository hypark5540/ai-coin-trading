from __future__ import annotations

import dataclasses

import pandas as pd
import pytest

from coinpilot.broker import HALT_PENDING, HALTED, PortfolioState
from coinpilot.config import ModelConfig, RiskConfig
from coinpilot.engine import BarExecutionEngine


def _bar(
    timestamp: str,
    *,
    opening: float = 100.0,
    high: float = 101.0,
    low: float = 99.0,
    close: float = 100.0,
):
    return {
        "timestamp": pd.Timestamp(timestamp),
        "market": "KRW-BTC",
        "open": opening,
        "high": high,
        "low": low,
        "close": close,
    }


def _configs() -> tuple[RiskConfig, ModelConfig]:
    risk = RiskConfig(
        initial_cash=1000,
        risk_per_trade=0.1,
        max_position_fraction=0.5,
        atr_stop_multiple=2,
        minimum_stop_pct=0.02,
        maximum_stop_pct=0.20,
        trailing_stop_pct=0.20,
        max_strategy_drawdown_pct=0.10,
        fee_rate=0.001,
        slippage_bps=10,
        cooldown_bars=2,
        minimum_order_quote=1,
    )
    model = ModelConfig(
        min_train_samples=100,
        train_window=100,
        entry_probability=0.6,
        exit_probability=0.4,
    )
    return risk, model


def _engine(risk: RiskConfig | None = None) -> BarExecutionEngine:
    default_risk, model = _configs()
    selected = risk or default_risk
    return BarExecutionEngine(
        state=PortfolioState.initial("KRW-BTC", selected.initial_cash),
        risk_config=selected,
        model_config=model,
        interval_minutes=60,
    )


def test_close_signal_fills_only_at_next_open() -> None:
    engine = _engine()
    first = engine.process_bar(
        _bar("2026-01-01T00:00:00Z"),
        current_probability=0.9,
        current_atr_pct=0.02,
        current_model_id="m1",
    )
    second = engine.process_bar(
        _bar("2026-01-01T01:00:00Z"),
        current_probability=0.9,
        current_atr_pct=0.02,
        current_model_id="m1",
    )

    assert not first.events
    fills = [event["payload"] for event in second.events if event["event_type"] == "fill"]
    assert len(fills) == 1
    assert fills[0]["side"] == "buy"
    assert fills[0]["timestamp"] == "2026-01-01T01:00:00+00:00"
    assert fills[0]["fill_price"] == pytest.approx(100.1)


def test_stop_gap_uses_adverse_open_not_stop_price() -> None:
    risk, _ = _configs()
    engine = _engine(dataclasses.replace(risk, trailing_stop_pct=0.5))
    engine.process_bar(
        _bar("2026-01-01T00:00:00Z"),
        current_probability=0.9,
        current_atr_pct=0.01,
        current_model_id="m1",
    )
    engine.process_bar(
        _bar("2026-01-01T01:00:00Z", low=99),
        current_probability=0.9,
        current_atr_pct=0.01,
        current_model_id="m1",
    )
    step = engine.process_bar(
        _bar(
            "2026-01-01T02:00:00Z",
            opening=94,
            high=95,
            low=93,
            close=94,
        ),
        current_probability=0.9,
        current_atr_pct=0.01,
        current_model_id="m1",
    )

    sells = [
        event["payload"]
        for event in step.events
        if event["event_type"] == "fill" and event["payload"]["side"] == "sell"
    ]
    assert len(sells) == 1
    assert sells[0]["raw_price"] == 94
    assert sells[0]["fill_price"] < 94


def test_exact_drawdown_threshold_halts_and_prevents_reentry() -> None:
    risk, _ = _configs()
    risk = dataclasses.replace(
        risk,
        fee_rate=0,
        slippage_bps=0,
        risk_per_trade=0.25,
        minimum_stop_pct=0.5,
        maximum_stop_pct=0.9,
        trailing_stop_pct=0.9,
    )
    engine = _engine(risk)
    engine.process_bar(
        _bar("2026-01-01T00:00:00Z"),
        current_probability=0.9,
        current_atr_pct=0.2,
        current_model_id="m1",
    )
    threshold = engine.process_bar(
        _bar(
            "2026-01-01T01:00:00Z",
            opening=100,
            high=100,
            low=80,
            close=80,
        ),
        current_probability=0.9,
        current_atr_pct=0.2,
        current_model_id="m1",
    )
    assert threshold.halt_state == HALT_PENDING

    liquidation = engine.process_bar(
        _bar(
            "2026-01-01T02:00:00Z",
            opening=79,
            high=82,
            low=78,
            close=80,
        ),
        current_probability=0.99,
        current_atr_pct=0.2,
        current_model_id="m1",
    )
    assert liquidation.halt_state == HALTED
    assert liquidation.quantity == 0
    assert sum(
        event["event_type"] == "fill"
        and event["payload"]["side"] == "sell"
        for event in liquidation.events
    ) == 1

    after = engine.process_bar(
        _bar("2026-01-01T03:00:00Z"),
        current_probability=0.99,
        current_atr_pct=0.2,
        current_model_id="m1",
    )
    assert after.quantity == 0
    assert not [event for event in after.events if event["event_type"] == "fill"]
