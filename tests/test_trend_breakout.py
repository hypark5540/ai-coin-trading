from __future__ import annotations

import dataclasses

import numpy as np
import pandas as pd
import pytest

from coinpilot.backtest import run_backtest
from coinpilot.broker import PortfolioState
from coinpilot.config import AppConfig, DataConfig, ModelConfig, RiskConfig
from coinpilot.data import MarketTicker, validate_candles
from coinpilot.engine import BarExecutionEngine
from coinpilot.paper import PaperSnapshotEngine
from coinpilot.strategy import (
    generate_walk_forward_predictions,
    prediction_alignment_hash,
    trend_breakout_model_id,
)


def _trend_model(**changes: object) -> ModelConfig:
    values: dict[str, object] = {
        "signal_mode": "trend_breakout",
        "horizon_bars": 336,
        "breakout_entry_window": 336,
        "breakout_exit_window": 168,
    }
    values.update(changes)
    return ModelConfig(**values)


def _trend_candles(count: int = 750, *, gap_at: int | None = None) -> pd.DataFrame:
    timestamps = pd.date_range(
        "2025-01-01T00:00:00Z", periods=count, freq="1h", tz="UTC"
    )
    if gap_at is not None:
        timestamps = pd.Series(timestamps)
        timestamps.loc[gap_at:] += pd.Timedelta(hours=1)
    opening = np.full(count, 100.0)
    high = np.full(count, 101.0)
    low = np.full(count, 99.0)
    close = np.full(count, 100.0)

    # The current high must be excluded: close 102 clears the prior high 101,
    # but does not clear its own high 110.
    close[336] = 102.0
    high[336] = 110.0
    # The current low must be excluded: close 98 is below the prior low 99,
    # but not below its own low 90.
    opening[337] = 103.0
    close[337] = 98.0
    high[337] = 104.0
    low[337] = 90.0
    opening[338] = 97.0
    low[338] = 96.0

    volume = np.full(count, 1.0)
    return validate_candles(
        pd.DataFrame(
            {
                "timestamp": timestamps,
                "market": "KRW-BTC",
                "open": opening,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
                "quote_volume": volume * close,
            }
        )
    )


def _wide_risk() -> RiskConfig:
    return RiskConfig(
        initial_cash=1_000_000.0,
        risk_per_trade=0.10,
        max_position_fraction=0.50,
        atr_stop_multiple=2.0,
        minimum_stop_pct=0.20,
        maximum_stop_pct=0.80,
        trailing_stop_pct=0.90,
        max_strategy_drawdown_pct=0.90,
        fee_rate=0.0,
        slippage_bps=0.0,
        cooldown_bars=0,
        minimum_order_quote=1.0,
    )


def _trend_config() -> AppConfig:
    return dataclasses.replace(
        AppConfig(),
        data=DataConfig(candle_count=750),
        model=_trend_model(),
        risk=_wide_risk(),
    ).validate()


def test_trend_rule_excludes_current_extremes_and_has_no_fits() -> None:
    candles = _trend_candles()
    model = _trend_model()

    result = generate_walk_forward_predictions(
        candles,
        interval_minutes=60,
        model_config=model,
        round_trip_cost=0.002,
    )

    assert result.probabilities.iloc[:336].isna().all()
    assert result.probabilities.iloc[336] == 1.0
    assert result.probabilities.iloc[337] == 0.0
    assert result.probabilities.iloc[338] == 0.5
    assert result.fits == ()
    assert set(result.model_ids.dropna()) == {trend_breakout_model_id(model)}
    assert result.expected_gross_returns.isna().all()
    assert result.expected_net_edges.isna().all()


def test_trend_rule_is_future_independent_and_resets_after_gap() -> None:
    candles = _trend_candles()
    model = _trend_model()
    original = generate_walk_forward_predictions(
        candles,
        interval_minutes=60,
        model_config=model,
        round_trip_cost=0.002,
    )
    mutated = candles.copy()
    mutated.loc[501:, ["open", "high", "low", "close"]] *= 4
    changed = generate_walk_forward_predictions(
        mutated,
        interval_minutes=60,
        model_config=model,
        round_trip_cost=0.002,
    )

    pd.testing.assert_series_equal(
        original.probabilities.loc[:500],
        changed.probabilities.loc[:500],
    )

    gapped = generate_walk_forward_predictions(
        _trend_candles(gap_at=400),
        interval_minutes=60,
        model_config=model,
        round_trip_cost=0.002,
    )
    assert gapped.probabilities.iloc[400:736].isna().all()
    assert gapped.probabilities.iloc[736] == 0.5


def test_trend_signal_executes_at_next_open_and_uses_probability_metrics() -> None:
    candles = _trend_candles()
    result = run_backtest(candles, _trend_config())
    fills = [
        event["payload"]
        for event in result.events
        if event["event_type"] == "fill"
    ]

    assert fills[0]["side"] == "buy"
    assert fills[0]["timestamp"] == candles.iloc[337]["timestamp"].isoformat()
    assert fills[0]["raw_price"] == candles.iloc[337]["open"]
    assert fills[1]["side"] == "sell"
    assert fills[1]["timestamp"] == candles.iloc[338]["timestamp"].isoformat()
    assert fills[1]["reason"] == "model_exit"
    assert result.metrics["signal_mode"] == "trend_breakout"
    assert result.metrics["prediction_count"] > 0
    assert result.metrics["model_fit_count"] == 0
    assert result.metrics["forecast_mae"] is None


def test_rehashed_trend_score_tampering_is_rejected() -> None:
    candles = _trend_candles()
    config = _trend_config()
    predictions = run_backtest(candles, config).predictions
    tampered_scores = predictions.probabilities.copy()
    tampered_scores.iloc[338] = 0.75
    tampered_hash = prediction_alignment_hash(
        predictions.dataset.frame["timestamp"],
        tampered_scores,
        predictions.model_ids,
        predictions.dataset.frame["atr_pct"],
        predictions.dataset.frame["target"],
        predictions.dataset.frame["forward_return"],
        source_data_hash=predictions.source_data_hash,
        positive_return_threshold=predictions.positive_return_threshold,
        training_round_trip_cost=predictions.training_round_trip_cost,
        prediction_interval_minutes=predictions.prediction_interval_minutes,
        model_config_hash_value=predictions.model_config_hash,
        fits_hash_value=predictions.fits_hash,
        signal_mode=predictions.signal_mode,
    )
    tampered = dataclasses.replace(
        predictions,
        probabilities=tampered_scores,
        alignment_hash=tampered_hash,
    )

    with pytest.raises(ValueError, match="do not match source candles"):
        run_backtest(candles, config, predictions=tampered)


def test_trend_config_bounds_and_history_are_validated() -> None:
    config = _trend_config()
    assert config.feature_warmup_bars == 336
    assert config.minimum_history_bars == 337

    too_short = dataclasses.replace(
        config, data=dataclasses.replace(config.data, candle_count=336)
    )
    with pytest.raises(ValueError, match="at least 337"):
        too_short.validate()

    invalid_windows = dataclasses.replace(
        config,
        model=dataclasses.replace(
            config.model,
            breakout_entry_window=168,
            breakout_exit_window=168,
        ),
    )
    with pytest.raises(ValueError, match="exit_window must be below"):
        invalid_windows.validate()

    wrong_interval = dataclasses.replace(
        config,
        data=dataclasses.replace(config.data, interval_minutes=30),
    )
    with pytest.raises(ValueError, match="interval_minutes to be 60"):
        wrong_interval.validate()


def test_trend_paper_path_enters_and_sets_hard_horizon() -> None:
    config = _trend_config()
    state = PortfolioState.initial("KRW-BTC", config.risk.initial_cash)
    state.last_bar_time = "2026-01-01T00:00:00+00:00"
    ticker = MarketTicker(
        market="KRW-BTC",
        price=105.0,
        exchange_timestamp=pd.Timestamp("2026-01-01T02:00:00Z"),
        observed_at=pd.Timestamp("2026-01-01T02:00:01Z"),
    )

    result = PaperSnapshotEngine(state, config).process_snapshot(
        latest_closed_bar=pd.Series(
            {"timestamp": pd.Timestamp("2026-01-01T01:00:00Z")}
        ),
        probability=1.0,
        atr_pct=0.02,
        model_id=trend_breakout_model_id(config.model),
        ticker=ticker,
    )

    assert state.quantity > 0
    assert any(event["event_type"] == "fill" for event in result.events)
    assert state.entry_horizon_exit_time == (
        pd.Timestamp("2026-01-01T02:00:00Z")
        + pd.Timedelta(hours=336)
    ).isoformat()


def test_trend_engine_forces_exit_at_configured_horizon() -> None:
    model = _trend_model(
        horizon_bars=2,
        breakout_entry_window=3,
        breakout_exit_window=2,
    )
    state = PortfolioState.initial("KRW-BTC", 1_000_000.0)
    engine = BarExecutionEngine(
        state=state,
        risk_config=_wide_risk(),
        model_config=model,
        interval_minutes=60,
    )

    def bar(hour: int) -> dict[str, object]:
        return {
            "timestamp": pd.Timestamp("2026-01-01T00:00:00Z")
            + pd.Timedelta(hours=hour),
            "market": "KRW-BTC",
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.0,
        }

    engine.process_bar(
        bar(0),
        current_probability=1.0,
        current_atr_pct=0.02,
        current_model_id=trend_breakout_model_id(model),
    )
    engine.process_bar(
        bar(1),
        current_probability=0.5,
        current_atr_pct=0.02,
        current_model_id=trend_breakout_model_id(model),
    )
    engine.process_bar(
        bar(2),
        current_probability=0.5,
        current_atr_pct=0.02,
        current_model_id=trend_breakout_model_id(model),
    )
    final = engine.process_bar(
        bar(3),
        current_probability=0.5,
        current_atr_pct=0.02,
        current_model_id=trend_breakout_model_id(model),
    )

    sells = [
        event["payload"]
        for event in final.events
        if event["event_type"] == "fill"
        and event["payload"]["side"] == "sell"
    ]
    assert len(sells) == 1
    assert sells[0]["reason"] == "horizon_exit"
