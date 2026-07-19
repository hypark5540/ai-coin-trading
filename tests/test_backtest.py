from __future__ import annotations

import dataclasses
import json

import pandas as pd
import pytest

from coinpilot.backtest import (
    TRADE_COLUMNS,
    run_backtest,
    verify_backtest_artifacts,
    with_scaled_costs,
    write_backtest_artifacts,
)
from coinpilot.config import AppConfig, DataConfig, ModelConfig
from coinpilot.data import synthetic_candles
from coinpilot.features import FeatureDataset
from coinpilot.strategy import prediction_alignment_hash


def _config() -> AppConfig:
    base = AppConfig()
    return dataclasses.replace(
        base,
        data=DataConfig(candle_count=500),
        model=ModelConfig(
            horizon_bars=3,
            min_train_samples=100,
            train_window=180,
            retrain_every=24,
            max_iterations=100,
        ),
    ).validate()


def test_backtest_is_reproducible_and_accounting_stays_non_negative() -> None:
    candles = synthetic_candles(500, seed=11)
    config = _config()
    first = run_backtest(candles, config)
    second = run_backtest(candles, config)

    assert first.metrics == second.metrics
    assert first.data_hash == second.data_hash
    assert (first.equity_curve["cash"] >= -1e-8).all()
    assert (first.equity_curve["quantity"] >= 0).all()
    assert len(first.equity_curve) == len(candles)
    assert first.metrics["prediction_count"] > 0
    assert first.metrics["model_fit_count"] > 0
    assert "oos_auc" in first.metrics


def test_external_predictions_must_align_by_timestamp() -> None:
    candles = synthetic_candles(500, seed=12)
    config = _config()
    baseline = run_backtest(candles, config)
    shifted_frame = baseline.predictions.dataset.frame.copy()
    shifted_frame["timestamp"] = shifted_frame["timestamp"].shift(-1)
    misaligned = dataclasses.replace(
        baseline.predictions,
        dataset=FeatureDataset(frame=shifted_frame),
    )
    with pytest.raises(ValueError, match="timestamps"):
        run_backtest(candles, config, predictions=misaligned)

    shifted_probabilities = baseline.predictions.probabilities.shift(-1)
    shifted_values = dataclasses.replace(
        baseline.predictions,
        probabilities=shifted_probabilities,
    )
    with pytest.raises(ValueError, match="manifest"):
        run_backtest(candles, config, predictions=shifted_values)

    shifted_atr_frame = baseline.predictions.dataset.frame.copy()
    shifted_atr_frame["atr_pct"] = shifted_atr_frame["atr_pct"].shift(-1)
    shifted_atr = dataclasses.replace(
        baseline.predictions,
        dataset=FeatureDataset(frame=shifted_atr_frame),
    )
    with pytest.raises(ValueError, match="manifest"):
        run_backtest(candles, config, predictions=shifted_atr)


def test_external_predictions_must_come_from_same_source_candles() -> None:
    candles = synthetic_candles(500, seed=12)
    revised = candles.copy()
    revised.loc[250, "close"] *= 1.001
    revised.loc[250, "high"] = max(
        revised.loc[250, "high"], revised.loc[250, "close"]
    )
    config = _config()
    predictions = run_backtest(candles, config).predictions

    with pytest.raises(ValueError, match="different source candles"):
        run_backtest(revised, config, predictions=predictions)


def test_external_prediction_provenance_cannot_be_relabelled() -> None:
    candles = synthetic_candles(500, seed=13)
    config = _config()
    predictions = run_backtest(candles, config).predictions
    changed_model = dataclasses.replace(
        config,
        model=dataclasses.replace(config.model, l2=0.75),
    ).validate()

    with pytest.raises(ValueError, match="model configuration"):
        run_backtest(candles, changed_model, predictions=predictions)

    stripped_fits = dataclasses.replace(predictions, fits=())
    with pytest.raises(ValueError, match="fit records"):
        run_backtest(candles, config, predictions=stripped_fits)


def test_rehashed_tampered_atr_is_rebuilt_from_source_candles() -> None:
    candles = synthetic_candles(500, seed=14)
    config = _config()
    predictions = run_backtest(candles, config).predictions
    tampered_frame = predictions.dataset.frame.copy()
    tampered_frame["atr_pct"] *= 10
    tampered_hash = prediction_alignment_hash(
        tampered_frame["timestamp"],
        predictions.probabilities,
        predictions.model_ids,
        tampered_frame["atr_pct"],
        tampered_frame["target"],
        tampered_frame["forward_return"],
        source_data_hash=predictions.source_data_hash,
        positive_return_threshold=predictions.positive_return_threshold,
        training_round_trip_cost=predictions.training_round_trip_cost,
        prediction_interval_minutes=predictions.prediction_interval_minutes,
        model_config_hash_value=predictions.model_config_hash,
        fits_hash_value=predictions.fits_hash,
    )
    tampered = dataclasses.replace(
        predictions,
        dataset=FeatureDataset(frame=tampered_frame),
        alignment_hash=tampered_hash,
    )

    with pytest.raises(ValueError, match="source candles: atr_pct"):
        run_backtest(candles, config, predictions=tampered)


def test_no_trade_artifacts_have_strict_json_and_csv_schema(tmp_path) -> None:
    candles = synthetic_candles(300, seed=15)
    base = _config()
    config = dataclasses.replace(
        base,
        data=DataConfig(candle_count=300),
        model=dataclasses.replace(
            base.model,
            minimum_edge_pct=0.9,
            entry_probability=0.99,
        ),
    ).validate()
    result = run_backtest(candles, config)
    paths = write_backtest_artifacts(result, tmp_path, name="no-trade")

    raw_summary = paths["summary"].read_text(encoding="utf-8")
    parsed = json.loads(
        raw_summary,
        parse_constant=lambda value: (_ for _ in ()).throw(
            ValueError(f"non-standard constant {value}")
        ),
    )
    trades = pd.read_csv(paths["trades"])
    assert parsed["artifact_schema_version"] == 3
    assert verify_backtest_artifacts(paths["summary"]) is True
    predictions = pd.read_csv(paths["predictions"])
    assert "atr_pct" in predictions.columns
    assert "expected_net_edge" in predictions.columns
    assert list(trades.columns) == list(TRADE_COLUMNS)

    paths["predictions"].write_text(
        paths["predictions"].read_text(encoding="utf-8") + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="hash mismatch"):
        verify_backtest_artifacts(paths["summary"])


def test_execution_cost_stress_reuses_fixed_predictions() -> None:
    candles = synthetic_candles(500, seed=21)
    config = _config()
    base = run_backtest(candles, config)
    stress_config = with_scaled_costs(config, 2.0)
    stress = run_backtest(
        candles,
        stress_config,
        predictions=base.predictions,
    )

    assert stress.data_hash == base.data_hash
    assert stress.predictions.alignment_hash == base.predictions.alignment_hash
    pd.testing.assert_series_equal(
        stress.predictions.probabilities,
        base.predictions.probabilities,
    )
    pd.testing.assert_series_equal(
        stress.predictions.model_ids,
        base.predictions.model_ids,
    )
    assert stress.config.risk.fee_rate == pytest.approx(
        base.config.risk.fee_rate * 2
    )
    assert stress.config.risk.slippage_bps == pytest.approx(
        base.config.risk.slippage_bps * 2
    )
