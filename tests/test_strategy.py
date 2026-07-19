from __future__ import annotations

import dataclasses

import numpy as np
import pandas as pd
import pytest

from coinpilot.config import ModelConfig
from coinpilot.data import synthetic_candles
from coinpilot.strategy import generate_walk_forward_predictions


def _model_config() -> ModelConfig:
    return ModelConfig(
        horizon_bars=3,
        min_train_samples=100,
        train_window=180,
        retrain_every=20,
        max_iterations=100,
    )


def test_walk_forward_training_labels_are_fully_observable() -> None:
    candles = synthetic_candles(420)
    result = generate_walk_forward_predictions(
        candles,
        interval_minutes=60,
        model_config=_model_config(),
        round_trip_cost=0.002,
    )

    assert result.fits
    for fit in result.fits:
        assert pd.Timestamp(fit.max_label_end) <= pd.Timestamp(fit.trained_at)


def test_future_price_mutation_does_not_change_prior_predictions() -> None:
    candles = synthetic_candles(420)
    cutoff = 300
    original = generate_walk_forward_predictions(
        candles,
        interval_minutes=60,
        model_config=_model_config(),
        round_trip_cost=0.002,
    )
    mutated = candles.copy()
    mutated.loc[cutoff + 1 :, ["open", "high", "low", "close"]] *= 4
    changed = generate_walk_forward_predictions(
        mutated,
        interval_minutes=60,
        model_config=_model_config(),
        round_trip_cost=0.002,
    )

    pd.testing.assert_series_equal(
        original.probabilities.loc[:cutoff],
        changed.probabilities.loc[:cutoff],
    )


def test_retrain_schedule_is_stable() -> None:
    candles = synthetic_candles(360)
    config = dataclasses.replace(_model_config(), retrain_every=12)
    first = generate_walk_forward_predictions(
        candles,
        interval_minutes=60,
        model_config=config,
        round_trip_cost=0.002,
    )
    second = generate_walk_forward_predictions(
        candles,
        interval_minutes=60,
        model_config=config,
        round_trip_cost=0.002,
    )
    assert [fit.as_dict() for fit in first.fits] == [
        fit.as_dict() for fit in second.fits
    ]


def test_retrain_schedule_is_anchored_when_history_window_shifts() -> None:
    all_candles = synthetic_candles(421)
    first_window = all_candles.iloc[:420].reset_index(drop=True)
    shifted_window = all_candles.iloc[1:].reset_index(drop=True)
    config = _model_config()
    first = generate_walk_forward_predictions(
        first_window,
        interval_minutes=60,
        model_config=config,
        round_trip_cost=0.002,
    )
    shifted = generate_walk_forward_predictions(
        shifted_window,
        interval_minutes=60,
        model_config=config,
        round_trip_cost=0.002,
    )
    shared_time = first_window.iloc[-1]["timestamp"]
    first_index = first_window.index[first_window["timestamp"] == shared_time][0]
    shifted_index = shifted_window.index[
        shifted_window["timestamp"] == shared_time
    ][0]

    assert first.model_ids.iloc[first_index] == shifted.model_ids.iloc[shifted_index]
    assert first.probabilities.iloc[first_index] == pytest.approx(
        shifted.probabilities.iloc[shifted_index]
    )


def test_minimum_valid_history_keeps_shared_prediction_stable() -> None:
    config = _model_config()
    minimum_history = (
        config.train_window
        + 72
        + config.horizon_bars
        + config.retrain_every
        + 1
    )
    all_candles = synthetic_candles(minimum_history + 1)
    first_window = all_candles.iloc[:-1].reset_index(drop=True)
    shifted_window = all_candles.iloc[1:].reset_index(drop=True)

    first = generate_walk_forward_predictions(
        first_window,
        interval_minutes=60,
        model_config=config,
        round_trip_cost=0.002,
    )
    shifted = generate_walk_forward_predictions(
        shifted_window,
        interval_minutes=60,
        model_config=config,
        round_trip_cost=0.002,
    )
    shared_time = first_window.iloc[-1]["timestamp"]
    first_index = first_window.index[
        first_window["timestamp"] == shared_time
    ][0]
    shifted_index = shifted_window.index[
        shifted_window["timestamp"] == shared_time
    ][0]

    assert pd.notna(first.probabilities.iloc[-1])
    assert first.model_ids.iloc[first_index] == shifted.model_ids.iloc[
        shifted_index
    ]
    assert first.probabilities.iloc[first_index] == pytest.approx(
        shifted.probabilities.iloc[shifted_index]
    )


def test_expected_return_buffer_uses_horizon_adjusted_effective_samples() -> None:
    candles = synthetic_candles(900, seed=31)
    config = ModelConfig(
        signal_mode="expected_return",
        horizon_bars=12,
        min_train_samples=100,
        train_window=360,
        retrain_every=24,
        calibration_window=180,
        calibration_min_samples=90,
        calibration_uncertainty_z=1.5,
    )

    result = generate_walk_forward_predictions(
        candles,
        interval_minutes=60,
        model_config=config,
        round_trip_cost=0.002,
    )

    assert result.fits
    fit = result.fits[-1]
    expected_effective_samples = (
        fit.calibration_samples / config.horizon_bars
    )
    assert fit.calibration_effective_samples == pytest.approx(
        expected_effective_samples
    )
    assert fit.calibration_buffer == pytest.approx(
        config.calibration_uncertainty_z
        * fit.calibration_rmse
        / np.sqrt(expected_effective_samples)
    )
