from __future__ import annotations

import numpy as np
import pandas as pd

from coinpilot.data import synthetic_candles, validate_candles
from coinpilot.features import FEATURE_COLUMNS, build_feature_dataset


def _linear_candles(count: int = 120) -> pd.DataFrame:
    opening = np.arange(100.0, 100.0 + count)
    close = opening + 0.25
    return validate_candles(
        pd.DataFrame(
            {
                "timestamp": pd.date_range(
                    "2026-01-01", periods=count, freq="1h", tz="UTC"
                ),
                "market": "KRW-BTC",
                "open": opening,
                "high": opening + 1,
                "low": opening - 1,
                "close": close,
                "volume": np.arange(1.0, count + 1),
                "quote_volume": opening * np.arange(1.0, count + 1),
            }
        )
    )


def test_label_alignment_uses_next_open_and_horizon_exit() -> None:
    candles = _linear_candles()
    dataset = build_feature_dataset(
        candles,
        interval_minutes=60,
        horizon_bars=2,
        positive_return_threshold=0.0,
    ).frame

    expected = candles.iloc[3]["open"] / candles.iloc[1]["open"] - 1
    assert dataset.iloc[0]["forward_return"] == expected
    assert dataset["forward_return"].tail(3).isna().all()


def test_future_mutation_does_not_change_past_features() -> None:
    candles = synthetic_candles(260)
    cutoff = 180
    original = build_feature_dataset(
        candles,
        interval_minutes=60,
        horizon_bars=3,
        positive_return_threshold=0.002,
    ).frame
    mutated = candles.copy()
    mutated.loc[cutoff + 1 :, ["open", "high", "low", "close"]] *= 10
    changed = build_feature_dataset(
        mutated,
        interval_minutes=60,
        horizon_bars=3,
        positive_return_threshold=0.002,
    ).frame

    pd.testing.assert_frame_equal(
        original.loc[:cutoff, FEATURE_COLUMNS],
        changed.loc[:cutoff, FEATURE_COLUMNS],
    )


def test_gap_invalidates_rolling_features_and_cross_gap_labels() -> None:
    candles = synthetic_candles(220).drop(index=110).reset_index(drop=True)
    dataset = build_feature_dataset(
        candles,
        interval_minutes=60,
        horizon_bars=3,
        positive_return_threshold=0.002,
    ).frame
    first_after_gap = 110

    assert pd.isna(dataset.loc[first_after_gap, "return_1"])
    assert dataset.loc[first_after_gap - 4 : first_after_gap - 1, "target"].isna().all()
    assert pd.notna(dataset.loc[first_after_gap + 72, "sma_gap_72"])
