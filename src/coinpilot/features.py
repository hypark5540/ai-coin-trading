from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from coinpilot.data import validate_candles


PROBABILITY_FEATURE_COLUMNS = (
    "return_1",
    "return_3",
    "return_6",
    "return_12",
    "return_24",
    "sma_gap_12",
    "sma_gap_24",
    "sma_gap_72",
    "volatility_6",
    "volatility_24",
    "volatility_72",
    "rsi_14",
    "atr_pct",
    "volume_z_24",
    "range_pct",
    "close_location",
)

EXPECTED_RETURN_FEATURE_COLUMNS = (
    *PROBABILITY_FEATURE_COLUMNS[:5],
    "return_72",
    "return_168",
    *PROBABILITY_FEATURE_COLUMNS[5:8],
    "sma_gap_168",
    *PROBABILITY_FEATURE_COLUMNS[8:11],
    "volatility_168",
    "volatility_ratio_24_168",
    *PROBABILITY_FEATURE_COLUMNS[11:],
)

FEATURE_COLUMNS = (
    *EXPECTED_RETURN_FEATURE_COLUMNS,
    "sma_gap_336",
)


@dataclass(frozen=True, slots=True)
class FeatureDataset:
    frame: pd.DataFrame
    feature_columns: tuple[str, ...] = FEATURE_COLUMNS


def _rsi(close: pd.Series, window: int = 14) -> pd.Series:
    change = close.diff()
    gain = change.clip(lower=0).rolling(window, min_periods=window).mean()
    loss = (-change.clip(upper=0)).rolling(window, min_periods=window).mean()
    relative_strength = gain / loss.replace(0, np.nan)
    result = 100 - (100 / (1 + relative_strength))
    result = result.where(loss != 0, 100.0)
    result = result.where(gain != 0, 0.0)
    return result / 100.0


def _segment_features(segment: pd.DataFrame) -> pd.DataFrame:
    output = pd.DataFrame(index=segment.index)
    close = segment["close"]
    log_close = np.log(close)
    log_return = log_close.diff()

    for lag in (1, 3, 6, 12, 24, 72, 168):
        output[f"return_{lag}"] = log_close.diff(lag)
    for window in (12, 24, 72, 168, 336):
        sma = close.rolling(window, min_periods=window).mean()
        output[f"sma_gap_{window}"] = close / sma - 1
    for window in (6, 24, 72, 168):
        output[f"volatility_{window}"] = log_return.rolling(
            window, min_periods=window
        ).std(ddof=0)
    output["volatility_ratio_24_168"] = (
        output["volatility_24"]
        / output["volatility_168"].replace(0, np.nan)
    )

    previous_close = close.shift(1)
    true_range = pd.concat(
        [
            segment["high"] - segment["low"],
            (segment["high"] - previous_close).abs(),
            (segment["low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    output["atr_pct"] = (
        true_range.rolling(14, min_periods=14).mean() / close
    )
    output["rsi_14"] = _rsi(close, 14)

    volume_mean = segment["volume"].rolling(24, min_periods=24).mean()
    volume_std = segment["volume"].rolling(24, min_periods=24).std(ddof=0)
    output["volume_z_24"] = (
        (segment["volume"] - volume_mean) / volume_std.replace(0, np.nan)
    )
    output["range_pct"] = (segment["high"] - segment["low"]) / close
    spread = (segment["high"] - segment["low"]).replace(0, np.nan)
    output["close_location"] = (close - segment["low"]) / spread
    output["close_location"] = output["close_location"].fillna(0.5)
    return output


def build_feature_dataset(
    candles: pd.DataFrame,
    *,
    interval_minutes: int,
    horizon_bars: int,
    positive_return_threshold: float,
) -> FeatureDataset:
    if horizon_bars < 1:
        raise ValueError("horizon_bars must be positive")
    frame = validate_candles(candles)
    expected = pd.Timedelta(minutes=interval_minutes)
    gaps = frame["timestamp"].diff().ne(expected)
    gaps.iloc[0] = True
    segment_ids = gaps.cumsum()

    feature_frame = pd.DataFrame(index=frame.index)
    forward_return = pd.Series(np.nan, index=frame.index, dtype=float)
    label_end_time = pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns, UTC]")

    for _, indexes in frame.groupby(segment_ids).groups.items():
        segment = frame.loc[indexes]
        computed = _segment_features(segment)
        feature_frame.loc[indexes, FEATURE_COLUMNS] = computed.loc[
            indexes, FEATURE_COLUMNS
        ]
        entry_open = segment["open"].shift(-1)
        exit_open = segment["open"].shift(-(horizon_bars + 1))
        forward_return.loc[indexes] = exit_open / entry_open - 1
        label_end_time.loc[indexes] = segment["timestamp"].shift(
            -(horizon_bars + 1)
        )

    target = (forward_return > positive_return_threshold).astype(float)
    target = target.where(forward_return.notna(), np.nan)

    result = pd.concat(
        [
            frame[["timestamp", "market"]],
            feature_frame,
            pd.DataFrame(
                {
                    "forward_return": forward_return,
                    "target": target,
                    "label_end_time": label_end_time,
                    "segment_id": segment_ids,
                }
            ),
        ],
        axis=1,
    )
    result.loc[:, FEATURE_COLUMNS] = result.loc[:, FEATURE_COLUMNS].replace(
        [np.inf, -np.inf], np.nan
    )
    return FeatureDataset(frame=result)
