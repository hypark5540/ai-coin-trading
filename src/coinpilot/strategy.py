from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass

import numpy as np
import pandas as pd

from coinpilot.config import ModelConfig
from coinpilot.data import candle_data_hash
from coinpilot.features import (
    FEATURE_COLUMNS,
    EXPECTED_RETURN_FEATURE_COLUMNS,
    PROBABILITY_FEATURE_COLUMNS,
    FeatureDataset,
    build_feature_dataset,
)
from coinpilot.model import (
    StandardizedLogisticRegression,
    StandardizedRidgeRegression,
    binary_auc,
)


@dataclass(frozen=True, slots=True)
class FitRecord:
    model_id: str
    trained_at: str
    train_start: str
    train_end: str
    max_label_end: str
    samples: int
    positive_rate: float
    train_auc: float
    iterations: int
    loss: float
    model_kind: str = "probability"
    train_rmse: float | None = None
    calibration_samples: int = 0
    calibration_effective_samples: float | None = None
    calibration_intercept: float | None = None
    calibration_slope: float | None = None
    calibration_rmse: float | None = None
    calibration_buffer: float | None = None
    calibration_raw_min: float | None = None
    calibration_raw_max: float | None = None
    signal_fit_eligible: bool | None = None

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class PredictionResult:
    probabilities: pd.Series
    model_ids: pd.Series
    dataset: FeatureDataset
    fits: tuple[FitRecord, ...]
    positive_return_threshold: float
    training_round_trip_cost: float
    prediction_interval_minutes: int
    model_config_hash: str
    fits_hash: str
    source_data_hash: str
    alignment_hash: str
    signal_mode: str
    raw_expected_gross_returns: pd.Series
    expected_gross_returns: pd.Series
    calibration_buffers: pd.Series
    expected_net_edges: pd.Series
    signal_eligible: pd.Series
    no_trade_reasons: pd.Series


def model_config_hash(model_config: ModelConfig) -> str:
    material = json.dumps(
        asdict(model_config),
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def fit_manifest_hash(fits: tuple[FitRecord, ...]) -> str:
    records = []
    for fit in fits:
        record = fit.as_dict()
        records.append(
            {
                key: (
                    None
                    if isinstance(value, float) and not np.isfinite(value)
                    else value
                )
                for key, value in record.items()
            }
        )
    material = json.dumps(
        records,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def prediction_alignment_hash(
    timestamps: pd.Series,
    probabilities: pd.Series,
    model_ids: pd.Series,
    atr_pct: pd.Series,
    target: pd.Series,
    forward_return: pd.Series,
    *,
    source_data_hash: str,
    positive_return_threshold: float,
    training_round_trip_cost: float,
    prediction_interval_minutes: int,
    model_config_hash_value: str,
    fits_hash_value: str,
    signal_mode: str = "probability",
    raw_expected_gross_returns: pd.Series | None = None,
    expected_gross_returns: pd.Series | None = None,
    calibration_buffers: pd.Series | None = None,
    expected_net_edges: pd.Series | None = None,
    signal_eligible: pd.Series | None = None,
    no_trade_reasons: pd.Series | None = None,
) -> str:
    expected_fields = (
        raw_expected_gross_returns,
        expected_gross_returns,
        calibration_buffers,
        expected_net_edges,
        signal_eligible,
        no_trade_reasons,
    )
    has_expected_fields = all(item is not None for item in expected_fields)
    if any(item is not None for item in expected_fields) and not has_expected_fields:
        raise ValueError("Expected-return alignment fields must be supplied together")
    digest = hashlib.sha256()
    digest.update(
        (
            f"source={source_data_hash}\n"
            f"signal_mode={signal_mode}\n"
            f"threshold={float(positive_return_threshold).hex()}\n"
            f"training_cost={float(training_round_trip_cost).hex()}\n"
            f"interval={prediction_interval_minutes}\n"
            f"model_config={model_config_hash_value}\n"
            f"fits={fits_hash_value}\n"
        ).encode("utf-8")
    )

    def float_text(value: object) -> str:
        return "nan" if pd.isna(value) else float(value).hex()

    for row_index, (
        timestamp,
        probability,
        model_id,
        atr_value,
        target_value,
        forward_value,
    ) in enumerate(
        zip(
            timestamps,
            probabilities,
            model_ids,
            atr_pct,
            target,
            forward_return,
            strict=True,
        )
    ):
        row_material = (
            f"{pd.Timestamp(timestamp).isoformat()}|"
            f"{float_text(probability)}|"
            f"{model_id if pd.notna(model_id) else ''}|"
            f"{float_text(atr_value)}|"
            f"{float_text(target_value)}|"
            f"{float_text(forward_value)}"
        )
        if has_expected_fields:
            assert raw_expected_gross_returns is not None
            assert expected_gross_returns is not None
            assert calibration_buffers is not None
            assert expected_net_edges is not None
            assert signal_eligible is not None
            assert no_trade_reasons is not None
            eligible_value = signal_eligible.iloc[row_index]
            reason_value = no_trade_reasons.iloc[row_index]
            row_material += (
                f"|{float_text(raw_expected_gross_returns.iloc[row_index])}"
                f"|{float_text(expected_gross_returns.iloc[row_index])}"
                f"|{float_text(calibration_buffers.iloc[row_index])}"
                f"|{float_text(expected_net_edges.iloc[row_index])}"
                f"|{int(bool(eligible_value))}"
                f"|{reason_value if pd.notna(reason_value) else ''}"
            )
        digest.update((row_material + "\n").encode("utf-8"))
    return digest.hexdigest()


def _probability_walk_forward(
    feature_frame: pd.DataFrame,
    *,
    interval_minutes: int,
    model_config: ModelConfig,
    positive_threshold: float,
) -> tuple[pd.Series, pd.Series, tuple[FitRecord, ...]]:
    probabilities = pd.Series(np.nan, index=feature_frame.index, dtype=float)
    model_ids = pd.Series(None, index=feature_frame.index, dtype=object)
    feature_matrix = feature_frame.loc[
        :, PROBABILITY_FEATURE_COLUMNS
    ].to_numpy(dtype=float)
    valid_features = np.isfinite(feature_matrix).all(axis=1)
    target = feature_frame["target"].to_numpy(dtype=float)
    valid_target = np.isfinite(target)
    model: StandardizedLogisticRegression | None = None
    active_model_id: str | None = None
    fits: list[FitRecord] = []
    interval = pd.Timedelta(minutes=interval_minutes)

    for prediction_index in range(len(feature_frame)):
        if not valid_features[prediction_index]:
            continue
        current_time = pd.Timestamp(
            feature_frame.at[prediction_index, "timestamp"]
        )
        absolute_slot = int(current_time.value // interval.value)
        scheduled_refit = absolute_slot % model_config.retrain_every == 0
        if model is None and not scheduled_refit:
            continue
        if scheduled_refit:
            # This intentionally purges one additional row. A training label
            # must be fully settled before the prediction bar is complete.
            train_end_exclusive = prediction_index - model_config.horizon_bars
            if train_end_exclusive <= 0:
                continue
            candidates = np.arange(train_end_exclusive)
            label_times = pd.to_datetime(
                feature_frame.loc[candidates, "label_end_time"], utc=True
            )
            observable = (
                label_times.notna() & label_times.le(current_time)
            ).to_numpy()
            candidates = candidates[
                valid_features[candidates]
                & valid_target[candidates]
                & observable
            ]
            if len(candidates) < model_config.min_train_samples:
                continue
            candidates = candidates[-model_config.train_window :]
            model = StandardizedLogisticRegression(
                l2=model_config.l2,
                learning_rate=model_config.learning_rate,
                max_iterations=model_config.max_iterations,
            )
            summary = model.fit(feature_matrix[candidates], target[candidates])
            training_scores = model.predict_proba(feature_matrix[candidates])
            fingerprint = hashlib.sha256(
                np.round(feature_matrix[candidates], 12).tobytes()
                + target[candidates].tobytes()
                + repr(
                    (
                        positive_threshold,
                        model_config.l2,
                        model_config.learning_rate,
                        model_config.max_iterations,
                    )
                ).encode("utf-8")
            ).hexdigest()[:12]
            trained_at = current_time + interval
            active_model_id = (
                f"wf-{trained_at.strftime('%Y%m%dT%H%M%SZ')}-{fingerprint}"
            )
            label_end_max = pd.Timestamp(
                feature_frame.loc[candidates, "label_end_time"].max()
            )
            fits.append(
                FitRecord(
                    model_id=active_model_id,
                    trained_at=trained_at.isoformat(),
                    train_start=pd.Timestamp(
                        feature_frame.at[candidates[0], "timestamp"]
                    ).isoformat(),
                    train_end=pd.Timestamp(
                        feature_frame.at[candidates[-1], "timestamp"]
                    ).isoformat(),
                    max_label_end=label_end_max.isoformat(),
                    samples=summary.samples,
                    positive_rate=summary.positive_rate,
                    train_auc=float(
                        binary_auc(target[candidates], training_scores)
                    ),
                    iterations=summary.iterations,
                    loss=summary.loss,
                )
            )

        if model is not None and active_model_id is not None:
            probabilities.iloc[prediction_index] = float(
                model.predict_proba(feature_matrix[prediction_index])[0]
            )
            model_ids.iloc[prediction_index] = active_model_id

    probabilities.name = "probability"
    model_ids.name = "model_id"
    return probabilities, model_ids, tuple(fits)


def _expected_return_walk_forward(
    feature_frame: pd.DataFrame,
    *,
    interval_minutes: int,
    model_config: ModelConfig,
    round_trip_cost: float,
    positive_threshold: float,
) -> tuple[
    pd.Series,
    pd.Series,
    pd.Series,
    pd.Series,
    pd.Series,
    pd.Series,
    pd.Series,
    pd.Series,
    tuple[FitRecord, ...],
]:
    probabilities = pd.Series(np.nan, index=feature_frame.index, dtype=float)
    model_ids = pd.Series(None, index=feature_frame.index, dtype=object)
    raw_forecasts = pd.Series(np.nan, index=feature_frame.index, dtype=float)
    gross_forecasts = pd.Series(np.nan, index=feature_frame.index, dtype=float)
    calibration_buffers = pd.Series(
        np.nan, index=feature_frame.index, dtype=float
    )
    net_edges = pd.Series(np.nan, index=feature_frame.index, dtype=float)
    signal_eligible = pd.Series(
        False, index=feature_frame.index, dtype=bool
    )
    no_trade_reasons = pd.Series(
        None, index=feature_frame.index, dtype=object
    )

    feature_matrix = feature_frame.loc[
        :, EXPECTED_RETURN_FEATURE_COLUMNS
    ].to_numpy(dtype=float)
    valid_features = np.isfinite(feature_matrix).all(axis=1)
    forward_return = feature_frame["forward_return"].to_numpy(dtype=float)
    valid_return = np.isfinite(forward_return)
    target = feature_frame["target"].to_numpy(dtype=float)

    model: StandardizedRidgeRegression | None = None
    active_model_id: str | None = None
    active_intercept = float("nan")
    active_slope = float("nan")
    active_buffer = float("nan")
    active_raw_min = float("nan")
    active_raw_max = float("nan")
    active_fit_eligible = False
    fits: list[FitRecord] = []
    interval = pd.Timedelta(minutes=interval_minutes)

    for prediction_index in range(len(feature_frame)):
        if not valid_features[prediction_index]:
            continue
        current_time = pd.Timestamp(
            feature_frame.at[prediction_index, "timestamp"]
        )
        absolute_slot = int(current_time.value // interval.value)
        scheduled_refit = absolute_slot % model_config.retrain_every == 0
        if model is None and not scheduled_refit:
            continue

        if scheduled_refit:
            train_end_exclusive = prediction_index - model_config.horizon_bars
            refit_completed = False
            if train_end_exclusive > 0:
                candidates = np.arange(train_end_exclusive)
                label_times = pd.to_datetime(
                    feature_frame.loc[candidates, "label_end_time"], utc=True
                )
                observable = (
                    label_times.notna() & label_times.le(current_time)
                ).to_numpy()
                candidates = candidates[
                    valid_features[candidates]
                    & valid_return[candidates]
                    & observable
                ]
                candidates = candidates[-model_config.train_window :]
                if len(candidates) >= (
                    model_config.min_train_samples
                    + model_config.calibration_min_samples
                ):
                    calibration_candidates = candidates[
                        -model_config.calibration_window :
                    ]
                    calibration_start_time = (
                        pd.Timestamp(
                            feature_frame.at[
                                calibration_candidates[0], "timestamp"
                            ]
                        )
                        + interval
                    )
                    base_candidates = candidates[
                        : -len(calibration_candidates)
                    ]
                    base_label_times = pd.to_datetime(
                        feature_frame.loc[
                            base_candidates, "label_end_time"
                        ],
                        utc=True,
                    )
                    base_candidates = base_candidates[
                        (
                            base_label_times.notna()
                            & base_label_times.le(calibration_start_time)
                        ).to_numpy()
                    ]
                    if (
                        len(base_candidates)
                        >= model_config.min_train_samples
                        and len(calibration_candidates)
                        >= model_config.calibration_min_samples
                    ):
                        fitted_model = StandardizedRidgeRegression(
                            l2=model_config.l2,
                            clip_quantile=(
                                model_config.target_clip_quantile
                            ),
                        )
                        summary = fitted_model.fit(
                            feature_matrix[base_candidates],
                            forward_return[base_candidates],
                        )
                        raw_calibration = fitted_model.predict(
                            feature_matrix[calibration_candidates]
                        )
                        actual_calibration = forward_return[
                            calibration_candidates
                        ]
                        raw_mean = float(raw_calibration.mean())
                        actual_mean = float(actual_calibration.mean())
                        centered_raw = raw_calibration - raw_mean
                        denominator = float(
                            np.dot(centered_raw, centered_raw)
                        )
                        if denominator > 1e-18:
                            slope = float(
                                np.dot(
                                    centered_raw,
                                    actual_calibration - actual_mean,
                                )
                                / denominator
                            )
                        else:
                            slope = 0.0
                        slope = float(np.clip(slope, 0.0, 1.0))
                        intercept = float(actual_mean - slope * raw_mean)
                        calibrated = intercept + slope * raw_calibration
                        calibration_rmse = float(
                            np.sqrt(
                                np.mean(
                                    np.square(
                                        actual_calibration - calibrated
                                    )
                                )
                            )
                        )
                        # Adjacent h-bar labels overlap heavily. Treating all
                        # hourly calibration rows as independent would shrink
                        # uncertainty by roughly sqrt(horizon) too much.
                        effective_samples = max(
                            1.0,
                            len(calibration_candidates)
                            / model_config.horizon_bars,
                        )
                        buffer = float(
                            model_config.calibration_uncertainty_z
                            * calibration_rmse
                            / np.sqrt(effective_samples)
                        )
                        raw_min = float(raw_calibration.min())
                        raw_max = float(raw_calibration.max())
                        fit_eligible = bool(
                            slope > 0
                            and np.isfinite(
                                (
                                    intercept,
                                    calibration_rmse,
                                    buffer,
                                    raw_min,
                                    raw_max,
                                )
                            ).all()
                        )
                        fingerprint = hashlib.sha256(
                            np.round(
                                feature_matrix[base_candidates], 12
                            ).tobytes()
                            + forward_return[base_candidates].tobytes()
                            + np.round(
                                feature_matrix[calibration_candidates], 12
                            ).tobytes()
                            + actual_calibration.tobytes()
                            + repr(
                                (
                                    round_trip_cost,
                                    positive_threshold,
                                    model_config.l2,
                                    model_config.target_clip_quantile,
                                    model_config.calibration_window,
                                    model_config.calibration_uncertainty_z,
                                )
                            ).encode("utf-8")
                        ).hexdigest()[:12]
                        trained_at = current_time + interval
                        active_model_id = (
                            "wf-ret-"
                            f"{trained_at.strftime('%Y%m%dT%H%M%SZ')}-"
                            f"{fingerprint}"
                        )
                        base_scores = fitted_model.predict(
                            feature_matrix[base_candidates]
                        )
                        binary_target = target[base_candidates]
                        label_end_max = pd.Timestamp(
                            feature_frame.loc[
                                calibration_candidates, "label_end_time"
                            ].max()
                        )
                        fits.append(
                            FitRecord(
                                model_id=active_model_id,
                                trained_at=trained_at.isoformat(),
                                train_start=pd.Timestamp(
                                    feature_frame.at[
                                        base_candidates[0], "timestamp"
                                    ]
                                ).isoformat(),
                                train_end=pd.Timestamp(
                                    feature_frame.at[
                                        calibration_candidates[-1],
                                        "timestamp",
                                    ]
                                ).isoformat(),
                                max_label_end=label_end_max.isoformat(),
                                samples=summary.samples,
                                positive_rate=float(
                                    np.mean(binary_target)
                                ),
                                train_auc=float(
                                    binary_auc(binary_target, base_scores)
                                ),
                                iterations=1,
                                loss=summary.rmse,
                                model_kind="expected_return",
                                train_rmse=summary.rmse,
                                calibration_samples=len(
                                    calibration_candidates
                                ),
                                calibration_effective_samples=(
                                    effective_samples
                                ),
                                calibration_intercept=intercept,
                                calibration_slope=slope,
                                calibration_rmse=calibration_rmse,
                                calibration_buffer=buffer,
                                calibration_raw_min=raw_min,
                                calibration_raw_max=raw_max,
                                signal_fit_eligible=fit_eligible,
                            )
                        )
                        model = fitted_model
                        active_intercept = intercept
                        active_slope = slope
                        active_buffer = buffer
                        active_raw_min = raw_min
                        active_raw_max = raw_max
                        active_fit_eligible = fit_eligible
                        refit_completed = True
            if model is None and not refit_completed:
                continue

        if model is None or active_model_id is None:
            continue
        raw_forecast = float(
            model.predict(feature_matrix[prediction_index])[0]
        )
        clamped_raw = float(
            np.clip(raw_forecast, active_raw_min, active_raw_max)
        )
        gross_forecast = float(
            active_intercept + active_slope * clamped_raw
        )
        net_edge = float(
            gross_forecast - round_trip_cost - active_buffer
        )
        regime_ok = bool(
            feature_frame.at[
                prediction_index,
                f"sma_gap_{model_config.regime_sma_window}",
            ]
            >= model_config.regime_sma_gap_min
        )
        eligible = bool(
            active_fit_eligible
            and regime_ok
            and net_edge >= model_config.minimum_edge_pct
        )
        if not active_fit_eligible:
            reason = "calibration_ineligible"
        elif not regime_ok:
            reason = "regime_filter"
        elif net_edge < model_config.minimum_edge_pct:
            reason = "edge_below_minimum"
        else:
            reason = "eligible"

        raw_forecasts.iloc[prediction_index] = raw_forecast
        gross_forecasts.iloc[prediction_index] = gross_forecast
        calibration_buffers.iloc[prediction_index] = active_buffer
        net_edges.iloc[prediction_index] = net_edge
        signal_eligible.iloc[prediction_index] = eligible
        no_trade_reasons.iloc[prediction_index] = reason
        model_ids.iloc[prediction_index] = active_model_id

    raw_forecasts.name = "raw_expected_gross_return"
    gross_forecasts.name = "expected_gross_return"
    calibration_buffers.name = "calibration_buffer"
    net_edges.name = "expected_net_edge"
    signal_eligible.name = "signal_eligible"
    no_trade_reasons.name = "no_trade_reason"
    probabilities.name = "probability"
    model_ids.name = "model_id"
    return (
        probabilities,
        model_ids,
        raw_forecasts,
        gross_forecasts,
        calibration_buffers,
        net_edges,
        signal_eligible,
        no_trade_reasons,
        tuple(fits),
    )


def generate_walk_forward_predictions(
    candles: pd.DataFrame,
    *,
    interval_minutes: int,
    model_config: ModelConfig,
    round_trip_cost: float,
) -> PredictionResult:
    positive_threshold = round_trip_cost + model_config.minimum_edge_pct
    dataset = build_feature_dataset(
        candles,
        interval_minutes=interval_minutes,
        horizon_bars=model_config.horizon_bars,
        positive_return_threshold=positive_threshold,
    )
    feature_frame = dataset.frame

    if model_config.signal_mode == "expected_return":
        (
            probabilities,
            model_ids,
            raw_forecasts,
            gross_forecasts,
            calibration_buffers,
            net_edges,
            signal_eligible,
            no_trade_reasons,
            fits_tuple,
        ) = _expected_return_walk_forward(
            feature_frame,
            interval_minutes=interval_minutes,
            model_config=model_config,
            round_trip_cost=round_trip_cost,
            positive_threshold=positive_threshold,
        )
    else:
        probabilities, model_ids, fits_tuple = _probability_walk_forward(
            feature_frame,
            interval_minutes=interval_minutes,
            model_config=model_config,
            positive_threshold=positive_threshold,
        )
        raw_forecasts = pd.Series(
            np.nan, index=feature_frame.index, dtype=float,
            name="raw_expected_gross_return",
        )
        gross_forecasts = pd.Series(
            np.nan, index=feature_frame.index, dtype=float,
            name="expected_gross_return",
        )
        calibration_buffers = pd.Series(
            np.nan, index=feature_frame.index, dtype=float,
            name="calibration_buffer",
        )
        net_edges = pd.Series(
            np.nan, index=feature_frame.index, dtype=float,
            name="expected_net_edge",
        )
        signal_eligible = pd.Series(
            False, index=feature_frame.index, dtype=bool,
            name="signal_eligible",
        )
        no_trade_reasons = pd.Series(
            None, index=feature_frame.index, dtype=object,
            name="no_trade_reason",
        )

    source_data_hash = candle_data_hash(candles)
    config_hash = model_config_hash(model_config)
    fits_hash_value = fit_manifest_hash(fits_tuple)
    alignment_kwargs: dict[str, object] = {}
    if model_config.signal_mode == "expected_return":
        alignment_kwargs = {
            "raw_expected_gross_returns": raw_forecasts,
            "expected_gross_returns": gross_forecasts,
            "calibration_buffers": calibration_buffers,
            "expected_net_edges": net_edges,
            "signal_eligible": signal_eligible,
            "no_trade_reasons": no_trade_reasons,
        }
    alignment_hash = prediction_alignment_hash(
        feature_frame["timestamp"],
        probabilities,
        model_ids,
        feature_frame["atr_pct"],
        feature_frame["target"],
        feature_frame["forward_return"],
        source_data_hash=source_data_hash,
        positive_return_threshold=positive_threshold,
        training_round_trip_cost=round_trip_cost,
        prediction_interval_minutes=interval_minutes,
        model_config_hash_value=config_hash,
        fits_hash_value=fits_hash_value,
        signal_mode=model_config.signal_mode,
        **alignment_kwargs,
    )
    return PredictionResult(
        probabilities=probabilities,
        model_ids=model_ids,
        dataset=dataset,
        fits=fits_tuple,
        positive_return_threshold=positive_threshold,
        training_round_trip_cost=float(round_trip_cost),
        prediction_interval_minutes=interval_minutes,
        model_config_hash=config_hash,
        fits_hash=fits_hash_value,
        source_data_hash=source_data_hash,
        alignment_hash=alignment_hash,
        signal_mode=model_config.signal_mode,
        raw_expected_gross_returns=raw_forecasts,
        expected_gross_returns=gross_forecasts,
        calibration_buffers=calibration_buffers,
        expected_net_edges=net_edges,
        signal_eligible=signal_eligible,
        no_trade_reasons=no_trade_reasons,
    )
