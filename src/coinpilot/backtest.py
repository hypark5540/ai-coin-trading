from __future__ import annotations

import dataclasses
import fcntl
import hashlib
import json
import math
import os
import platform
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from coinpilot.broker import PortfolioState, Trade
from coinpilot.config import PROBABILITY_SIGNAL_MODES, AppConfig
from coinpilot.data import candle_data_hash, validate_candles
from coinpilot.engine import BarExecutionEngine
from coinpilot.features import FEATURE_COLUMNS, build_feature_dataset
from coinpilot.model import binary_auc
from coinpilot.strategy import (
    PredictionResult,
    fit_manifest_hash,
    generate_walk_forward_predictions,
    model_config_hash,
    prediction_alignment_hash,
    trend_breakout_model_id,
    trend_breakout_scores,
)


TRADE_COLUMNS = tuple(field.name for field in dataclasses.fields(Trade))


@dataclass(frozen=True, slots=True)
class BacktestResult:
    metrics: dict[str, Any]
    equity_curve: pd.DataFrame
    trades: pd.DataFrame
    events: tuple[dict[str, Any], ...]
    predictions: PredictionResult
    config: AppConfig
    data_hash: str


def _data_hash(candles: pd.DataFrame) -> str:
    return candle_data_hash(candles)


def _safe_ratio(numerator: float, denominator: float) -> float | None:
    if denominator == 0 or not np.isfinite(denominator):
        return None
    value = numerator / denominator
    return float(value) if np.isfinite(value) else None


def _buy_and_hold_return(candles: pd.DataFrame, config: AppConfig) -> float:
    initial = config.risk.initial_cash
    slippage = config.risk.slippage_bps / 10_000
    buy_price = float(candles.iloc[0]["open"]) * (1 + slippage)
    buy_notional = initial / (1 + config.risk.fee_rate)
    quantity = buy_notional / buy_price
    sell_price = float(candles.iloc[-1]["close"]) * (1 - slippage)
    sell_notional = quantity * sell_price
    final_cash = sell_notional * (1 - config.risk.fee_rate)
    return float(final_cash / initial - 1)


def _calculate_metrics(
    *,
    candles: pd.DataFrame,
    equity_curve: pd.DataFrame,
    trades: pd.DataFrame,
    events: tuple[dict[str, Any], ...],
    predictions: PredictionResult,
    config: AppConfig,
) -> dict[str, Any]:
    equity = equity_curve["equity"].astype(float)
    bar_returns = equity.pct_change().replace([np.inf, -np.inf], np.nan).dropna()
    timestamps = pd.to_datetime(equity_curve["timestamp"], utc=True)
    interval = pd.Timedelta(minutes=config.data.interval_minutes)
    evaluation_start = pd.Timestamp(candles.iloc[0]["timestamp"])
    evaluation_end = pd.Timestamp(candles.iloc[-1]["timestamp"]) + interval
    elapsed = evaluation_end - evaluation_start
    median_delta = timestamps.diff().dropna().median()
    periods_per_year = (
        pd.Timedelta(days=365.25) / median_delta
        if pd.notna(median_delta) and median_delta > pd.Timedelta(0)
        else 0.0
    )
    initial_cash = float(config.risk.initial_cash)
    total_return = float(equity.iloc[-1] / initial_cash - 1)
    if elapsed > pd.Timedelta(0) and equity.iloc[-1] > 0:
        years = elapsed / pd.Timedelta(days=365.25)
        annualized_return = float(
            (equity.iloc[-1] / initial_cash) ** (1 / years) - 1
        )
    else:
        annualized_return = None

    return_std = float(bar_returns.std(ddof=0)) if len(bar_returns) else 0.0
    sharpe = (
        float(bar_returns.mean() / return_std * math.sqrt(periods_per_year))
        if return_std > 0 and periods_per_year > 0
        else None
    )
    downside = bar_returns[bar_returns < 0]
    downside_deviation = (
        float(np.sqrt(np.mean(np.square(downside)))) if len(downside) else 0.0
    )
    sortino = (
        float(bar_returns.mean() / downside_deviation * math.sqrt(periods_per_year))
        if downside_deviation > 0 and periods_per_year > 0
        else None
    )
    running_peak = equity.cummax()
    drawdown = equity / running_peak - 1
    max_drawdown = max(0.0, float(-drawdown.min()))

    fills = [event["payload"] for event in events if event["event_type"] == "fill"]
    total_fees = float(sum(float(fill["fee"]) for fill in fills))
    total_slippage = float(
        sum(float(fill["slippage_cost"]) for fill in fills)
    )
    turnover = float(
        sum(float(fill["notional"]) for fill in fills) / equity.mean()
    )
    exposure = float((equity_curve["quantity"] > 0).mean())

    if trades.empty:
        wins = pd.Series(dtype=float)
        losses = pd.Series(dtype=float)
    else:
        wins = trades.loc[trades["pnl"] > 0, "pnl"]
        losses = trades.loc[trades["pnl"] < 0, "pnl"]
    gross_profit = float(wins.sum()) if len(wins) else 0.0
    gross_loss = float(-losses.sum()) if len(losses) else 0.0
    fit_aucs = [fit.train_auc for fit in predictions.fits if np.isfinite(fit.train_auc)]
    prediction_times = pd.to_datetime(
        predictions.dataset.frame["timestamp"], utc=True
    )
    label_end_times = pd.to_datetime(
        predictions.dataset.frame["label_end_time"], utc=True
    )
    evaluation_mask = (
        prediction_times.ge(evaluation_start)
        & prediction_times.lt(evaluation_end)
        & label_end_times.notna()
        & label_end_times.le(evaluation_end)
    )
    diagnostic_scores = (
        predictions.probabilities
        if predictions.signal_mode in PROBABILITY_SIGNAL_MODES
        else predictions.expected_gross_returns
    )
    oos_mask = (
        evaluation_mask
        & diagnostic_scores.notna()
        & predictions.dataset.frame["target"].notna()
    )
    oos_auc = (
        float(
            binary_auc(
                predictions.dataset.frame.loc[oos_mask, "target"].to_numpy(),
                diagnostic_scores.loc[oos_mask].to_numpy(),
            )
        )
        if oos_mask.any()
        else None
    )
    if oos_auc is not None and not np.isfinite(oos_auc):
        oos_auc = None

    realized_forecast_mask = (
        evaluation_mask
        & predictions.expected_gross_returns.notna()
        & predictions.dataset.frame["forward_return"].notna()
    )
    if realized_forecast_mask.any():
        forecast_values = predictions.expected_gross_returns.loc[
            realized_forecast_mask
        ].astype(float)
        realized_values = predictions.dataset.frame.loc[
            realized_forecast_mask, "forward_return"
        ].astype(float)
        forecast_errors = forecast_values - realized_values
        forecast_mae = float(forecast_errors.abs().mean())
        forecast_rmse = float(np.sqrt(np.mean(np.square(forecast_errors))))
        rank_correlation = forecast_values.rank(method="average").corr(
            realized_values.rank(method="average")
        )
        forecast_rank_correlation = (
            float(rank_correlation)
            if pd.notna(rank_correlation) and np.isfinite(rank_correlation)
            else None
        )
        eligible_mask = (
            realized_forecast_mask & predictions.signal_eligible.astype(bool)
        )
        eligible_realized_mean = (
            float(
                predictions.dataset.frame.loc[
                    eligible_mask, "forward_return"
                ].mean()
            )
            if eligible_mask.any()
            else None
        )
    else:
        forecast_mae = None
        forecast_rmse = None
        forecast_rank_correlation = None
        eligible_mask = pd.Series(
            False, index=predictions.dataset.frame.index, dtype=bool
        )
        eligible_realized_mean = None

    signal_window = (
        prediction_times.ge(evaluation_start)
        & prediction_times.lt(evaluation_end)
    )
    valid_signal_count = int(
        (
            predictions.probabilities.notna()
            if predictions.signal_mode in PROBABILITY_SIGNAL_MODES
            else predictions.expected_net_edges.notna()
        ).loc[signal_window].sum()
    )
    eligible_signal_count = int(
        predictions.signal_eligible.loc[signal_window].sum()
    )

    return {
        "market": str(candles.iloc[0]["market"]),
        "interval_minutes": config.data.interval_minutes,
        "bars": int(len(candles)),
        "start": pd.Timestamp(candles.iloc[0]["timestamp"]).isoformat(),
        "end": pd.Timestamp(candles.iloc[-1]["timestamp"]).isoformat(),
        "evaluation_start": evaluation_start.isoformat(),
        "evaluation_end_exclusive": evaluation_end.isoformat(),
        "evaluation_days": float(elapsed / pd.Timedelta(days=1)),
        "signal_mode": predictions.signal_mode,
        "initial_cash": initial_cash,
        "final_equity": float(equity.iloc[-1]),
        "total_return": total_return,
        "annualized_return": annualized_return,
        "sharpe": sharpe,
        "sortino": sortino,
        "max_drawdown": max_drawdown,
        "buy_and_hold_return": _buy_and_hold_return(candles, config),
        "trade_count": int(len(trades)),
        "win_rate": float((trades["pnl"] > 0).mean()) if len(trades) else None,
        "profit_factor": _safe_ratio(gross_profit, gross_loss),
        "exposure": exposure,
        "turnover": turnover,
        "total_fees": total_fees,
        "total_slippage": total_slippage,
        "prediction_count": valid_signal_count,
        "eligible_signal_count": eligible_signal_count,
        "eligible_signal_share": _safe_ratio(
            float(eligible_signal_count), float(valid_signal_count)
        ),
        "forecast_mae": forecast_mae,
        "forecast_rmse": forecast_rmse,
        "forecast_rank_correlation": forecast_rank_correlation,
        "eligible_realized_mean_return": eligible_realized_mean,
        "model_fit_count": len(predictions.fits),
        "mean_train_auc": float(np.mean(fit_aucs)) if fit_aucs else None,
        "oos_auc": oos_auc,
        "final_halt_state": str(equity_curve.iloc[-1]["halt_state"]),
    }


def run_backtest(
    candles: pd.DataFrame,
    config: AppConfig,
    *,
    predictions: PredictionResult | None = None,
    execution_start: str | pd.Timestamp | None = None,
    execution_end: str | pd.Timestamp | None = None,
) -> BacktestResult:
    frame = validate_candles(candles)
    if predictions is None:
        predictions = generate_walk_forward_predictions(
            frame,
            interval_minutes=config.data.interval_minutes,
            model_config=config.model,
            round_trip_cost=config.round_trip_cost,
        )
    if len(predictions.probabilities) != len(frame):
        raise ValueError("Prediction row count must match candle row count")
    if (
        len(predictions.model_ids) != len(frame)
        or len(predictions.dataset.frame) != len(frame)
        or len(predictions.raw_expected_gross_returns) != len(frame)
        or len(predictions.expected_gross_returns) != len(frame)
        or len(predictions.calibration_buffers) != len(frame)
        or len(predictions.expected_net_edges) != len(frame)
        or len(predictions.signal_eligible) != len(frame)
        or len(predictions.no_trade_reasons) != len(frame)
    ):
        raise ValueError("Prediction metadata row count must match candles")
    prediction_timestamps = pd.to_datetime(
        predictions.dataset.frame["timestamp"], utc=True
    ).reset_index(drop=True)
    candle_timestamps = pd.to_datetime(frame["timestamp"], utc=True).reset_index(
        drop=True
    )
    if not prediction_timestamps.equals(candle_timestamps):
        raise ValueError("Prediction timestamps are not aligned with candles")
    if not predictions.dataset.frame["market"].reset_index(drop=True).equals(
        frame["market"].reset_index(drop=True)
    ):
        raise ValueError("Prediction markets are not aligned with candles")
    finite_probabilities = predictions.probabilities.dropna()
    if (
        not np.isfinite(finite_probabilities.to_numpy()).all()
        or not finite_probabilities.between(0, 1).all()
    ):
        raise ValueError("Predicted probabilities must be between zero and one")
    if predictions.signal_mode != config.model.signal_mode:
        raise ValueError("Predictions use a different signal mode")
    for name, values in {
        "raw expected returns": predictions.raw_expected_gross_returns,
        "expected returns": predictions.expected_gross_returns,
        "calibration buffers": predictions.calibration_buffers,
        "expected net edges": predictions.expected_net_edges,
    }.items():
        finite_values = values.dropna().to_numpy(dtype=float)
        if not np.isfinite(finite_values).all():
            raise ValueError(f"Predicted {name} must be finite")
    alignment_kwargs: dict[str, object] = {}
    if predictions.signal_mode == "expected_return":
        alignment_kwargs = {
            "raw_expected_gross_returns": (
                predictions.raw_expected_gross_returns
            ),
            "expected_gross_returns": predictions.expected_gross_returns,
            "calibration_buffers": predictions.calibration_buffers,
            "expected_net_edges": predictions.expected_net_edges,
            "signal_eligible": predictions.signal_eligible,
            "no_trade_reasons": predictions.no_trade_reasons,
        }
    if predictions.alignment_hash != prediction_alignment_hash(
        predictions.dataset.frame["timestamp"],
        predictions.probabilities,
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
        **alignment_kwargs,
    ):
        raise ValueError("Prediction values are not aligned with their manifest")
    if predictions.fits_hash != fit_manifest_hash(predictions.fits):
        raise ValueError("Prediction fit records do not match their manifest")
    if predictions.model_config_hash != model_config_hash(config.model):
        raise ValueError(
            "Predictions were generated with a different model configuration"
        )
    if predictions.prediction_interval_minutes != config.data.interval_minutes:
        raise ValueError(
            "Predictions were generated for a different candle interval"
        )
    if predictions.source_data_hash != _data_hash(frame):
        raise ValueError("Predictions were generated from different source candles")
    if (
        not np.isfinite(predictions.training_round_trip_cost)
        or predictions.training_round_trip_cost < 0
    ):
        raise ValueError("Prediction training cost must be finite and non-negative")
    expected_threshold = (
        predictions.training_round_trip_cost
        + config.model.minimum_edge_pct
    )
    if not np.isclose(
        predictions.positive_return_threshold,
        expected_threshold,
        rtol=0,
        atol=1e-15,
    ):
        raise ValueError(
            "Prediction target threshold does not match its signal configuration"
        )
    rebuilt_dataset = build_feature_dataset(
        frame,
        interval_minutes=config.data.interval_minutes,
        horizon_bars=config.model.horizon_bars,
        positive_return_threshold=predictions.positive_return_threshold,
    ).frame
    float_dataset_columns = (
        *FEATURE_COLUMNS,
        "forward_return",
        "target",
    )
    for column in float_dataset_columns:
        expected_values = rebuilt_dataset[column].to_numpy(dtype=float)
        actual_values = predictions.dataset.frame[column].to_numpy(dtype=float)
        if not np.allclose(
            actual_values,
            expected_values,
            rtol=0,
            atol=1e-15,
            equal_nan=True,
        ):
            raise ValueError(
                f"Prediction feature dataset does not match source candles: {column}"
            )
    expected_label_times = pd.to_datetime(
        rebuilt_dataset["label_end_time"], utc=True
    ).reset_index(drop=True)
    actual_label_times = pd.to_datetime(
        predictions.dataset.frame["label_end_time"], utc=True
    ).reset_index(drop=True)
    if not expected_label_times.equals(actual_label_times):
        raise ValueError(
            "Prediction feature dataset has mismatched label end times"
        )
    if not predictions.dataset.frame["segment_id"].reset_index(drop=True).equals(
        rebuilt_dataset["segment_id"].reset_index(drop=True)
    ):
        raise ValueError("Prediction feature dataset has mismatched gap segments")
    referenced_model_ids = set(predictions.model_ids.dropna().astype(str))
    if predictions.signal_mode == "trend_breakout":
        if predictions.fits or not referenced_model_ids.issubset(
            {trend_breakout_model_id(config.model)}
        ):
            raise ValueError(
                "Deterministic trend predictions have invalid model provenance"
            )
        rebuilt_scores, rebuilt_model_ids = trend_breakout_scores(
            frame,
            interval_minutes=config.data.interval_minutes,
            model_config=config.model,
        )
        if not np.allclose(
            predictions.probabilities.to_numpy(dtype=float),
            rebuilt_scores.to_numpy(dtype=float),
            rtol=0,
            atol=0,
            equal_nan=True,
        ) or not predictions.model_ids.equals(rebuilt_model_ids):
            raise ValueError(
                "Deterministic trend predictions do not match source candles"
            )
    else:
        fit_ids = [fit.model_id for fit in predictions.fits]
        if len(fit_ids) != len(set(fit_ids)) or not referenced_model_ids.issubset(
            set(fit_ids)
        ):
            raise ValueError(
                "Prediction model IDs do not match the recorded model fits"
            )

    def utc_boundary(
        value: str | pd.Timestamp | None,
    ) -> pd.Timestamp | None:
        if value is None:
            return None
        timestamp = pd.Timestamp(value)
        if timestamp.tzinfo is None:
            return timestamp.tz_localize("UTC")
        return timestamp.tz_convert("UTC")

    start_boundary = utc_boundary(execution_start)
    end_boundary = utc_boundary(execution_end)
    if (
        start_boundary is not None
        and end_boundary is not None
        and start_boundary >= end_boundary
    ):
        raise ValueError("execution_start must be before execution_end")
    candle_times = pd.to_datetime(frame["timestamp"], utc=True)
    execution_mask = pd.Series(True, index=frame.index, dtype=bool)
    if start_boundary is not None:
        execution_mask &= candle_times.ge(start_boundary)
    if end_boundary is not None:
        execution_mask &= candle_times.lt(end_boundary)
    execution_indices = list(frame.index[execution_mask])
    if len(execution_indices) < 2:
        raise ValueError("Execution range must contain at least two candles")
    evaluation_frame = frame.loc[execution_indices].reset_index(drop=True)

    state = PortfolioState.initial(
        market=config.data.market, cash=config.risk.initial_cash
    )
    engine = BarExecutionEngine(
        state=state,
        risk_config=config.risk,
        model_config=config.model,
        interval_minutes=config.data.interval_minutes,
        halt_on_data_gap=False,
    )
    equity_records: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []
    trades: list[Trade] = []
    feature_frame = predictions.dataset.frame

    for index in execution_indices:
        row = frame.loc[index]
        probability = predictions.probabilities.iloc[index]
        probability_value = float(probability) if pd.notna(probability) else None
        expected_gross = predictions.expected_gross_returns.iloc[index]
        expected_gross_value = (
            float(expected_gross) if pd.notna(expected_gross) else None
        )
        calibration_buffer = predictions.calibration_buffers.iloc[index]
        calibration_buffer_value = (
            float(calibration_buffer)
            if pd.notna(calibration_buffer)
            else None
        )
        expected_edge = predictions.expected_net_edges.iloc[index]
        expected_edge_value = (
            float(expected_edge) if pd.notna(expected_edge) else None
        )
        eligible_value = bool(predictions.signal_eligible.iloc[index])
        no_trade_reason = predictions.no_trade_reasons.iloc[index]
        no_trade_reason_value = (
            str(no_trade_reason) if pd.notna(no_trade_reason) else None
        )
        atr = feature_frame.at[index, "atr_pct"]
        atr_value = float(atr) if pd.notna(atr) else None
        model_id = predictions.model_ids.iloc[index]
        model_id_value = str(model_id) if pd.notna(model_id) else None
        step = engine.process_bar(
            row,
            current_probability=probability_value,
            current_atr_pct=atr_value,
            current_model_id=model_id_value,
            current_expected_gross_return=expected_gross_value,
            current_calibration_buffer=calibration_buffer_value,
            current_expected_net_edge=expected_edge_value,
            current_signal_eligible=eligible_value,
            current_no_trade_reason=no_trade_reason_value,
        )
        equity_records.append(step.equity_record())
        events.extend(step.events)
        trades.extend(step.trades)

    final_row = frame.loc[execution_indices[-1]]
    liquidation = engine.liquidate(
        timestamp=(
            pd.Timestamp(final_row["timestamp"])
            + pd.Timedelta(minutes=config.data.interval_minutes)
        ),
        raw_price=float(final_row["close"]),
    )
    if liquidation is not None:
        fill, trade = liquidation
        events.append(
            {
                "event_id": fill.event_id,
                "timestamp": fill.timestamp,
                "event_type": "fill",
                "payload": fill.to_dict(),
            }
        )
        trades.append(trade)
        final_equity = state.cash
        state.peak_equity = max(state.peak_equity, final_equity)
        equity_records[-1].update(
            {
                "equity": final_equity,
                "cash": state.cash,
                "quantity": 0.0,
                "drawdown": max(0.0, 1 - final_equity / state.peak_equity),
                "halt_state": state.halt_state,
            }
        )

    equity_curve = pd.DataFrame(equity_records)
    trade_frame = pd.DataFrame(
        [trade.to_dict() for trade in trades], columns=TRADE_COLUMNS
    )
    event_tuple = tuple(events)
    metrics = _calculate_metrics(
        candles=evaluation_frame,
        equity_curve=equity_curve,
        trades=trade_frame,
        events=event_tuple,
        predictions=predictions,
        config=config,
    )
    return BacktestResult(
        metrics=metrics,
        equity_curve=equity_curve,
        trades=trade_frame,
        events=event_tuple,
        predictions=predictions,
        config=config,
        data_hash=_data_hash(frame),
    )


def write_backtest_artifacts(
    result: BacktestResult,
    directory: str | Path,
    *,
    name: str = "backtest",
) -> dict[str, Path]:
    output = Path(directory)
    output.mkdir(parents=True, exist_ok=True)
    summary_path = output / f"{name}-summary.json"
    equity_path = output / f"{name}-equity.csv"
    trades_path = output / f"{name}-trades.csv"
    predictions_path = output / f"{name}-predictions.csv"
    events_path = output / f"{name}-events.jsonl"
    final_paths = {
        "equity": equity_path,
        "trades": trades_path,
        "predictions": predictions_path,
        "events": events_path,
    }
    token = uuid.uuid4().hex
    temp_paths = {
        key: path.with_name(f".{path.name}.{token}.tmp")
        for key, path in final_paths.items()
    }
    summary_temp = summary_path.with_name(
        f".{summary_path.name}.{token}.tmp"
    )
    prediction_frame = pd.DataFrame(
        {
            "timestamp": result.predictions.dataset.frame["timestamp"],
            "probability": result.predictions.probabilities,
            "raw_expected_gross_return": (
                result.predictions.raw_expected_gross_returns
            ),
            "expected_gross_return": (
                result.predictions.expected_gross_returns
            ),
            "calibration_buffer": result.predictions.calibration_buffers,
            "expected_net_edge": result.predictions.expected_net_edges,
            "signal_eligible": result.predictions.signal_eligible,
            "no_trade_reason": result.predictions.no_trade_reasons,
            "model_id": result.predictions.model_ids,
            "atr_pct": result.predictions.dataset.frame["atr_pct"],
            "forward_return": result.predictions.dataset.frame["forward_return"],
            "target": result.predictions.dataset.frame["target"],
        }
    )
    lock_path = output / f".{name}.artifacts.lock"
    lock_descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        os.chmod(lock_path, 0o600)
        fcntl.flock(lock_descriptor, fcntl.LOCK_EX)
        result.equity_curve.to_csv(temp_paths["equity"], index=False)
        result.trades.to_csv(temp_paths["trades"], index=False)
        prediction_frame.to_csv(temp_paths["predictions"], index=False)
        temp_paths["events"].write_text(
            "".join(
                json.dumps(
                    _strict_json_value(event),
                    ensure_ascii=False,
                    sort_keys=True,
                    allow_nan=False,
                )
                + "\n"
                for event in result.events
            ),
            encoding="utf-8",
        )
        artifact_files = {
            key: {
                "filename": final_paths[key].name,
                "sha256": _file_sha256(temp_paths[key]),
            }
            for key in final_paths
        }
        bundle_material = json.dumps(
            artifact_files, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        manifest = {
            "artifact_schema_version": 3,
            "bundle_id": hashlib.sha256(bundle_material).hexdigest()[:20],
            "artifact_files": artifact_files,
            "metrics": result.metrics,
            "data_hash": result.data_hash,
            "prediction_source_data_hash": result.predictions.source_data_hash,
            "prediction_alignment_hash": result.predictions.alignment_hash,
            "config": result.config.as_dict(),
            "execution_config": result.config.as_dict(),
            "signal_config": {
                "interval_minutes": (
                    result.predictions.prediction_interval_minutes
                ),
                "model": dataclasses.asdict(result.config.model),
                "model_config_hash": result.predictions.model_config_hash,
                "training_round_trip_cost": (
                    result.predictions.training_round_trip_cost
                ),
                "positive_return_threshold": (
                    result.predictions.positive_return_threshold
                ),
                "fits_hash": result.predictions.fits_hash,
            },
            "prediction_target_threshold": (
                result.predictions.positive_return_threshold
            ),
            "fits": [fit.as_dict() for fit in result.predictions.fits],
            "runtime": {
                "python": platform.python_version(),
                "numpy": np.__version__,
                "pandas": pd.__version__,
            },
        }
        summary_temp.write_text(
            json.dumps(
                _strict_json_value(manifest),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
                allow_nan=False,
            ),
            encoding="utf-8",
        )
        for key in ("equity", "trades", "predictions", "events"):
            temp_paths[key].replace(final_paths[key])
        # The hash-bearing summary is the commit marker and is published last.
        summary_temp.replace(summary_path)
    finally:
        for temp_path in (*temp_paths.values(), summary_temp):
            temp_path.unlink(missing_ok=True)
        try:
            fcntl.flock(lock_descriptor, fcntl.LOCK_UN)
        finally:
            os.close(lock_descriptor)
    return {
        "summary": summary_path,
        **final_paths,
    }


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def verify_backtest_artifacts(summary_path: str | Path) -> bool:
    summary = Path(summary_path)
    try:
        manifest = json.loads(summary.read_text(encoding="utf-8"))
        files = manifest["artifact_files"]
    except (OSError, KeyError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError("Backtest artifact manifest is invalid") from exc
    if manifest.get("artifact_schema_version") not in {2, 3} or not isinstance(
        files, dict
    ):
        raise ValueError("Unsupported backtest artifact manifest")
    for key in ("equity", "trades", "predictions", "events"):
        metadata = files.get(key)
        if not isinstance(metadata, dict):
            raise ValueError(f"Backtest artifact metadata is missing: {key}")
        filename = metadata.get("filename")
        expected_hash = metadata.get("sha256")
        if (
            not isinstance(filename, str)
            or Path(filename).name != filename
            or not isinstance(expected_hash, str)
        ):
            raise ValueError(f"Backtest artifact metadata is invalid: {key}")
        artifact_path = summary.parent / filename
        if not artifact_path.is_file() or _file_sha256(
            artifact_path
        ) != expected_hash:
            raise ValueError(f"Backtest artifact hash mismatch: {key}")
    return True


def _strict_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _strict_json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_strict_json_value(item) for item in value]
    if isinstance(value, np.generic):
        return _strict_json_value(value.item())
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def with_scaled_costs(config: AppConfig, multiplier: float) -> AppConfig:
    if (
        isinstance(multiplier, bool)
        or not isinstance(multiplier, (int, float))
        or not np.isfinite(multiplier)
        or multiplier <= 0
    ):
        raise ValueError("Cost multiplier must be positive")
    risk = dataclasses.replace(
        config.risk,
        fee_rate=config.risk.fee_rate * multiplier,
        slippage_bps=config.risk.slippage_bps * multiplier,
    )
    return dataclasses.replace(config, risk=risk).validate()
