from __future__ import annotations

import dataclasses
import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from coinpilot.backtest import BacktestResult, run_backtest, with_scaled_costs
from coinpilot.config import AppConfig
from coinpilot.data import candle_data_hash, validate_candles
from coinpilot.strategy import PredictionResult, generate_walk_forward_predictions


PORTFOLIO_RESEARCH_SCHEMA_VERSION = 1
PASS = "PASS"
FAIL = "FAIL"
INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"
EVALUATED = "EVALUATED"
MAX_CANDIDATES = 4
MAX_MARKETS = 12
MAX_FOLDS = 12
RESEARCH_SIGNAL_MODES = frozenset({"expected_return", "trend_breakout"})
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
RESEARCH_ONLY_CAVEAT = (
    "Historical walk-forward results are research evidence only. This runner "
    "does not rank candidates, select parameters, authorize promotion, or "
    "guarantee future profit. A separate untouched forward paper period is "
    "required before any deployment decision."
)


@dataclass(frozen=True, slots=True)
class CalendarFold:
    """One explicit, half-open calendar out-of-sample interval."""

    fold_id: str
    start: str | pd.Timestamp
    end: str | pd.Timestamp


@dataclass(frozen=True, slots=True)
class PortfolioResearchGates:
    """Predeclared evidence and performance gates for each frozen candidate."""

    minimum_folds: int = 3
    minimum_markets_per_fold: int = 2
    minimum_total_trades: int = 20
    minimum_nonnegative_market_count: int = 3
    minimum_annualized_return: float = 0.0
    minimum_cost_stress_annualized_return: float = 0.0
    minimum_positive_fold_share: float = 0.5
    minimum_cost_stress_positive_fold_share: float = 0.5
    minimum_profit_factor: float = 1.15
    maximum_worst_fold_drawdown: float = 0.15

    def validate(self) -> PortfolioResearchGates:
        for name in (
            "minimum_folds",
            "minimum_markets_per_fold",
            "minimum_total_trades",
            "minimum_nonnegative_market_count",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        for name in (
            "minimum_annualized_return",
            "minimum_cost_stress_annualized_return",
            "minimum_positive_fold_share",
            "minimum_cost_stress_positive_fold_share",
            "minimum_profit_factor",
            "maximum_worst_fold_drawdown",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
            ):
                raise ValueError(f"{name} must be a finite number")
        if self.minimum_annualized_return < -1:
            raise ValueError("minimum_annualized_return cannot be below -1")
        if self.minimum_cost_stress_annualized_return < -1:
            raise ValueError(
                "minimum_cost_stress_annualized_return cannot be below -1"
            )
        if not 0 <= self.minimum_positive_fold_share <= 1:
            raise ValueError("minimum_positive_fold_share must be in [0, 1]")
        if not 0 <= self.minimum_cost_stress_positive_fold_share <= 1:
            raise ValueError(
                "minimum_cost_stress_positive_fold_share must be in [0, 1]"
            )
        if self.minimum_profit_factor < 0:
            raise ValueError("minimum_profit_factor cannot be negative")
        if not 0 <= self.maximum_worst_fold_drawdown < 1:
            raise ValueError("maximum_worst_fold_drawdown must be in [0, 1)")
        return self


@dataclass(frozen=True, slots=True)
class PortfolioResearchResult:
    """Portable research result with JSON- and CSV-safe projections."""

    design_hash: str
    result_hash: str
    source_hashes: dict[str, str]
    candidate_hashes: dict[str, str]
    folds: tuple[dict[str, Any], ...]
    candidates: tuple[dict[str, Any], ...]
    fold_results: tuple[dict[str, Any], ...]
    sleeve_results: tuple[dict[str, Any], ...]
    gate_results: tuple[dict[str, Any], ...]
    equity_curves: tuple[dict[str, Any], ...]
    gates: PortfolioResearchGates
    caveat: str = RESEARCH_ONLY_CAVEAT

    def as_dict(self) -> dict[str, Any]:
        payload = {
            "portfolio_research_schema_version": (
                PORTFOLIO_RESEARCH_SCHEMA_VERSION
            ),
            "research_only": True,
            "decision": {
                "candidate_selection": "NOT_PERFORMED",
                "promotion": "NOT_AUTHORIZED",
                "interpretation": (
                    "Gate status is diagnostic evidence, not a ranking or "
                    "deployment decision."
                ),
            },
            "design_hash": self.design_hash,
            "result_hash": self.result_hash,
            "source_hashes": dict(self.source_hashes),
            "candidate_hashes": dict(self.candidate_hashes),
            "folds": list(self.folds),
            "gates": dataclasses.asdict(self.gates),
            "candidates": list(self.candidates),
            "fold_results": list(self.fold_results),
            "sleeve_results": list(self.sleeve_results),
            "gate_results": list(self.gate_results),
            "equity_curves": list(self.equity_curves),
            "caveat": self.caveat,
        }
        return _json_safe(payload)

    def csv_frames(self) -> dict[str, pd.DataFrame]:
        """Return flat frames whose cells are CSV-safe scalar values."""

        return {
            "candidates": pd.DataFrame(self.candidates),
            "folds": pd.DataFrame(self.fold_results),
            "sleeves": pd.DataFrame(self.sleeve_results),
            "gates": pd.DataFrame(self.gate_results),
            "equity": pd.DataFrame(self.equity_curves),
        }


def _utc_boundary(value: str | pd.Timestamp, *, name: str) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if pd.isna(timestamp):
        raise ValueError(f"{name} must be a valid timestamp")
    if timestamp.tzinfo is None:
        raise ValueError(f"{name} must include an explicit UTC offset")
    return timestamp.tz_convert("UTC")


def _canonical_hash(value: Any) -> str:
    material = json.dumps(
        _json_safe(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _model_policy(config: AppConfig) -> dict[str, Any]:
    model = config.model
    if model.signal_mode == "trend_breakout":
        return {
            "signal_mode": model.signal_mode,
            "horizon_bars": model.horizon_bars,
            "entry_probability": model.entry_probability,
            "exit_probability": model.exit_probability,
            "breakout_entry_window": model.breakout_entry_window,
            "breakout_exit_window": model.breakout_exit_window,
        }
    if model.signal_mode == "expected_return":
        return {
            "signal_mode": model.signal_mode,
            "horizon_bars": model.horizon_bars,
            "min_train_samples": model.min_train_samples,
            "train_window": model.train_window,
            "retrain_every": model.retrain_every,
            "minimum_edge_pct": model.minimum_edge_pct,
            "l2": model.l2,
            "calibration_window": model.calibration_window,
            "calibration_min_samples": model.calibration_min_samples,
            "calibration_uncertainty_z": model.calibration_uncertainty_z,
            "target_clip_quantile": model.target_clip_quantile,
            "regime_sma_window": model.regime_sma_window,
            "regime_sma_gap_min": model.regime_sma_gap_min,
        }
    raise ValueError(f"Unsupported research signal mode: {model.signal_mode}")


def _candidate_policy_hash(config: AppConfig) -> str:
    """Hash a market-agnostic strategy policy with no local path material."""

    return _canonical_hash(
        {
            "schema_version": PORTFOLIO_RESEARCH_SCHEMA_VERSION,
            "interval_minutes": config.data.interval_minutes,
            "model": _model_policy(config),
            "risk": dataclasses.asdict(config.risk),
        }
    )


def _execution_config_hash(config: AppConfig) -> str:
    """Hash the effective per-market strategy execution policy."""

    return _canonical_hash(
        {
            "candidate_policy_sha256": _candidate_policy_hash(config),
            "market": config.data.market,
        }
    )


def _json_safe(value: Any) -> Any:
    if dataclasses.is_dataclass(value):
        return _json_safe(dataclasses.asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        number = float(value)
        return number if math.isfinite(number) else None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if value is pd.NA:
        return None
    return value


def _finite_number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(
        value, (int, float, np.integer, np.floating)
    ):
        return None
    result = float(value)
    return result if math.isfinite(result) else None


def _normalize_folds(
    folds: Sequence[CalendarFold],
) -> tuple[dict[str, Any], ...]:
    if not folds:
        raise ValueError("At least one calendar fold is required")
    if len(folds) > MAX_FOLDS:
        raise ValueError(f"At most {MAX_FOLDS} calendar folds are allowed")
    normalized: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for index, fold in enumerate(folds):
        if not isinstance(fold, CalendarFold):
            raise TypeError("folds must contain CalendarFold values")
        if not _IDENTIFIER_PATTERN.fullmatch(fold.fold_id):
            raise ValueError(f"Invalid fold_id: {fold.fold_id!r}")
        if fold.fold_id in seen_ids:
            raise ValueError(f"Duplicate fold_id: {fold.fold_id}")
        seen_ids.add(fold.fold_id)
        start = _utc_boundary(fold.start, name=f"folds[{index}].start")
        end = _utc_boundary(fold.end, name=f"folds[{index}].end")
        if start >= end:
            raise ValueError(f"Fold {fold.fold_id} start must be before end")
        normalized.append(
            {
                "fold_id": fold.fold_id,
                "start": start.isoformat(),
                "end_exclusive": end.isoformat(),
                "calendar_days": float((end - start) / pd.Timedelta(days=1)),
            }
        )
    normalized.sort(key=lambda item: (item["start"], item["fold_id"]))
    for previous, current in zip(normalized, normalized[1:], strict=False):
        if pd.Timestamp(current["start"]) < pd.Timestamp(
            previous["end_exclusive"]
        ):
            raise ValueError(
                f"Calendar folds overlap: {previous['fold_id']} and "
                f"{current['fold_id']}"
            )
    return tuple(normalized)


def _snapshot_candidates(
    candidates: Mapping[str, AppConfig],
) -> tuple[tuple[str, AppConfig], ...]:
    if not candidates:
        raise ValueError("At least one frozen candidate is required")
    if len(candidates) > MAX_CANDIDATES:
        raise ValueError(f"At most {MAX_CANDIDATES} candidates are allowed")
    snapshot: list[tuple[str, AppConfig]] = []
    hashes: set[str] = set()
    for candidate_id, supplied_config in sorted(candidates.items()):
        if not isinstance(candidate_id, str) or not _IDENTIFIER_PATTERN.fullmatch(
            candidate_id
        ):
            raise ValueError(f"Invalid candidate id: {candidate_id!r}")
        if not isinstance(supplied_config, AppConfig):
            raise TypeError("candidate values must be AppConfig instances")
        config = supplied_config.validate()
        if config.model.signal_mode not in RESEARCH_SIGNAL_MODES:
            raise ValueError(
                f"Candidate {candidate_id} signal_mode must be expected_return "
                "or trend_breakout"
            )
        config_hash = _candidate_policy_hash(config)
        if config_hash in hashes:
            raise ValueError("Frozen candidates must have distinct configurations")
        hashes.add(config_hash)
        snapshot.append((candidate_id, config))
    return tuple(snapshot)


def _snapshot_sources(
    candles_by_market: Mapping[str, pd.DataFrame],
) -> tuple[dict[str, pd.DataFrame], dict[str, str]]:
    if not candles_by_market:
        raise ValueError("At least one market candle frame is required")
    if len(candles_by_market) > MAX_MARKETS:
        raise ValueError(f"At most {MAX_MARKETS} markets are allowed")
    frames: dict[str, pd.DataFrame] = {}
    hashes: dict[str, str] = {}
    for market, supplied_frame in sorted(candles_by_market.items()):
        if not isinstance(market, str) or not market.strip():
            raise ValueError("Market keys must be non-empty strings")
        frame = validate_candles(supplied_frame)
        source_market = str(frame.iloc[0]["market"])
        if source_market != market:
            raise ValueError(
                f"Market key {market} does not match candle market {source_market}"
            )
        frames[market] = frame
        hashes[market] = candle_data_hash(frame)
    return frames, hashes


def _market_config(template: AppConfig, market: str) -> AppConfig:
    data = dataclasses.replace(template.data, market=market)
    return dataclasses.replace(template, data=data).validate()


def _fold_coverage(
    frame: pd.DataFrame,
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
    interval_minutes: int,
) -> tuple[bool, int, str | None]:
    timestamps = pd.to_datetime(frame["timestamp"], utc=True)
    selected = timestamps.loc[timestamps.ge(start) & timestamps.lt(end)]
    if len(selected) < 2:
        return False, int(len(selected)), "fewer_than_two_candles"
    interval = pd.Timedelta(minutes=interval_minutes)
    if selected.iloc[0] >= start + interval:
        return False, int(len(selected)), "missing_fold_start_boundary"
    if selected.iloc[-1] + interval < end:
        return False, int(len(selected)), "missing_fold_end_boundary"
    return True, int(len(selected)), None


def _profit_components(results: Mapping[str, BacktestResult]) -> tuple[float, float]:
    gross_profit = 0.0
    gross_loss = 0.0
    for result in results.values():
        if result.trades.empty or "pnl" not in result.trades:
            continue
        pnl = pd.to_numeric(result.trades["pnl"], errors="coerce").dropna()
        gross_profit += float(pnl.loc[pnl > 0].sum())
        gross_loss += float(-pnl.loc[pnl < 0].sum())
    return gross_profit, gross_loss


def _portfolio_metrics_and_curve(
    results: Mapping[str, BacktestResult],
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> tuple[dict[str, Any], tuple[dict[str, Any], ...], str]:
    if not results:
        raise ValueError("At least one sleeve result is required")
    normalized: dict[str, pd.Series] = {}
    timestamps: set[pd.Timestamp] = {start, end}
    for market, result in sorted(results.items()):
        initial_cash = _finite_number(result.metrics.get("initial_cash"))
        if initial_cash is None or initial_cash <= 0:
            raise ValueError(f"Sleeve {market} has invalid initial_cash")
        curve = result.equity_curve.loc[:, ["timestamp", "equity"]].copy()
        curve["timestamp"] = pd.to_datetime(curve["timestamp"], utc=True)
        curve["equity"] = pd.to_numeric(curve["equity"], errors="raise")
        curve = curve.loc[
            curve["timestamp"].ge(start) & curve["timestamp"].lt(end)
        ]
        if curve.empty or curve["timestamp"].duplicated().any():
            raise ValueError(f"Sleeve {market} has an invalid equity curve")
        values = curve["equity"].to_numpy(dtype=float)
        if not np.isfinite(values).all() or (values <= 0).any():
            raise ValueError(f"Sleeve {market} equity must be finite and positive")
        series = pd.Series(
            values / initial_cash,
            index=pd.DatetimeIndex(curve["timestamp"]),
            name=market,
        )
        normalized[market] = series
        timestamps.update(series.index.to_list())

    index = pd.DatetimeIndex(sorted(timestamps))
    aligned = pd.DataFrame(index=index)
    for market, series in normalized.items():
        aligned[market] = series.reindex(index).ffill().fillna(1.0)
    equity_index = aligned.mean(axis=1)
    equity_index.iloc[0] = 1.0
    final_index = float(equity_index.iloc[-1])
    total_return = final_index - 1.0
    calendar_days = float((end - start) / pd.Timedelta(days=1))
    annualized_return = (
        float(final_index ** (365.25 / calendar_days) - 1)
        if calendar_days > 0 and final_index > 0
        else None
    )
    drawdown = equity_index / equity_index.cummax() - 1
    max_drawdown = max(0.0, float(-drawdown.min()))
    daily = equity_index.resample("1D").last()
    daily_returns = daily.pct_change().replace([np.inf, -np.inf], np.nan).dropna()
    daily_std = float(daily_returns.std(ddof=0)) if len(daily_returns) else 0.0
    sharpe = (
        float(daily_returns.mean() / daily_std * math.sqrt(365.25))
        if daily_std > 0
        else None
    )
    gross_profit, gross_loss = _profit_components(results)
    profit_factor = (
        float(gross_profit / gross_loss) if gross_loss > 0 else None
    )
    sleeve_returns = [
        _finite_number(result.metrics.get("total_return"))
        for result in results.values()
    ]
    finite_sleeve_returns = [value for value in sleeve_returns if value is not None]
    halted_markets = sorted(
        market
        for market, result in results.items()
        if str(result.metrics.get("final_halt_state", "")).upper() == "HALTED"
    )
    metrics = {
        "market_count": len(results),
        "calendar_days": calendar_days,
        "initial_equity_index": 1.0,
        "final_equity_index": final_index,
        "total_return": total_return,
        "annualized_return": annualized_return,
        "max_drawdown": max_drawdown,
        "sharpe": sharpe,
        "trade_count": int(
            sum(
                int(result.metrics.get("trade_count", 0))
                for result in results.values()
            )
        ),
        "profit_factor": profit_factor,
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
        "total_fees": float(
            sum(
                float(result.metrics.get("total_fees", 0.0))
                for result in results.values()
            )
        ),
        "total_slippage": float(
            sum(
                float(result.metrics.get("total_slippage", 0.0))
                for result in results.values()
            )
        ),
        "positive_sleeve_share": (
            float(
                sum(value > 0 for value in finite_sleeve_returns)
                / len(finite_sleeve_returns)
            )
            if finite_sleeve_returns
            else None
        ),
        "halted_market_count": len(halted_markets),
        "halted_markets": "|".join(halted_markets),
    }
    curve_rows = tuple(
        {
            "timestamp": timestamp.isoformat(),
            "equity_index": float(value),
        }
        for timestamp, value in equity_index.items()
    )
    curve_hash = _canonical_hash(curve_rows)
    return metrics, curve_rows, curve_hash


def _fold_row(
    *,
    candidate_id: str,
    fold: Mapping[str, Any],
    covered_markets: Sequence[str],
    missing_markets: Mapping[str, str],
    base_metrics: Mapping[str, Any] | None,
    stress_metrics: Mapping[str, Any] | None,
    base_curve_hash: str | None,
    stress_curve_hash: str | None,
    prediction_bundle_hash: str | None,
    evidence_eligible: bool,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "candidate_id": candidate_id,
        "fold_id": fold["fold_id"],
        "start": fold["start"],
        "end_exclusive": fold["end_exclusive"],
        "calendar_days": fold["calendar_days"],
        "gate_status": EVALUATED if evidence_eligible else INSUFFICIENT_EVIDENCE,
        "evidence_eligible": evidence_eligible,
        "market_count": len(covered_markets),
        "markets": "|".join(sorted(covered_markets)),
        "missing_markets": "|".join(sorted(missing_markets)),
        "missing_market_reasons": "|".join(
            f"{market}:{reason}" for market, reason in sorted(missing_markets.items())
        ),
        "prediction_bundle_sha256": prediction_bundle_hash,
        "base_curve_sha256": base_curve_hash,
        "cost_stress_2x_curve_sha256": stress_curve_hash,
    }
    metric_names = (
        "final_equity_index",
        "total_return",
        "annualized_return",
        "max_drawdown",
        "sharpe",
        "trade_count",
        "profit_factor",
        "gross_profit",
        "gross_loss",
        "total_fees",
        "total_slippage",
        "positive_sleeve_share",
        "halted_market_count",
        "halted_markets",
    )
    for prefix, metrics in (
        ("base", base_metrics),
        ("cost_stress_2x", stress_metrics),
    ):
        for name in metric_names:
            row[f"{prefix}_{name}"] = metrics.get(name) if metrics else None
    return _json_safe(row)


def _sleeve_row(
    *,
    candidate_id: str,
    fold: Mapping[str, Any],
    market: str,
    source_hash: str,
    config_hash: str,
    predictions: PredictionResult,
    base: BacktestResult,
    stress: BacktestResult,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "candidate_id": candidate_id,
        "fold_id": fold["fold_id"],
        "market": market,
        "start": fold["start"],
        "end_exclusive": fold["end_exclusive"],
        "source_data_sha256": source_hash,
        "execution_config_sha256": config_hash,
        "model_config_sha256": predictions.model_config_hash,
        "prediction_alignment_sha256": predictions.alignment_hash,
        "fit_manifest_sha256": predictions.fits_hash,
        "prediction_reused_for_cost_stress": True,
    }
    metric_names = (
        "total_return",
        "annualized_return",
        "max_drawdown",
        "trade_count",
        "profit_factor",
        "total_fees",
        "total_slippage",
        "eligible_signal_count",
        "final_halt_state",
    )
    for prefix, result in (("base", base), ("cost_stress_2x", stress)):
        for name in metric_names:
            row[f"{prefix}_{name}"] = result.metrics.get(name)
    return _json_safe(row)


def _compound_returns(values: Sequence[float]) -> float | None:
    if not values:
        return None
    factor = float(np.prod([1.0 + value for value in values]))
    return factor - 1.0 if math.isfinite(factor) else None


def _candidate_summary_and_gates(
    *,
    candidate_id: str,
    candidate_hash: str,
    fold_rows: Sequence[dict[str, Any]],
    sleeve_rows: Sequence[dict[str, Any]],
    gates: PortfolioResearchGates,
) -> tuple[dict[str, Any], tuple[dict[str, Any], ...]]:
    eligible = [row for row in fold_rows if row["evidence_eligible"]]
    eligible_fold_ids = {str(row["fold_id"]) for row in eligible}
    every_fold_has_breadth = all(
        int(row["market_count"]) >= gates.minimum_markets_per_fold
        for row in fold_rows
    )
    base_returns = [
        float(row["base_total_return"])
        for row in eligible
        if _finite_number(row["base_total_return"]) is not None
    ]
    stress_returns = [
        float(row["cost_stress_2x_total_return"])
        for row in eligible
        if _finite_number(row["cost_stress_2x_total_return"]) is not None
    ]
    total_days = float(sum(float(row["calendar_days"]) for row in eligible))
    base_compounded = _compound_returns(base_returns)
    stress_compounded = _compound_returns(stress_returns)
    base_annualized = (
        float((1 + base_compounded) ** (365.25 / total_days) - 1)
        if base_compounded is not None and base_compounded > -1 and total_days > 0
        else None
    )
    stress_annualized = (
        float((1 + stress_compounded) ** (365.25 / total_days) - 1)
        if stress_compounded is not None
        and stress_compounded > -1
        and total_days > 0
        else None
    )
    positive_fold_share = (
        float(sum(value > 0 for value in base_returns) / len(base_returns))
        if base_returns
        else None
    )
    cost_stress_positive_fold_share = (
        float(sum(value > 0 for value in stress_returns) / len(stress_returns))
        if stress_returns
        else None
    )
    worst_drawdown_values = [
        float(row["base_max_drawdown"])
        for row in eligible
        if _finite_number(row["base_max_drawdown"]) is not None
    ]
    stress_drawdown_values = [
        float(row["cost_stress_2x_max_drawdown"])
        for row in eligible
        if _finite_number(row["cost_stress_2x_max_drawdown"]) is not None
    ]
    worst_drawdown = (
        max((*worst_drawdown_values, *stress_drawdown_values))
        if worst_drawdown_values or stress_drawdown_values
        else None
    )
    total_trades = int(
        sum(int(row["base_trade_count"] or 0) for row in eligible)
    )
    base_gross_profit = float(
        sum(float(row["base_gross_profit"] or 0.0) for row in eligible)
    )
    base_gross_loss = float(
        sum(float(row["base_gross_loss"] or 0.0) for row in eligible)
    )
    base_profit_factor = (
        float(base_gross_profit / base_gross_loss)
        if base_gross_loss > 0
        else None
    )
    unbounded_profit_factor = base_gross_loss == 0 and base_gross_profit > 0
    profit_factor_actual: float | str | None = base_profit_factor
    if unbounded_profit_factor:
        profit_factor_actual = "UNBOUNDED"

    market_fold_returns: dict[str, dict[str, float]] = {}
    for row in sleeve_rows:
        fold_id = str(row["fold_id"])
        value = _finite_number(row.get("base_total_return"))
        if fold_id not in eligible_fold_ids or value is None:
            continue
        market_fold_returns.setdefault(str(row["market"]), {})[fold_id] = value
    market_compounded_returns: dict[str, float] = {}
    for market, returns_by_fold in sorted(market_fold_returns.items()):
        if set(returns_by_fold) != eligible_fold_ids:
            continue
        compounded = _compound_returns(
            [returns_by_fold[fold_id] for fold_id in sorted(eligible_fold_ids)]
        )
        if compounded is not None:
            market_compounded_returns[market] = compounded
    nonnegative_markets = sorted(
        market
        for market, value in market_compounded_returns.items()
        if value >= 0
    )
    nonnegative_market_count = len(nonnegative_markets)
    halted_sleeves = int(
        sum(
            int(row["base_halted_market_count"] or 0)
            + int(row["cost_stress_2x_halted_market_count"] or 0)
            for row in eligible
        )
    )

    gate_specs = (
        (
            "minimum_fold_count",
            len(eligible),
            ">=",
            gates.minimum_folds,
            len(eligible) >= gates.minimum_folds,
            True,
        ),
        (
            "all_folds_have_market_breadth",
            every_fold_has_breadth,
            "==",
            True,
            every_fold_has_breadth,
            True,
        ),
        (
            "minimum_total_trades",
            total_trades,
            ">=",
            gates.minimum_total_trades,
            total_trades >= gates.minimum_total_trades,
            True,
        ),
        (
            "minimum_profit_factor",
            profit_factor_actual,
            ">=",
            gates.minimum_profit_factor,
            unbounded_profit_factor
            or (
                base_profit_factor is not None
                and base_profit_factor >= gates.minimum_profit_factor
            ),
            base_profit_factor is not None or unbounded_profit_factor,
        ),
        (
            "minimum_annualized_return",
            base_annualized,
            ">=",
            gates.minimum_annualized_return,
            base_annualized is not None
            and base_annualized >= gates.minimum_annualized_return,
            base_annualized is not None,
        ),
        (
            "minimum_cost_stress_annualized_return",
            stress_annualized,
            ">=",
            gates.minimum_cost_stress_annualized_return,
            stress_annualized is not None
            and stress_annualized
            >= gates.minimum_cost_stress_annualized_return,
            stress_annualized is not None,
        ),
        (
            "minimum_positive_fold_share",
            positive_fold_share,
            ">=",
            gates.minimum_positive_fold_share,
            positive_fold_share is not None
            and positive_fold_share >= gates.minimum_positive_fold_share,
            positive_fold_share is not None,
        ),
        (
            "minimum_cost_stress_positive_fold_share",
            cost_stress_positive_fold_share,
            ">=",
            gates.minimum_cost_stress_positive_fold_share,
            cost_stress_positive_fold_share is not None
            and cost_stress_positive_fold_share
            >= gates.minimum_cost_stress_positive_fold_share,
            cost_stress_positive_fold_share is not None,
        ),
        (
            "minimum_nonnegative_market_count",
            nonnegative_market_count,
            ">=",
            gates.minimum_nonnegative_market_count,
            nonnegative_market_count
            >= gates.minimum_nonnegative_market_count,
            bool(eligible_fold_ids),
        ),
        (
            "maximum_worst_fold_drawdown",
            worst_drawdown,
            "<=",
            gates.maximum_worst_fold_drawdown,
            worst_drawdown is not None
            and worst_drawdown <= gates.maximum_worst_fold_drawdown,
            worst_drawdown is not None,
        ),
        (
            "no_halted_sleeves",
            halted_sleeves,
            "==",
            0,
            halted_sleeves == 0,
            True,
        ),
    )
    gate_rows = tuple(
        {
            "candidate_id": candidate_id,
            "gate": name,
            "actual": _json_safe(actual),
            "operator": operator,
            "threshold": _json_safe(threshold),
            "evaluated": evaluated,
            "passed": bool(passed),
        }
        for name, actual, operator, threshold, passed, evaluated in gate_specs
    )
    structural_names = {
        "minimum_fold_count",
        "all_folds_have_market_breadth",
        "minimum_total_trades",
    }
    structurally_sufficient = all(
        row["passed"] for row in gate_rows if row["gate"] in structural_names
    )
    if not structurally_sufficient:
        gate_status = INSUFFICIENT_EVIDENCE
    else:
        gate_status = (
            PASS
            if all(row["evaluated"] and row["passed"] for row in gate_rows)
            else FAIL
        )
    failed_gates = [row["gate"] for row in gate_rows if not row["passed"]]
    summary = {
        "candidate_id": candidate_id,
        "candidate_config_sha256": candidate_hash,
        "gate_status": gate_status,
        "promotion_status": "NOT_AUTHORIZED",
        "requested_fold_count": len(fold_rows),
        "evidence_eligible_fold_count": len(eligible),
        "total_evaluation_days": total_days,
        "base_compounded_return": base_compounded,
        "base_annualized_return": base_annualized,
        "cost_stress_2x_compounded_return": stress_compounded,
        "cost_stress_2x_annualized_return": stress_annualized,
        "positive_fold_share": positive_fold_share,
        "cost_stress_positive_fold_share": (
            cost_stress_positive_fold_share
        ),
        "worst_fold_drawdown": worst_drawdown,
        "total_trades": total_trades,
        "base_gross_profit": base_gross_profit,
        "base_gross_loss": base_gross_loss,
        "base_profit_factor": base_profit_factor,
        "base_profit_factor_unbounded": unbounded_profit_factor,
        "nonnegative_market_count": nonnegative_market_count,
        "nonnegative_markets": "|".join(nonnegative_markets),
        "market_compounded_returns": "|".join(
            f"{market}:{value:.12g}"
            for market, value in market_compounded_returns.items()
        ),
        "halted_sleeve_count": halted_sleeves,
        "failed_gates": "|".join(failed_gates),
    }
    return _json_safe(summary), gate_rows


def run_portfolio_research(
    candles_by_market: Mapping[str, pd.DataFrame],
    candidates: Mapping[str, AppConfig],
    folds: Sequence[CalendarFold],
    *,
    gates: PortfolioResearchGates | None = None,
) -> PortfolioResearchResult:
    """Evaluate frozen return-model or trend candidates without choosing a winner.

    Candle frames are caller-supplied and snapshotted; this module performs no
    network access. Predictions are generated once per candidate and market,
    then the same prediction object is reused for every fold and for both base
    and 2x execution-cost runs. Each candidate is reported independently as an
    equal-weight portfolio of the markets with complete fold coverage. A
    candidate ``AppConfig`` is a template: its data market is replaced by each
    supplied mapping key while every model and risk parameter remains frozen.
    """

    criteria = (gates or PortfolioResearchGates()).validate()
    normalized_folds = _normalize_folds(folds)
    candidate_snapshot = _snapshot_candidates(candidates)
    source_frames, source_hashes = _snapshot_sources(candles_by_market)
    if criteria.minimum_folds > len(normalized_folds):
        raise ValueError("minimum_folds cannot exceed the supplied fold count")
    if criteria.minimum_markets_per_fold > len(source_frames):
        raise ValueError(
            "minimum_markets_per_fold cannot exceed the supplied market count"
        )
    if criteria.minimum_nonnegative_market_count > len(source_frames):
        raise ValueError(
            "minimum_nonnegative_market_count cannot exceed the supplied "
            "market count"
        )

    candidate_hashes = {
        candidate_id: _candidate_policy_hash(config)
        for candidate_id, config in candidate_snapshot
    }
    design_payload = {
        "schema_version": PORTFOLIO_RESEARCH_SCHEMA_VERSION,
        "candidate_hashes": candidate_hashes,
        "source_markets": sorted(source_frames),
        "folds": normalized_folds,
        "gates": dataclasses.asdict(criteria),
        "cost_scenarios": {"base": 1.0, "cost_stress_2x": 2.0},
        "weighting": "equal_weight_market_sleeves_per_candidate",
        "candidate_selection": "NOT_PERFORMED",
    }
    design_hash = _canonical_hash(design_payload)

    fold_results: list[dict[str, Any]] = []
    sleeve_results: list[dict[str, Any]] = []
    gate_results: list[dict[str, Any]] = []
    equity_curves: list[dict[str, Any]] = []
    candidate_summaries: list[dict[str, Any]] = []

    for candidate_id, template in candidate_snapshot:
        market_configs: dict[str, AppConfig] = {}
        predictions: dict[str, PredictionResult] = {}
        for market, frame in source_frames.items():
            market_config = _market_config(template, market)
            market_configs[market] = market_config
            predictions[market] = generate_walk_forward_predictions(
                frame,
                interval_minutes=market_config.data.interval_minutes,
                model_config=market_config.model,
                round_trip_cost=market_config.round_trip_cost,
            )

        candidate_fold_rows: list[dict[str, Any]] = []
        candidate_sleeve_rows: list[dict[str, Any]] = []
        for fold in normalized_folds:
            start = pd.Timestamp(fold["start"])
            end = pd.Timestamp(fold["end_exclusive"])
            covered_markets: list[str] = []
            missing_markets: dict[str, str] = {}
            for market, frame in source_frames.items():
                coverage, _, reason = _fold_coverage(
                    frame,
                    start=start,
                    end=end,
                    interval_minutes=market_configs[market].data.interval_minutes,
                )
                if coverage:
                    covered_markets.append(market)
                else:
                    missing_markets[market] = reason or "incomplete_coverage"

            base_results: dict[str, BacktestResult] = {}
            stress_results: dict[str, BacktestResult] = {}
            for market in sorted(covered_markets):
                config = market_configs[market]
                frozen_predictions = predictions[market]
                base = run_backtest(
                    source_frames[market],
                    config,
                    predictions=frozen_predictions,
                    execution_start=start,
                    execution_end=end,
                )
                stress = run_backtest(
                    source_frames[market],
                    with_scaled_costs(config, 2.0),
                    predictions=frozen_predictions,
                    execution_start=start,
                    execution_end=end,
                )
                if (
                    base.predictions is not frozen_predictions
                    or stress.predictions is not frozen_predictions
                    or base.predictions.alignment_hash
                    != stress.predictions.alignment_hash
                ):
                    raise RuntimeError(
                        "Base and 2x cost executions must reuse frozen predictions"
                    )
                base_results[market] = base
                stress_results[market] = stress
                sleeve_row = _sleeve_row(
                    candidate_id=candidate_id,
                    fold=fold,
                    market=market,
                    source_hash=source_hashes[market],
                        config_hash=_execution_config_hash(config),
                    predictions=frozen_predictions,
                    base=base,
                    stress=stress,
                )
                sleeve_results.append(sleeve_row)
                candidate_sleeve_rows.append(sleeve_row)

            base_metrics: dict[str, Any] | None = None
            stress_metrics: dict[str, Any] | None = None
            base_curve_hash: str | None = None
            stress_curve_hash: str | None = None
            prediction_bundle_hash: str | None = None
            if covered_markets:
                base_metrics, base_curve, base_curve_hash = (
                    _portfolio_metrics_and_curve(
                        base_results,
                        start=start,
                        end=end,
                    )
                )
                stress_metrics, stress_curve, stress_curve_hash = (
                    _portfolio_metrics_and_curve(
                        stress_results,
                        start=start,
                        end=end,
                    )
                )
                prediction_bundle_hash = _canonical_hash(
                    {
                        market: predictions[market].alignment_hash
                        for market in sorted(covered_markets)
                    }
                )
                for scenario, curve in (
                    ("base", base_curve),
                    ("cost_stress_2x", stress_curve),
                ):
                    equity_curves.extend(
                        {
                            "candidate_id": candidate_id,
                            "fold_id": fold["fold_id"],
                            "scenario": scenario,
                            **row,
                        }
                        for row in curve
                    )
            evidence_eligible = (
                len(covered_markets) >= criteria.minimum_markets_per_fold
            )
            fold_row = _fold_row(
                candidate_id=candidate_id,
                fold=fold,
                covered_markets=covered_markets,
                missing_markets=missing_markets,
                base_metrics=base_metrics,
                stress_metrics=stress_metrics,
                base_curve_hash=base_curve_hash,
                stress_curve_hash=stress_curve_hash,
                prediction_bundle_hash=prediction_bundle_hash,
                evidence_eligible=evidence_eligible,
            )
            fold_results.append(fold_row)
            candidate_fold_rows.append(fold_row)

        summary, candidate_gate_rows = _candidate_summary_and_gates(
            candidate_id=candidate_id,
            candidate_hash=candidate_hashes[candidate_id],
            fold_rows=candidate_fold_rows,
            sleeve_rows=candidate_sleeve_rows,
            gates=criteria,
        )
        candidate_summaries.append(summary)
        gate_results.extend(candidate_gate_rows)

    result_payload = {
        "design_hash": design_hash,
        "source_hashes": source_hashes,
        "candidate_hashes": candidate_hashes,
        "folds": normalized_folds,
        "candidates": candidate_summaries,
        "fold_results": fold_results,
        "sleeve_results": sleeve_results,
        "gate_results": gate_results,
        "equity_curves": equity_curves,
        "caveat": RESEARCH_ONLY_CAVEAT,
    }
    result_hash = _canonical_hash(result_payload)
    return PortfolioResearchResult(
        design_hash=design_hash,
        result_hash=result_hash,
        source_hashes=dict(source_hashes),
        candidate_hashes=dict(candidate_hashes),
        folds=tuple(normalized_folds),
        candidates=tuple(candidate_summaries),
        fold_results=tuple(fold_results),
        sleeve_results=tuple(sleeve_results),
        gate_results=tuple(gate_results),
        equity_curves=tuple(equity_curves),
        gates=criteria,
    )
