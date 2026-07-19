from __future__ import annotations

import dataclasses
import hashlib
import json
import math
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from coinpilot.backtest import BacktestResult, run_backtest, with_scaled_costs
from coinpilot.config import AppConfig
from coinpilot.data import candle_data_hash, validate_candles
from coinpilot.strategy import generate_walk_forward_predictions


DEV_DIAGNOSTIC = "DEV_DIAGNOSTIC"
HOLDOUT_CONFIRMATION = "HOLDOUT_CONFIRMATION"
PASS = "PASS"
FAIL = "FAIL"
INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"
RESEARCH_SCHEMA_VERSION = 1
HISTORICAL_HOLDOUT_CAVEAT = (
    "This is a historical confirmation holdout, not proof of an untouched "
    "future sample and not a guarantee of profit. Repeated evaluation or "
    "parameter changes after seeing these results contaminate the holdout; "
    "forward paper trading is still required."
)


@dataclass(frozen=True, slots=True)
class ResearchGates:
    minimum_holdout_days: float = 365.0
    minimum_annualized_return: float = 0.20
    maximum_drawdown: float = 0.15
    minimum_profit_factor: float = 1.15
    minimum_cost_stress_annualized_return: float = 0.0

    def validate(self) -> ResearchGates:
        values = dataclasses.asdict(self)
        for name, value in values.items():
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
            ):
                raise ValueError(f"{name} must be a finite number")
        if self.minimum_holdout_days <= 0:
            raise ValueError("minimum_holdout_days must be positive")
        if self.minimum_annualized_return < -1:
            raise ValueError("minimum_annualized_return cannot be below -1")
        if not 0 <= self.maximum_drawdown < 1:
            raise ValueError("maximum_drawdown must be in [0, 1)")
        if self.minimum_profit_factor < 0:
            raise ValueError("minimum_profit_factor cannot be negative")
        if self.minimum_cost_stress_annualized_return < -1:
            raise ValueError(
                "minimum_cost_stress_annualized_return cannot be below -1"
            )
        return self


@dataclass(frozen=True, slots=True)
class GateOutcome:
    passed: bool
    evaluated: bool
    actual: Any
    operator: str
    threshold: Any

    def as_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


@dataclass(frozen=True, slots=True)
class ResearchAssessment:
    status: str
    holdout_start: str
    holdout_end_exclusive: str
    holdout_calendar_days: float
    development_metrics: dict[str, Any] | None
    confirmation_metrics: dict[str, Any] | None
    cost_stress_2x_metrics: dict[str, Any] | None
    gates: dict[str, GateOutcome]
    hashes: dict[str, str | None]
    caveat: str = HISTORICAL_HOLDOUT_CAVEAT

    def as_dict(self) -> dict[str, Any]:
        development_status = (
            "COMPLETED" if self.development_metrics is not None else "NOT_RUN"
        )
        confirmation_status = (
            "COMPLETED" if self.confirmation_metrics is not None else "NOT_RUN"
        )
        return {
            "research_schema_version": RESEARCH_SCHEMA_VERSION,
            "status": self.status,
            "objective": {
                "annualized_return": self.gates[
                    "annualized_return"
                ].threshold,
                "interpretation": (
                    "A validation gate for this candidate, not a promised return."
                ),
            },
            "holdout": {
                "start": self.holdout_start,
                "end_exclusive": self.holdout_end_exclusive,
                "calendar_days": self.holdout_calendar_days,
            },
            "phases": {
                DEV_DIAGNOSTIC: {
                    "status": development_status,
                    "metrics": self.development_metrics,
                },
                HOLDOUT_CONFIRMATION: {
                    "status": confirmation_status,
                    "metrics": self.confirmation_metrics,
                    "cost_stress_2x_metrics": self.cost_stress_2x_metrics,
                },
            },
            "gates": {
                name: outcome.as_dict()
                for name, outcome in self.gates.items()
            },
            "hashes": dict(self.hashes),
            "caveat": self.caveat,
        }


@dataclass(frozen=True, slots=True)
class ResearchRun:
    assessment: ResearchAssessment
    development: BacktestResult | None
    confirmation: BacktestResult | None
    cost_stress_2x: BacktestResult | None

    def as_dict(self) -> dict[str, Any]:
        return self.assessment.as_dict()


def _utc_boundary(value: str | pd.Timestamp, *, name: str) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if pd.isna(timestamp):
        raise ValueError(f"{name} must be a valid timestamp")
    if timestamp.tzinfo is None:
        raise ValueError(f"{name} must include an explicit UTC offset")
    return timestamp.tz_convert("UTC")


def _config_hash(config: AppConfig) -> str:
    material = json.dumps(
        dataclasses.asdict(config),
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _finite_number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(
        value, (int, float, np.integer, np.floating)
    ):
        return None
    result = float(value)
    return result if math.isfinite(result) else None


def assess_research_metrics(
    confirmation_metrics: dict[str, Any],
    cost_stress_2x_metrics: dict[str, Any],
    *,
    holdout_calendar_days: float,
    gates: ResearchGates | None = None,
) -> tuple[str, dict[str, GateOutcome]]:
    criteria = (gates or ResearchGates()).validate()
    duration = _finite_number(holdout_calendar_days)
    duration_passed = (
        duration is not None and duration >= criteria.minimum_holdout_days
    )
    annualized_return = _finite_number(
        confirmation_metrics.get("annualized_return")
    )
    max_drawdown = _finite_number(
        confirmation_metrics.get("max_drawdown")
    )
    profit_factor = _finite_number(
        confirmation_metrics.get("profit_factor")
    )
    stress_annualized_return = _finite_number(
        cost_stress_2x_metrics.get("annualized_return")
    )
    final_halt_state = confirmation_metrics.get("final_halt_state")
    stress_final_halt_state = cost_stress_2x_metrics.get("final_halt_state")
    halt_state_evaluated = isinstance(
        final_halt_state, str
    ) and isinstance(stress_final_halt_state, str)

    outcomes = {
        "minimum_holdout_duration": GateOutcome(
            passed=duration_passed,
            evaluated=duration is not None,
            actual=duration,
            operator=">=",
            threshold=criteria.minimum_holdout_days,
        ),
        "annualized_return": GateOutcome(
            passed=(
                annualized_return is not None
                and annualized_return
                >= criteria.minimum_annualized_return
            ),
            evaluated=annualized_return is not None,
            actual=annualized_return,
            operator=">=",
            threshold=criteria.minimum_annualized_return,
        ),
        "max_drawdown": GateOutcome(
            passed=(
                max_drawdown is not None
                and max_drawdown <= criteria.maximum_drawdown
            ),
            evaluated=max_drawdown is not None,
            actual=max_drawdown,
            operator="<=",
            threshold=criteria.maximum_drawdown,
        ),
        "profit_factor": GateOutcome(
            passed=(
                profit_factor is not None
                and profit_factor >= criteria.minimum_profit_factor
            ),
            evaluated=profit_factor is not None,
            actual=profit_factor,
            operator=">=",
            threshold=criteria.minimum_profit_factor,
        ),
        "cost_stress_2x_annualized_return": GateOutcome(
            passed=(
                stress_annualized_return is not None
                and stress_annualized_return
                > criteria.minimum_cost_stress_annualized_return
            ),
            evaluated=stress_annualized_return is not None,
            actual=stress_annualized_return,
            operator=">",
            threshold=criteria.minimum_cost_stress_annualized_return,
        ),
        "final_state_not_halted": GateOutcome(
            passed=(
                halt_state_evaluated
                and str(final_halt_state).upper() != "HALTED"
                and str(stress_final_halt_state).upper() != "HALTED"
            ),
            evaluated=halt_state_evaluated,
            actual={
                "confirmation": final_halt_state,
                "cost_stress_2x": stress_final_halt_state,
            },
            operator="all !=",
            threshold="HALTED",
        ),
    }
    if not duration_passed:
        status = INSUFFICIENT_EVIDENCE
    else:
        status = PASS if all(item.passed for item in outcomes.values()) else FAIL
    return status, outcomes


def _not_evaluated_gates(
    *,
    holdout_calendar_days: float,
    gates: ResearchGates,
) -> dict[str, GateOutcome]:
    _, outcomes = assess_research_metrics(
        {},
        {},
        holdout_calendar_days=holdout_calendar_days,
        gates=gates,
    )
    return outcomes


def run_research_assessment(
    candles: pd.DataFrame,
    config: AppConfig,
    *,
    holdout_start: str | pd.Timestamp,
    holdout_end: str | pd.Timestamp,
    gates: ResearchGates | None = None,
) -> ResearchRun:
    """Evaluate one frozen candidate against one explicit historical holdout.

    Development is diagnostic only and is trained and executed on rows strictly
    before ``holdout_start``. Confirmation predictions are generated once from
    the supplied source frame. Base-cost and 2x-cost executions share that exact
    prediction object and begin from separate fresh portfolios.
    """

    config = config.validate()
    criteria = (gates or ResearchGates()).validate()
    start = _utc_boundary(holdout_start, name="holdout_start")
    end = _utc_boundary(holdout_end, name="holdout_end")
    if start >= end:
        raise ValueError("holdout_start must be before holdout_end")

    frame = validate_candles(candles)
    timestamps = pd.to_datetime(frame["timestamp"], utc=True)
    interval = pd.Timedelta(minutes=config.data.interval_minutes)
    development = frame.loc[timestamps.lt(start)].reset_index(drop=True)
    confirmation_rows = frame.loc[
        timestamps.ge(start) & timestamps.lt(end)
    ].reset_index(drop=True)
    if len(development) < 2:
        raise ValueError(
            "Development range must contain at least two candles before "
            "holdout_start"
        )
    if len(confirmation_rows) < 2:
        raise ValueError(
            "Holdout range must contain at least two candles in [start, end)"
        )
    if pd.Timestamp(confirmation_rows.iloc[-1]["timestamp"]) + interval < end:
        raise ValueError(
            "Source candles do not cover holdout_end at the configured interval"
        )

    calendar_days = float((end - start) / pd.Timedelta(days=1))
    base_hashes: dict[str, str | None] = {
        "source_data_sha256": candle_data_hash(frame),
        "development_data_sha256": candle_data_hash(development),
        "candidate_config_sha256": _config_hash(config),
        "stress_config_sha256": None,
        "prediction_alignment_sha256": None,
        "fit_manifest_sha256": None,
    }
    if calendar_days < criteria.minimum_holdout_days:
        assessment = ResearchAssessment(
            status=INSUFFICIENT_EVIDENCE,
            holdout_start=start.isoformat(),
            holdout_end_exclusive=end.isoformat(),
            holdout_calendar_days=calendar_days,
            development_metrics=None,
            confirmation_metrics=None,
            cost_stress_2x_metrics=None,
            gates=_not_evaluated_gates(
                holdout_calendar_days=calendar_days,
                gates=criteria,
            ),
            hashes=base_hashes,
        )
        return ResearchRun(
            assessment=assessment,
            development=None,
            confirmation=None,
            cost_stress_2x=None,
        )

    development_result = run_backtest(development, config)
    confirmation_predictions = generate_walk_forward_predictions(
        frame,
        interval_minutes=config.data.interval_minutes,
        model_config=config.model,
        round_trip_cost=config.round_trip_cost,
    )
    confirmation_result = run_backtest(
        frame,
        config,
        predictions=confirmation_predictions,
        execution_start=start,
        execution_end=end,
    )
    stress_config = with_scaled_costs(config, 2.0)
    cost_stress_result = run_backtest(
        frame,
        stress_config,
        predictions=confirmation_predictions,
        execution_start=start,
        execution_end=end,
    )
    if (
        confirmation_result.predictions is not confirmation_predictions
        or cost_stress_result.predictions is not confirmation_predictions
    ):
        raise RuntimeError(
            "Confirmation executions did not preserve the frozen predictions"
        )
    if (
        confirmation_result.predictions.alignment_hash
        != cost_stress_result.predictions.alignment_hash
    ):
        raise RuntimeError(
            "Cost stress must reuse the confirmation prediction manifest"
        )

    status, outcomes = assess_research_metrics(
        confirmation_result.metrics,
        cost_stress_result.metrics,
        holdout_calendar_days=calendar_days,
        gates=criteria,
    )
    base_hashes.update(
        {
            "stress_config_sha256": _config_hash(stress_config),
            "prediction_alignment_sha256": (
                confirmation_predictions.alignment_hash
            ),
            "fit_manifest_sha256": confirmation_predictions.fits_hash,
        }
    )
    assessment = ResearchAssessment(
        status=status,
        holdout_start=start.isoformat(),
        holdout_end_exclusive=end.isoformat(),
        holdout_calendar_days=calendar_days,
        development_metrics=dict(development_result.metrics),
        confirmation_metrics=dict(confirmation_result.metrics),
        cost_stress_2x_metrics=dict(cost_stress_result.metrics),
        gates=outcomes,
        hashes=base_hashes,
    )
    return ResearchRun(
        assessment=assessment,
        development=development_result,
        confirmation=confirmation_result,
        cost_stress_2x=cost_stress_result,
    )


def _json_safe(value: Any) -> Any:
    if dataclasses.is_dataclass(value):
        return _json_safe(dataclasses.asdict(value))
    if isinstance(value, dict):
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
    return value


def write_research_assessment(
    assessment: ResearchAssessment | ResearchRun,
    path: str | Path,
) -> Path:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    payload = assessment.as_dict()
    serialized = (
        json.dumps(
            _json_safe(payload),
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
    )
    temporary_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=destination.parent,
            prefix=f".{destination.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary_name = temporary.name
            temporary.write(serialized)
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(temporary_name, destination)
    finally:
        if temporary_name is not None and os.path.exists(temporary_name):
            os.unlink(temporary_name)
    return destination
