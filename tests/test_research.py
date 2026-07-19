from __future__ import annotations

import dataclasses
import json
from types import SimpleNamespace

import pandas as pd
import pytest

import coinpilot.research as research
from coinpilot.config import AppConfig, DataConfig, ModelConfig
from coinpilot.data import synthetic_candles
from coinpilot.research import (
    FAIL,
    INSUFFICIENT_EVIDENCE,
    PASS,
    ResearchAssessment,
    ResearchGates,
    assess_research_metrics,
    run_research_assessment,
    write_research_assessment,
)


def _passing_metrics() -> dict[str, object]:
    return {
        "annualized_return": 0.20,
        "max_drawdown": 0.15,
        "profit_factor": 1.15,
        "final_halt_state": "ACTIVE",
    }


def test_research_gates_use_explicit_threshold_directions() -> None:
    status, outcomes = assess_research_metrics(
        _passing_metrics(),
        {"annualized_return": 0.000001, "final_halt_state": "ACTIVE"},
        holdout_calendar_days=365.0,
    )

    assert status == PASS
    assert all(outcome.passed for outcome in outcomes.values())
    assert outcomes["annualized_return"].operator == ">="
    assert outcomes["cost_stress_2x_annualized_return"].operator == ">"

    failed_status, failed = assess_research_metrics(
        {
            **_passing_metrics(),
            "annualized_return": 0.199999,
            "max_drawdown": 0.150001,
            "profit_factor": None,
            "final_halt_state": "HALTED",
        },
        {"annualized_return": 0.0},
        holdout_calendar_days=365.0,
    )
    assert failed_status == FAIL
    assert failed["annualized_return"].passed is False
    assert failed["max_drawdown"].passed is False
    assert failed["profit_factor"].evaluated is False
    assert failed["cost_stress_2x_annualized_return"].passed is False
    assert failed["final_state_not_halted"].passed is False


def test_short_holdout_is_insufficient_and_does_not_run_models(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candles = synthetic_candles(9000, seed=101)
    end = pd.Timestamp(candles.iloc[-1]["timestamp"]) + pd.Timedelta(hours=1)
    start = end - pd.Timedelta(days=364)

    def unexpected_call(*args: object, **kwargs: object) -> object:
        raise AssertionError("short holdout must not train or execute")

    monkeypatch.setattr(research, "run_backtest", unexpected_call)
    monkeypatch.setattr(
        research, "generate_walk_forward_predictions", unexpected_call
    )
    result = run_research_assessment(
        candles,
        AppConfig(),
        holdout_start=start,
        holdout_end=end,
    )

    assert result.assessment.status == INSUFFICIENT_EVIDENCE
    assert result.development is None
    assert result.confirmation is None
    assert result.cost_stress_2x is None
    duration_gate = result.assessment.gates["minimum_holdout_duration"]
    assert duration_gate.evaluated is True
    assert duration_gate.passed is False
    assert result.assessment.hashes["prediction_alignment_sha256"] is None


def test_confirmation_boundary_fresh_run_and_prediction_reuse(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candles = synthetic_candles(9000, seed=102)
    interval = pd.Timedelta(hours=1)
    end = pd.Timestamp(candles.iloc[-1]["timestamp"]) + interval
    start = end - pd.Timedelta(days=365)
    frozen_predictions = SimpleNamespace(
        alignment_hash="frozen-prediction-hash",
        fits_hash="frozen-fit-hash",
    )
    dev_predictions = SimpleNamespace(
        alignment_hash="dev-prediction-hash",
        fits_hash="dev-fit-hash",
    )
    generation_calls: list[pd.DataFrame] = []
    execution_calls: list[tuple[pd.DataFrame, AppConfig, dict[str, object]]] = []

    def fake_generate(
        frame: pd.DataFrame, **kwargs: object
    ) -> SimpleNamespace:
        generation_calls.append(frame.copy())
        return frozen_predictions

    def fake_backtest(
        frame: pd.DataFrame,
        config: AppConfig,
        **kwargs: object,
    ) -> SimpleNamespace:
        execution_calls.append((frame.copy(), config, dict(kwargs)))
        supplied = kwargs.get("predictions")
        if supplied is None:
            return SimpleNamespace(
                metrics={"phase": "development"},
                predictions=dev_predictions,
            )
        metrics = _passing_metrics()
        if config.risk.fee_rate > AppConfig().risk.fee_rate:
            metrics = {**metrics, "annualized_return": 0.01}
        return SimpleNamespace(metrics=metrics, predictions=supplied)

    monkeypatch.setattr(
        research, "generate_walk_forward_predictions", fake_generate
    )
    monkeypatch.setattr(research, "run_backtest", fake_backtest)

    result = run_research_assessment(
        candles,
        AppConfig(),
        holdout_start=start,
        holdout_end=end,
    )

    assert len(generation_calls) == 1
    pd.testing.assert_frame_equal(generation_calls[0], candles)
    assert len(execution_calls) == 3
    development_frame, _, development_kwargs = execution_calls[0]
    assert (
        pd.to_datetime(development_frame["timestamp"], utc=True) < start
    ).all()
    assert pd.Timestamp(development_frame.iloc[-1]["timestamp"]) < start
    assert development_kwargs == {}

    for full_frame, _, kwargs in execution_calls[1:]:
        pd.testing.assert_frame_equal(full_frame, candles)
        assert kwargs["predictions"] is frozen_predictions
        assert kwargs["execution_start"] == start
        assert kwargs["execution_end"] == end
    assert execution_calls[1][1] is not execution_calls[2][1]
    assert result.confirmation is not result.cost_stress_2x
    assert result.confirmation.predictions is frozen_predictions
    assert result.cost_stress_2x.predictions is frozen_predictions
    assert result.assessment.status == PASS
    assert (
        result.assessment.hashes["prediction_alignment_sha256"]
        == "frozen-prediction-hash"
    )


def test_real_confirmation_starts_with_a_fresh_empty_account() -> None:
    candles = synthetic_candles(
        2500,
        interval_minutes=240,
        seed=103,
    )
    base = AppConfig()
    config = dataclasses.replace(
        base,
        data=DataConfig(interval_minutes=240, candle_count=2500),
        model=ModelConfig(
            horizon_bars=3,
            min_train_samples=100,
            train_window=300,
            retrain_every=240,
            max_iterations=20,
        ),
    ).validate()
    interval = pd.Timedelta(hours=4)
    end = pd.Timestamp(candles.iloc[-1]["timestamp"]) + interval
    start = end - pd.Timedelta(days=365)

    result = run_research_assessment(
        candles,
        config,
        holdout_start=start,
        holdout_end=end,
    )

    first = result.confirmation.equity_curve.iloc[0]
    assert pd.Timestamp(first["timestamp"]) == start
    assert first["cash"] == pytest.approx(config.risk.initial_cash)
    assert first["equity"] == pytest.approx(config.risk.initial_cash)
    assert first["quantity"] == pytest.approx(0.0)
    assert result.confirmation.metrics["evaluation_days"] == pytest.approx(
        365.0
    )
    assert (
        result.confirmation.predictions
        is result.cost_stress_2x.predictions
    )


def test_holdout_is_half_open_and_requires_explicit_timezone() -> None:
    candles = synthetic_candles(9000, seed=104)
    end = pd.Timestamp(candles.iloc[-1]["timestamp"]) + pd.Timedelta(hours=1)
    start = end - pd.Timedelta(days=365)

    with pytest.raises(ValueError, match="explicit UTC offset"):
        run_research_assessment(
            candles,
            AppConfig(),
            holdout_start=start.tz_localize(None),
            holdout_end=end,
        )
    with pytest.raises(ValueError, match="before holdout_end"):
        run_research_assessment(
            candles,
            AppConfig(),
            holdout_start=end,
            holdout_end=start,
        )


def test_atomic_writer_emits_strict_json(tmp_path) -> None:
    status, outcomes = assess_research_metrics(
        _passing_metrics(),
        {"annualized_return": 0.01, "final_halt_state": "ACTIVE"},
        holdout_calendar_days=365,
    )
    assessment = ResearchAssessment(
        status=status,
        holdout_start="2025-01-01T00:00:00+00:00",
        holdout_end_exclusive="2026-01-01T00:00:00+00:00",
        holdout_calendar_days=365.0,
        development_metrics={"sharpe": float("nan")},
        confirmation_metrics=_passing_metrics(),
        cost_stress_2x_metrics={
            "annualized_return": 0.01,
            "final_halt_state": "ACTIVE",
        },
        gates=outcomes,
        hashes={"prediction_alignment_sha256": "abc"},
    )
    destination = tmp_path / "nested" / "assessment.json"

    returned = write_research_assessment(assessment, destination)
    payload = json.loads(
        returned.read_text(encoding="utf-8"),
        parse_constant=lambda value: (_ for _ in ()).throw(
            ValueError(f"non-standard constant {value}")
        ),
    )

    assert payload["status"] == PASS
    assert payload["phases"]["DEV_DIAGNOSTIC"]["metrics"]["sharpe"] is None
    assert payload["objective"]["annualized_return"] == pytest.approx(0.20)
    assert "not a guarantee" in payload["caveat"]
    assert list(destination.parent.glob(f".{destination.name}.*.tmp")) == []
