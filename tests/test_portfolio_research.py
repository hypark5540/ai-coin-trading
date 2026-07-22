from __future__ import annotations

import dataclasses
import json
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

import coinpilot.portfolio_research as portfolio_research
from coinpilot.config import AppConfig, ModelConfig
from coinpilot.data import synthetic_candles
from coinpilot.portfolio_research import (
    INSUFFICIENT_EVIDENCE,
    PASS,
    CalendarFold,
    PortfolioResearchGates,
    run_portfolio_research,
)


def _candidate(
    *,
    horizon_bars: int = 24,
    signal_mode: str = "expected_return",
) -> AppConfig:
    base = AppConfig()
    return dataclasses.replace(
        base,
        model=ModelConfig(
            signal_mode=signal_mode,
            horizon_bars=horizon_bars,
            min_train_samples=100,
            train_window=400,
            retrain_every=24,
            max_iterations=20,
            calibration_window=120,
            calibration_min_samples=30,
        ),
    ).validate()


def _folds() -> tuple[CalendarFold, ...]:
    return (
        CalendarFold(
            "fold-1",
            "2025-12-01T00:00:00Z",
            "2025-12-11T00:00:00Z",
        ),
        CalendarFold(
            "fold-2",
            "2025-12-11T00:00:00Z",
            "2025-12-21T00:00:00Z",
        ),
        CalendarFold(
            "fold-3",
            "2025-12-21T00:00:00Z",
            "2025-12-31T00:00:00Z",
        ),
    )


def _install_fake_engine(
    monkeypatch: pytest.MonkeyPatch,
    *,
    base_returns_by_market: dict[str, float] | None = None,
    trade_pnl: list[float] | None = None,
) -> tuple[list[SimpleNamespace], list[dict[str, object]]]:
    generated: list[SimpleNamespace] = []
    executions: list[dict[str, object]] = []

    def fake_generate(
        frame: pd.DataFrame,
        *,
        interval_minutes: int,
        model_config: ModelConfig,
        round_trip_cost: float,
    ) -> SimpleNamespace:
        market = str(frame.iloc[0]["market"])
        token = (
            f"{market}-{model_config.signal_mode}-"
            f"h{model_config.horizon_bars}"
        )
        prediction = SimpleNamespace(
            alignment_hash=f"alignment-{token}",
            fits_hash=f"fits-{token}",
            model_config_hash=f"model-{model_config.horizon_bars}",
        )
        generated.append(prediction)
        return prediction

    def fake_backtest(
        frame: pd.DataFrame,
        config: AppConfig,
        *,
        predictions: SimpleNamespace,
        execution_start: pd.Timestamp,
        execution_end: pd.Timestamp,
    ) -> SimpleNamespace:
        timestamps = pd.to_datetime(frame["timestamp"], utc=True)
        selected = frame.loc[
            timestamps.ge(execution_start) & timestamps.lt(execution_end)
        ]
        stressed = config.risk.fee_rate > AppConfig().risk.fee_rate
        market = str(frame.iloc[0]["market"])
        base_return = (base_returns_by_market or {}).get(market, 0.01)
        total_return = base_return - 0.005 if stressed else base_return
        equity = np.linspace(
            config.risk.initial_cash,
            config.risk.initial_cash * (1 + total_return),
            len(selected),
        )
        executions.append(
            {
                "market": market,
                "predictions": predictions,
                "stressed": stressed,
                "start": execution_start,
                "end": execution_end,
            }
        )
        pnl_values = trade_pnl or [10_000.0, -2_000.0]
        return SimpleNamespace(
            predictions=predictions,
            metrics={
                "initial_cash": config.risk.initial_cash,
                "total_return": total_return,
                "annualized_return": 0.4 if not stressed else 0.2,
                "max_drawdown": 0.01,
                "trade_count": len(pnl_values),
                "profit_factor": 5.0,
                "total_fees": 1_000.0 if not stressed else 2_000.0,
                "total_slippage": 1_000.0 if not stressed else 2_000.0,
                "eligible_signal_count": 12,
                "final_halt_state": "ACTIVE",
            },
            equity_curve=pd.DataFrame(
                {
                    "timestamp": selected["timestamp"].reset_index(drop=True),
                    "equity": equity,
                }
            ),
            trades=pd.DataFrame({"pnl": pnl_values}),
        )

    monkeypatch.setattr(
        portfolio_research,
        "generate_walk_forward_predictions",
        fake_generate,
    )
    monkeypatch.setattr(portfolio_research, "run_backtest", fake_backtest)
    return generated, executions


def test_frozen_predictions_are_reused_and_results_do_not_select_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generated, executions = _install_fake_engine(monkeypatch)
    sources = {
        "KRW-BTC": synthetic_candles(1000, market="KRW-BTC", seed=201),
        "KRW-ETH": synthetic_candles(1000, market="KRW-ETH", seed=202),
    }
    candidates = {
        "horizon-72": _candidate(horizon_bars=72),
        "horizon-24": _candidate(horizon_bars=24),
        "trend": _candidate(signal_mode="trend_breakout"),
    }
    gates = PortfolioResearchGates(
        minimum_folds=3,
        minimum_markets_per_fold=2,
        minimum_total_trades=1,
        minimum_nonnegative_market_count=2,
    )

    result = run_portfolio_research(
        sources,
        candidates,
        _folds(),
        gates=gates,
    )

    assert len(generated) == 6
    assert len(executions) == 36
    for prediction in generated:
        uses = [
            call for call in executions if call["predictions"] is prediction
        ]
        assert len(uses) == 6
        assert sum(bool(call["stressed"]) for call in uses) == 3
    payload = result.as_dict()
    assert payload["decision"]["candidate_selection"] == "NOT_PERFORMED"
    assert payload["decision"]["promotion"] == "NOT_AUTHORIZED"
    assert "best" not in payload
    assert [row["candidate_id"] for row in result.candidates] == [
        "horizon-24",
        "horizon-72",
        "trend",
    ]
    assert all(row["gate_status"] == PASS for row in result.candidates)
    assert all(
        row["prediction_reused_for_cost_stress"]
        for row in result.sleeve_results
    )
    assert all(row["market_count"] == 2 for row in result.fold_results)
    for candidate in result.candidates:
        assert candidate["base_profit_factor"] == pytest.approx(5.0)
        assert candidate["nonnegative_market_count"] == 2
    protocol_gates = {
        row["gate"]: row
        for row in result.gate_results
        if row["candidate_id"] == "horizon-24"
    }
    assert protocol_gates["minimum_profit_factor"]["passed"] is True
    assert (
        protocol_gates["minimum_nonnegative_market_count"]["passed"]
        is True
    )


def test_result_is_reproducible_strict_json_and_flat_csv(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_engine(monkeypatch)
    sources = {
        "KRW-BTC": synthetic_candles(1000, market="KRW-BTC", seed=203),
        "KRW-ETH": synthetic_candles(1000, market="KRW-ETH", seed=204),
    }
    arguments = (
        sources,
        {"horizon-24": _candidate(horizon_bars=24)},
        _folds(),
    )
    gates = PortfolioResearchGates(
        minimum_folds=3,
        minimum_markets_per_fold=2,
        minimum_total_trades=1,
        minimum_nonnegative_market_count=2,
    )

    first = run_portfolio_research(*arguments, gates=gates)
    second = run_portfolio_research(*arguments, gates=gates)

    assert first.design_hash == second.design_hash
    assert first.result_hash == second.result_hash
    json.dumps(first.as_dict(), allow_nan=False)
    assert len(first.result_hash) == 64
    assert set(first.source_hashes) == {"KRW-BTC", "KRW-ETH"}
    for frame in first.csv_frames().values():
        for value in frame.to_numpy().ravel():
            assert not isinstance(value, (dict, list, tuple, pd.Timestamp))


def test_candidate_hash_ignores_machine_local_storage_paths() -> None:
    first = _candidate()
    second = dataclasses.replace(
        first,
        data=dataclasses.replace(first.data, database_path="/tmp/other.db"),
        output=dataclasses.replace(
            first.output,
            artifacts_dir="/tmp/other-artifacts",
        ),
        shadow=dataclasses.replace(
            first.shadow,
            database_path="/tmp/shadow.db",
            archive_root="/tmp/archive",
        ),
        operations=dataclasses.replace(
            first.operations,
            backup_dir="/tmp/backups",
        ),
    ).validate()

    assert portfolio_research._candidate_policy_hash(first) == (
        portfolio_research._candidate_policy_hash(second)
    )
    assert portfolio_research._execution_config_hash(first) == (
        portfolio_research._execution_config_hash(second)
    )


def test_candidate_policy_hash_ignores_template_market_and_unused_fields() -> None:
    first = _candidate()
    second = dataclasses.replace(
        first,
        data=dataclasses.replace(first.data, market="KRW-ETH"),
        model=dataclasses.replace(
            first.model,
            breakout_entry_window=720,
            breakout_exit_window=360,
        ),
    ).validate()

    assert portfolio_research._candidate_policy_hash(first) == (
        portfolio_research._candidate_policy_hash(second)
    )
    assert portfolio_research._execution_config_hash(first) != (
        portfolio_research._execution_config_hash(second)
    )

    trend = dataclasses.replace(
        first,
        model=dataclasses.replace(
            first.model,
            signal_mode="trend_breakout",
            horizon_bars=336,
        ),
    ).validate()
    changed_unused = dataclasses.replace(
        trend,
        model=dataclasses.replace(
            trend.model,
            min_train_samples=700,
            train_window=2100,
            retrain_every=168,
            l2=0.75,
            calibration_window=480,
            calibration_min_samples=240,
            regime_sma_window=336,
        ),
    ).validate()
    assert portfolio_research._candidate_policy_hash(trend) == (
        portfolio_research._candidate_policy_hash(changed_unused)
    )


def test_missing_market_fold_is_reported_as_insufficient_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_engine(monkeypatch)
    btc = synthetic_candles(1000, market="KRW-BTC", seed=205)
    eth = synthetic_candles(1000, market="KRW-ETH", seed=206)
    eth = eth.loc[
        pd.to_datetime(eth["timestamp"], utc=True)
        >= pd.Timestamp("2025-12-10T23:00:00Z")
    ].reset_index(drop=True)
    fold = CalendarFold(
        "early",
        "2025-12-01T00:00:00Z",
        "2025-12-11T00:00:00Z",
    )

    result = run_portfolio_research(
        {"KRW-BTC": btc, "KRW-ETH": eth},
        {"horizon-24": _candidate()},
        (fold,),
        gates=PortfolioResearchGates(
            minimum_folds=1,
            minimum_markets_per_fold=2,
            minimum_total_trades=1,
            minimum_nonnegative_market_count=1,
        ),
    )

    assert result.fold_results[0]["gate_status"] == INSUFFICIENT_EVIDENCE
    assert result.fold_results[0]["market_count"] == 1
    assert result.fold_results[0]["missing_markets"] == "KRW-ETH"
    assert result.candidates[0]["gate_status"] == INSUFFICIENT_EVIDENCE
    breadth = next(
        row
        for row in result.gate_results
        if row["gate"] == "all_folds_have_market_breadth"
    )
    assert breadth["passed"] is False


def test_protocol_profit_factor_and_market_breadth_can_fail_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_engine(
        monkeypatch,
        base_returns_by_market={
            "KRW-BTC": 0.01,
            "KRW-ETH": -0.02,
            "KRW-XRP": 0.0,
        },
        trade_pnl=[1_000.0, -1_000.0],
    )
    sources = {
        market: synthetic_candles(1000, market=market, seed=seed)
        for market, seed in (
            ("KRW-BTC", 208),
            ("KRW-ETH", 209),
            ("KRW-XRP", 210),
        )
    }

    result = run_portfolio_research(
        sources,
        {"horizon-24": _candidate()},
        (_folds()[0],),
        gates=PortfolioResearchGates(
            minimum_folds=1,
            minimum_markets_per_fold=3,
            minimum_total_trades=1,
            minimum_nonnegative_market_count=3,
            minimum_annualized_return=-1.0,
            minimum_cost_stress_annualized_return=-1.0,
            minimum_positive_fold_share=0.0,
        ),
    )

    candidate = result.candidates[0]
    assert candidate["gate_status"] == "FAIL"
    assert candidate["base_profit_factor"] == pytest.approx(1.0)
    assert candidate["nonnegative_market_count"] == 2
    gates = {row["gate"]: row for row in result.gate_results}
    assert gates["minimum_profit_factor"]["passed"] is False
    assert gates["minimum_nonnegative_market_count"]["passed"] is False


def test_rejects_non_expected_return_and_overlapping_folds() -> None:
    sources = {
        "KRW-BTC": synthetic_candles(1000, market="KRW-BTC", seed=207),
    }
    with pytest.raises(ValueError, match="expected_return"):
        run_portfolio_research(
            sources,
            {"probability": AppConfig()},
            (_folds()[0],),
            gates=PortfolioResearchGates(
                minimum_folds=1,
                minimum_markets_per_fold=1,
                minimum_total_trades=1,
                minimum_nonnegative_market_count=1,
            ),
        )

    overlapping = (
        CalendarFold(
            "one",
            "2025-12-01T00:00:00Z",
            "2025-12-11T00:00:00Z",
        ),
        CalendarFold(
            "two",
            "2025-12-10T00:00:00Z",
            "2025-12-20T00:00:00Z",
        ),
    )
    with pytest.raises(ValueError, match="overlap"):
        run_portfolio_research(
            sources,
            {"expected": _candidate()},
            overlapping,
            gates=PortfolioResearchGates(
                minimum_folds=1,
                minimum_markets_per_fold=1,
                minimum_total_trades=1,
                minimum_nonnegative_market_count=1,
            ),
        )
