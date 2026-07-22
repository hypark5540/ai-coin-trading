from __future__ import annotations

import importlib.util
import json
import sqlite3
import subprocess
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest

from coinpilot.portfolio_research import (
    PortfolioResearchGates,
    PortfolioResearchResult,
)


def _load_script() -> ModuleType:
    path = Path(__file__).parents[1] / "scripts" / "run_strategy_research_v2.py"
    spec = importlib.util.spec_from_file_location("run_strategy_research_v2", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


suite = _load_script()


def _result() -> PortfolioResearchResult:
    candidates = (
        "C0_CONTROL_ER72",
        "C1_SLOW_ER168",
        "C2_TREND_336_168",
    )
    fold_definitions = (
        ("D1", "2024-07-19T07:00:00+00:00", "2025-01-19T07:00:00+00:00"),
        ("D2", "2025-01-19T07:00:00+00:00", "2025-07-19T07:00:00+00:00"),
        (
            "D3_CONTAMINATED",
            "2025-07-19T07:00:00+00:00",
            "2026-07-19T07:00:00+00:00",
        ),
    )
    folds = tuple(
        {
            "fold_id": fold_id,
            "start": start,
            "end_exclusive": end,
            "calendar_days": float(
                (pd.Timestamp(end) - pd.Timestamp(start)) / pd.Timedelta(days=1)
            ),
        }
        for fold_id, start, end in fold_definitions
    )
    fold_results = []
    sleeve_results = []
    curves = []
    for candidate in candidates:
        for fold_id, start, end in fold_definitions:
            if candidate == "C1_SLOW_ER168":
                base_return = -0.90 if fold_id == "D3_CONTAMINATED" else 0.04
                stress_return = -0.92 if fold_id == "D3_CONTAMINATED" else 0.02
            elif candidate == "C2_TREND_336_168":
                base_return = -0.02 if fold_id == "D1" else 0.01
                stress_return = -0.03 if fold_id == "D1" else 0.005
            else:
                base_return = 0.0
                stress_return = -0.01
            row = {
                "candidate_id": candidate,
                "fold_id": fold_id,
                "market_count": 4,
                "prediction_bundle_sha256": f"bundle-{candidate}",
            }
            for prefix, value in (
                ("base", base_return),
                ("cost_stress_2x", stress_return),
            ):
                row.update(
                    {
                        f"{prefix}_total_return": value,
                        f"{prefix}_max_drawdown": 0.05,
                        f"{prefix}_trade_count": 20,
                        f"{prefix}_gross_profit": 200.0,
                        f"{prefix}_gross_loss": 100.0,
                        f"{prefix}_total_fees": 10.0,
                        f"{prefix}_total_slippage": 10.0,
                        f"{prefix}_halted_market_count": 0,
                        f"{prefix}_profit_factor": 2.0,
                    }
                )
                curves.extend(
                    (
                        {
                            "candidate_id": candidate,
                            "fold_id": fold_id,
                            "scenario": prefix,
                            "timestamp": start,
                            "equity_index": 1.0,
                        },
                        {
                            "candidate_id": candidate,
                            "fold_id": fold_id,
                            "scenario": prefix,
                            "timestamp": end,
                            "equity_index": 1.0 + value,
                        },
                    )
                )
            fold_results.append(row)
            for market_index, market in enumerate(suite.EXPECTED_MARKETS):
                market_base = base_return + (0.001 if market_index < 3 else -0.001)
                sleeve_results.append(
                    {
                        "candidate_id": candidate,
                        "fold_id": fold_id,
                        "market": market,
                        "base_total_return": market_base,
                        "cost_stress_2x_total_return": stress_return,
                        "base_trade_count": 5,
                        "cost_stress_2x_trade_count": 5,
                        "base_final_halt_state": "ACTIVE",
                        "cost_stress_2x_final_halt_state": "ACTIVE",
                        "prediction_reused_for_cost_stress": True,
                        "prediction_alignment_sha256": f"align-{candidate}-{market}",
                        "fit_manifest_sha256": f"fits-{candidate}-{market}",
                    }
                )
    return PortfolioResearchResult(
        design_hash="d" * 64,
        result_hash="r" * 64,
        source_hashes={market: market.lower() for market in suite.EXPECTED_MARKETS},
        candidate_hashes={candidate: candidate.lower() for candidate in candidates},
        folds=folds,
        candidates=tuple(),
        fold_results=tuple(fold_results),
        sleeve_results=tuple(sleeve_results),
        gate_results=tuple(),
        equity_curves=tuple(curves),
        gates=PortfolioResearchGates(),
    )


def test_protocol_uses_only_d1_d2_and_c0_is_comparison_only() -> None:
    result = _result()
    ensemble = suite.build_c3_ensemble(result)
    protocol = suite.build_protocol_summary(result, ensemble)
    candidates = {row["candidate_id"]: row for row in protocol["candidates"]}

    assert candidates["C0_CONTROL_ER72"]["development_status"] == "NOT_EVALUATED"
    assert candidates["C1_SLOW_ER168"]["development_status"] == "PASS"
    assert candidates["C1_SLOW_ER168"]["D3_CONTAMINATED"][
        "base_total_return"
    ] == pytest.approx(-0.90)
    assert candidates["C1_SLOW_ER168"]["D3_CONTAMINATED"][
        "influences_development_status"
    ] is False
    assert candidates["C2_TREND_336_168"]["development_status"] == "FAIL"
    assert [row["candidate_id"] for row in protocol["decision"]["ranking"]] == [
        "C1_SLOW_ER168"
    ]
    assert (
        protocol["decision"]["selected_development_challenger"]
        == "C1_SLOW_ER168"
    )
    assert protocol["decision"]["promotion"] == "NOT_AUTHORIZED"
    assert protocol["orders_sent"] == 0
    json.dumps(protocol, allow_nan=False)


def test_c3_is_fixed_half_and_half_normalized_equity() -> None:
    ensemble = suite.build_c3_ensemble(_result())
    d1_base = next(
        row
        for row in ensemble["fold_metrics"]
        if row["fold_id"] == "D1" and row["scenario"] == "base"
    )

    assert ensemble["within_market_component_weights"] == {
        "C1_SLOW_ER168": 0.5,
        "C2_TREND_336_168": 0.5,
    }
    assert d1_base["total_return"] == pytest.approx(0.01)
    assert d1_base["trade_count"] == 40
    assert d1_base["gross_profit"] == pytest.approx(200.0)
    assert ensemble["selection_dependent_weighting"] is False


def test_development_ranking_uses_score_then_drawdown_then_id() -> None:
    candidates = [
        {
            "candidate_id": "C3",
            "protocol_role": "CHALLENGER",
            "development_status": "PASS",
            "development_selection_score_min_d1_d2_2x_return": 0.02,
            "worst_fold_drawdown_base_or_2x": 0.04,
        },
        {
            "candidate_id": "C2",
            "protocol_role": "CHALLENGER",
            "development_status": "PASS",
            "development_selection_score_min_d1_d2_2x_return": 0.02,
            "worst_fold_drawdown_base_or_2x": 0.03,
        },
        {
            "candidate_id": "C1",
            "protocol_role": "CHALLENGER",
            "development_status": "FAIL",
            "development_selection_score_min_d1_d2_2x_return": 0.50,
            "worst_fold_drawdown_base_or_2x": 0.01,
        },
        {
            "candidate_id": "C0",
            "protocol_role": "COMPARISON_ONLY",
            "development_status": "PASS",
            "development_selection_score_min_d1_d2_2x_return": 0.80,
            "worst_fold_drawdown_base_or_2x": 0.01,
        },
    ]

    ranking = suite._development_ranking(candidates)

    assert [row["candidate_id"] for row in ranking] == ["C2", "C3"]


def _make_shadow_db(path: Path) -> None:
    with sqlite3.connect(path) as connection:
        connection.executescript(
            """
            CREATE TABLE shadow_runs (
                run_id TEXT PRIMARY KEY,
                market TEXT NOT NULL,
                started_wall_ns INTEGER NOT NULL,
                halt_reason TEXT
            );
            CREATE TABLE shadow_state (
                run_id TEXT PRIMARY KEY,
                realized_pnl_quote REAL NOT NULL,
                cumulative_fees_quote REAL NOT NULL,
                last_equity_quote REAL NOT NULL
            );
            CREATE TABLE shadow_fills (
                fill_id TEXT PRIMARY KEY,
                side TEXT NOT NULL,
                filled_base REAL NOT NULL,
                filled_quote REAL NOT NULL,
                fee_quote REAL NOT NULL,
                book_wall_ns INTEGER,
                created_wall_ns INTEGER NOT NULL
            );
            INSERT INTO shadow_runs VALUES
                ('one', 'KRW-BTC', 1, 'graceful:runtime_error'),
                ('two', 'KRW-BTC', 2, 'external_halt:daily_loss');
            INSERT INTO shadow_state VALUES ('two', -12.0, 2.0, 4988.0);
            INSERT INTO shadow_fills VALUES
                ('buy', 'buy', 1.0, 100.0, 1.0, 1000000000, 1000000000),
                ('sell', 'sell', 1.0, 90.0, 1.0, 6000000000, 6000000000);
            """
        )


def test_shadow_snapshot_pairs_fills_and_never_emits_source_path(tmp_path: Path) -> None:
    database = tmp_path / "shadow.db"
    _make_shadow_db(database)

    snapshot = suite.build_shadow_snapshot({"btc": database})

    assert snapshot["combined"]["net_pnl_quote"] == pytest.approx(-12.0)
    assert snapshot["combined"]["loss_quote"] == pytest.approx(12.0)
    assert snapshot["combined"]["fee_quote"] == pytest.approx(2.0)
    assert snapshot["combined"]["round_trip_count"] == 1
    assert snapshot["combined"]["average_hold_seconds"] == pytest.approx(5.0)
    assert snapshot["combined"]["runtime_error_count"] == 1
    assert str(tmp_path) not in json.dumps(snapshot)
    assert snapshot["orders_sent"] == 0


def test_artifact_writer_is_strict_hashed_and_replaces_symlink_never(
    tmp_path: Path,
) -> None:
    output = tmp_path / "out"
    writer = suite.ArtifactWriter(output)
    writer.add_json("one.json", {"finite": 1.0}, role="test_json")
    writer.add_csv("two.csv", pd.DataFrame([{"value": 2}]), role="test_csv")

    manifest = writer.commit(receipt={"result": "ok"})

    assert json.loads((output / "one.json").read_text())["finite"] == 1.0
    assert {row["file"] for row in manifest["files"]} == {"one.json", "two.csv"}
    for row in manifest["files"]:
        assert row["sha256"] == suite._sha256((output / row["file"]).read_bytes())
    assert (output / "manifest.json").stat().st_mode & 0o777 == 0o600
    with pytest.raises(ValueError, match="absolute path"):
        suite._canonical_json_bytes({"leak": str(tmp_path)})


def test_symlink_inputs_are_rejected_before_resolution(tmp_path: Path) -> None:
    real_output = tmp_path / "real-output"
    real_output.mkdir()
    linked_output = tmp_path / "linked-output"
    linked_output.symlink_to(real_output, target_is_directory=True)
    with pytest.raises(ValueError, match="symbolic link"):
        suite.ArtifactWriter(linked_output)

    real_configs = tmp_path / "real-configs"
    real_configs.mkdir()
    linked_configs = tmp_path / "linked-configs"
    linked_configs.symlink_to(real_configs, target_is_directory=True)
    with pytest.raises(ValueError, match="symbolic link"):
        suite.load_frozen_inputs(linked_configs)


def test_shell_launcher_selects_a_supported_python() -> None:
    launcher = Path(__file__).parents[1] / "scripts" / "run-strategy-research-v2"
    completed = subprocess.run(
        [str(launcher), "--help"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    assert "--output-dir" in completed.stdout
