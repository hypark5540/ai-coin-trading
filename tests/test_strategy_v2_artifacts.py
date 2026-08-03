from __future__ import annotations

import csv
import importlib.util
import json
from pathlib import Path
from types import ModuleType

import pytest


def _load_script(name: str) -> ModuleType:
    path = Path(__file__).parents[1] / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


report = _load_script("build_strategy_v2_report")
notebook = _load_script("build_strategy_v2_notebook")


def _write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")


def _suite_fixture(path: Path) -> Path:
    path.mkdir()
    folds = {
        "C0_CONTROL_ER72": {
            "D1": (0.00, -0.01),
            "D2": (-0.01, -0.02),
            "D3_CONTAMINATED": (-0.02, -0.03),
        },
        "C1_SLOW_ER168": {
            "D1": (0.01, 0.005),
            "D2": (-0.01, -0.02),
            "D3_CONTAMINATED": (0.005, -0.001),
        },
        "C2_TREND_336_168": {
            "D1": (0.03, 0.02),
            "D2": (0.04, 0.03),
            "D3_CONTAMINATED": (-0.04, -0.05),
        },
        "C3_ENSEMBLE_50_50": {
            "D1": (0.02, 0.01),
            "D2": (0.015, 0.008),
            "D3_CONTAMINATED": (-0.02, -0.03),
        },
    }

    candidates = [
        {
            "candidate_id": "C0_CONTROL_ER72",
            "protocol_role": "COMPARISON_ONLY",
            "development_status": "NOT_EVALUATED",
            "promotion_status": "NOT_ELIGIBLE",
            "fold_metrics": {
                fold_id: {
                    "base_total_return": values[0],
                    "cost_stress_2x_total_return": values[1],
                }
                for fold_id, values in folds["C0_CONTROL_ER72"].items()
            },
        }
    ]
    for candidate_id, passed in (
        ("C1_SLOW_ER168", False),
        ("C2_TREND_336_168", True),
        ("C3_ENSEMBLE_50_50", True),
    ):
        d1 = folds[candidate_id]["D1"]
        d2 = folds[candidate_id]["D2"]
        d3 = folds[candidate_id]["D3_CONTAMINATED"]
        candidates.append(
            {
                "candidate_id": candidate_id,
                "protocol_role": "CHALLENGER",
                "development_status": "PASS" if passed else "FAIL",
                "promotion_status": "NOT_AUTHORIZED",
                "complete_four_market_evidence": True,
                "portfolio_round_trip_count": 60,
                "base_compounded_return": (1 + d1[0]) * (1 + d2[0]) - 1,
                "cost_stress_2x_compounded_return": (1 + d1[1]) * (1 + d2[1]) - 1,
                "combined_base_profit_factor": 1.5 if passed else 0.8,
                "worst_fold_drawdown_base_or_2x": 0.04,
                "nonnegative_market_count": 4 if passed else 2,
                "gates": [{"gate": "fixture", "passed": passed}],
                "D3_CONTAMINATED": {
                    "influences_development_status": False,
                    "base_total_return": d3[0],
                    "cost_stress_2x_total_return": d3[1],
                },
            }
        )

    protocol = {
        "protocol": "STRATEGY_RESEARCH_V2",
        "development_fold_ids": ["D1", "D2"],
        "contaminated_diagnostic_fold_id": "D3_CONTAMINATED",
        "contaminated_fold_influences_any_status": False,
        "thresholds": {"minimum_total_trades": 30},
        "decision": {
            "candidate_selection": "DEVELOPMENT_CHALLENGER_SELECTED",
            "ranking": [
                {"rank": 1, "candidate_id": "C2_TREND_336_168"},
                {"rank": 2, "candidate_id": "C3_ENSEMBLE_50_50"},
            ],
            "selected_development_challenger": "C2_TREND_336_168",
            "promotion": "NOT_AUTHORIZED",
            "active_champion": "CASH_OBSERVE_ONLY",
        },
        "candidates": candidates,
        "orders_sent": 0,
        "live_order_routing": False,
    }
    _write_json(path / "protocol_summary.json", protocol)

    shadow = {
        "ledgers": [
            {
                "market": "KRW-BTC",
                "net_pnl_quote": -60.0,
                "loss_quote": 60.0,
                "fee_quote": 40.0,
                "round_trip_count": 3,
                "average_hold_seconds": 10.0,
                "runtime_error_count": 2,
                "fill_to_state_reconciliation_delta_quote": 0.0,
            },
            {
                "market": "KRW-ETH",
                "net_pnl_quote": -40.0,
                "loss_quote": 40.0,
                "fee_quote": 30.0,
                "round_trip_count": 2,
                "average_hold_seconds": 20.0,
                "runtime_error_count": 0,
                "fill_to_state_reconciliation_delta_quote": 0.0,
            },
        ],
        "combined": {
            "net_pnl_quote": -100.0,
            "loss_quote": 100.0,
            "fee_quote": 70.0,
            "round_trip_count": 5,
        },
        "orders_sent": 0,
        "live_order_routing": False,
    }
    _write_json(path / "shadow_snapshot.json", shadow)

    quality_rows = [
        {
            "market": market,
            "row_count": 10_000,
            "gap_count": 1,
            "estimated_missing_interval_count": 1,
            "maximum_gap_minutes": 120.0,
            "invalid_total_count": 0,
            "D1_complete": True,
            "D2_complete": True,
            "D3_CONTAMINATED_complete": True,
        }
        for market in ("KRW-BTC", "KRW-ETH", "KRW-XRP", "KRW-SOL")
    ]
    _write_json(
        path / "data_quality.json",
        {
            "markets": quality_rows,
            "total_invalid_count": 0,
            "all_protocol_windows_complete": True,
        },
    )
    with (path / "data_quality.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(quality_rows[0]))
        writer.writeheader()
        writer.writerows(quality_rows)

    raw_rows = []
    for candidate_id in report.CANDIDATE_ORDER[:3]:
        for fold_id in report.FOLD_ORDER:
            values = folds[candidate_id][fold_id]
            raw_rows.append(
                {
                    "candidate_id": candidate_id,
                    "fold_id": fold_id,
                    "base_total_return": values[0],
                    "cost_stress_2x_total_return": values[1],
                }
            )
    with (path / "suite_folds.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(raw_rows[0]))
        writer.writeheader()
        writer.writerows(raw_rows)

    ensemble_rows = []
    for fold_id in report.FOLD_ORDER:
        for scenario, index in (("base", 0), ("cost_stress_2x", 1)):
            ensemble_rows.append(
                {
                    "candidate_id": "C3_ENSEMBLE_50_50",
                    "fold_id": fold_id,
                    "scenario": scenario,
                    "total_return": folds["C3_ENSEMBLE_50_50"][fold_id][index],
                }
            )
    with (path / "ensemble_c3_folds.csv").open(
        "w", encoding="utf-8", newline=""
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=list(ensemble_rows[0]))
        writer.writeheader()
        writer.writerows(ensemble_rows)
    return path


def test_report_artifact_is_canonical_portable_and_reconciled(tmp_path: Path) -> None:
    suite_dir = _suite_fixture(tmp_path / "suite")

    artifact = report.build_artifact(suite_dir)

    assert artifact["surface"] == "report"
    assert artifact["manifest"]["blocks"][0]["body"].startswith(
        "# 전략 자가검수"
    )
    assert artifact["manifest"]["blocks"][1]["body"].startswith(
        "## Executive Summary"
    )
    assert {chart["id"] for chart in artifact["manifest"]["charts"]} == {
        "shadow_loss_components",
        "candidate_fold_returns",
    }
    assert len(artifact["snapshot"]["datasets"]["candidate_fold_returns"]) == 24
    assert len(artifact["snapshot"]["datasets"]["candidate_summary"]) == 4
    assert artifact["snapshot"]["datasets"]["headline_strategy"][0][
        "selected_challenger"
    ].startswith("C2")
    assert all(not source["path"].startswith("/") for source in artifact["sources"])
    assert all(
        source["query"]["sql"].lstrip().upper().startswith(("SELECT", "WITH"))
        for source in artifact["sources"]
    )
    json.dumps(artifact, ensure_ascii=False, allow_nan=False)

    destination = report.write_artifact(suite_dir)
    assert destination.name == "artifact.json"
    assert destination.stat().st_mode & 0o777 == 0o600


def test_report_rejects_d3_selection_contamination(tmp_path: Path) -> None:
    suite_dir = _suite_fixture(tmp_path / "suite")
    protocol_path = suite_dir / "protocol_summary.json"
    protocol = json.loads(protocol_path.read_text(encoding="utf-8"))
    protocol["contaminated_fold_influences_any_status"] = True
    _write_json(protocol_path, protocol)

    with pytest.raises(ValueError, match="contaminated fold"):
        report.build_artifact(suite_dir)


def test_notebook_payload_has_exact_sections_and_audits() -> None:
    payload = notebook.build_notebook_payload()
    headings = [
        cell["source"].splitlines()[0].lstrip("# ")
        for cell in payload["cells"]
        if cell["cell_type"] == "markdown"
    ]

    assert headings == list(notebook.SECTION_HEADINGS)
    assert len({cell["id"] for cell in payload["cells"]}) == len(payload["cells"])
    code = "\n".join(
        cell["source"]
        for cell in payload["cells"]
        if cell["cell_type"] == "code"
    )
    assert 'protocol["development_fold_ids"] == ["D1", "D2"]' in code
    assert 'influences_development_status"] is False' in code
    assert 'combined["fee_quote"]' in code
    assert 'protocol["orders_sent"] == 0' in code
    notebook._assert_safe_notebook(payload)


def test_notebook_safety_rejects_machine_local_output() -> None:
    with pytest.raises(ValueError, match="Machine-local path"):
        notebook._assert_safe_notebook({"output": "/Users/example/secret.txt"})
