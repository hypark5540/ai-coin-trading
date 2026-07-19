from __future__ import annotations

import csv
import json

from coinpilot.hft_data import normalize_upbit_message
from coinpilot.hft_research import (
    run_hft_scenario_suite,
    write_capture_companions,
    write_hft_study_artifacts,
)
from coinpilot.hft_sim import HFTSimulationConfig


def test_scenario_suite_reuses_weak_events_and_cost_decisions() -> None:
    study = run_hft_scenario_suite(
        event_count=800,
        seed=11,
        base_config=HFTSimulationConfig(
            initial_cash=1_000_000.0,
            order_notional=25_000.0,
            equity_sample_points=31,
        ),
    )
    scenarios = {scenario.name: scenario for scenario in study.scenarios}
    weak = scenarios["weak_alpha_base"]
    stressed = scenarios["weak_alpha_cost_2x"]

    assert study.as_dict()["source_kind"] == "synthetic"
    assert weak.event_sha256 == stressed.event_sha256
    assert weak.result.decisions == stressed.result.decisions
    assert stressed.result.metrics["total_fees"] > weak.result.metrics["total_fees"]
    assert stressed.result.metrics["net_pnl"] < weak.result.metrics["net_pnl"]
    assert all(
        scenario.result.metrics["is_synthetic"] is True
        for scenario in study.scenarios
    )


def test_study_artifacts_mark_every_execution_as_simulated(tmp_path) -> None:
    study = run_hft_scenario_suite(
        event_count=500,
        seed=3,
        base_config=HFTSimulationConfig(
            initial_cash=1_000_000.0,
            order_notional=25_000.0,
            equity_sample_points=21,
        ),
    )
    paths = write_hft_study_artifacts(study, tmp_path)

    summary = json.loads(paths["summary"].read_text(encoding="utf-8"))
    assert summary["source_kind"] == "synthetic"
    assert len(summary["scenarios"]) == 5
    with paths["simulated_executions"].open(
        "r",
        encoding="utf-8",
        newline="",
    ) as handle:
        rows = list(csv.DictReader(handle))
    assert rows
    assert {row["simulated"] for row in rows} == {"True"}
    assert {row["source_kind"] for row in rows} == {"synthetic"}


def test_capture_companions_never_label_public_trades_as_own_fills(
    tmp_path,
) -> None:
    event = normalize_upbit_message(
        {
            "type": "trade",
            "code": "KRW-BTC",
            "trade_timestamp": 1_700_000_000_000,
            "sequential_id": 1_700_000_000_000_000,
            "trade_price": 100_000_000,
            "trade_volume": 0.001,
            "ask_bid": "BID",
            "best_ask_price": 100_001_000,
            "best_ask_size": 1.0,
            "best_bid_price": 100_000_000,
            "best_bid_size": 2.0,
        },
        received_at_ns=1_700_000_000_100_000_000,
    )
    capture = tmp_path / "capture.jsonl"
    capture.write_text(
        json.dumps(event, separators=(",", ":"), sort_keys=True) + "\n",
        encoding="utf-8",
    )

    paths = write_capture_companions(capture)
    quality = json.loads(paths["quality"].read_text(encoding="utf-8"))
    assert quality["source_kind"] == "captured_public_feed"
    with paths["public_trades"].open(
        "r",
        encoding="utf-8",
        newline="",
    ) as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["observed_public_trade"] == "True"
    assert rows[0]["own_execution"] == "False"
