#!/usr/bin/env python3
"""Build the canonical portable-report artifact for Strategy Research V2.

The script consumes only the strict JSON/CSV outputs created by
``run_strategy_research_v2.py``.  It does not read exchange credentials,
modify a service, or route an order.  The resulting ``artifact.json`` is the
single canonical input for the Data Analytics portable HTML deliverer.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


CANDIDATE_ORDER = (
    "C0_CONTROL_ER72",
    "C1_SLOW_ER168",
    "C2_TREND_336_168",
    "C3_ENSEMBLE_50_50",
)
CHALLENGERS = CANDIDATE_ORDER[1:]
FOLD_ORDER = ("D1", "D2", "D3_CONTAMINATED")
SCENARIO_ORDER = ("base", "cost_stress_2x")

CANDIDATE_LABELS = {
    "C0_CONTROL_ER72": "C0 · 기존 ER72",
    "C1_SLOW_ER168": "C1 · 느린 ER168",
    "C2_TREND_336_168": "C2 · 추세 돌파",
    "C3_ENSEMBLE_50_50": "C3 · 50/50 앙상블",
}
SHORT_CANDIDATE_LABELS = {
    candidate_id: candidate_id.split("_", 1)[0]
    for candidate_id in CANDIDATE_ORDER
}
FOLD_LABELS = {
    "D1": "D1 (개발)",
    "D2": "D2 (개발)",
    "D3_CONTAMINATED": "D3* (오염·진단 전용)",
}
SCENARIO_LABELS = {
    "base": "기본 비용",
    "cost_stress_2x": "비용 2배",
}
REQUIRED_FILES = (
    "protocol_summary.json",
    "shadow_snapshot.json",
    "data_quality.json",
    "suite_folds.csv",
    "ensemble_c3_folds.csv",
)

_WINDOWS_ABSOLUTE_PATH = re.compile(r"^[A-Za-z]:[\\/]")
_LOCAL_PATH_FRAGMENT = re.compile(
    r"(?:^|[\s'\"])/(?:Users|home|private/var/folders|tmp)/"
)
_SECRET_FRAGMENT = re.compile(
    r"hooks\.slack\.com|(?:api|access|secret)[_-]?key\s*[:=]|bearer\s+[A-Za-z0-9._~-]+",
    re.IGNORECASE,
)


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise ValueError(f"Required suite file is missing: {path.name}") from error
    if not isinstance(payload, dict):
        raise ValueError(f"{path.name} must contain one JSON object")
    return payload


def _load_csv(path: Path) -> list[dict[str, str]]:
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            return list(csv.DictReader(handle))
    except FileNotFoundError as error:
        raise ValueError(f"Required suite file is missing: {path.name}") from error


def _number(value: Any, *, name: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{name} must be numeric") from error
    if not math.isfinite(result):
        raise ValueError(f"{name} must be finite")
    return result


def _optional_number(value: Any) -> float | None:
    if value in (None, "", "None", "null"):
        return None
    result = float(value)
    return result if math.isfinite(result) else None


def _compound(values: Sequence[float]) -> float:
    result = 1.0
    for value in values:
        result *= 1.0 + float(value)
    return result - 1.0


def _percent(value: float | None, digits: int = 4) -> str:
    if value is None:
        return "—"
    return f"{value:+.{digits}%}"


def _won(value: float) -> str:
    return f"₩{value:,.2f}"


def _assert_close(actual: float, expected: float, *, name: str) -> None:
    if not math.isclose(actual, expected, rel_tol=1e-9, abs_tol=1e-7):
        raise ValueError(
            f"{name} does not reconcile: actual={actual!r}, expected={expected!r}"
        )


def _assert_no_orders(payload: Mapping[str, Any], *, name: str) -> None:
    if int(payload.get("orders_sent", 0)) != 0:
        raise ValueError(f"{name} reports non-zero orders_sent")
    if payload.get("live_order_routing") not in (None, False):
        raise ValueError(f"{name} reports live order routing")


def _assert_portable(value: Any, *, location: str = "$") -> None:
    """Reject machine-local paths and obvious credentials from the artifact."""

    if isinstance(value, Mapping):
        for key, item in value.items():
            _assert_portable(item, location=f"{location}.{key}")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _assert_portable(item, location=f"{location}[{index}]")
        return
    if isinstance(value, str):
        if (
            value.startswith(("/", "~/", "file://"))
            or _WINDOWS_ABSOLUTE_PATH.match(value)
            or _LOCAL_PATH_FRAGMENT.search(value)
        ):
            raise ValueError(f"Machine-local path found at {location}")
        if _SECRET_FRAGMENT.search(value):
            raise ValueError(f"Credential-like text found at {location}")
        return
    if value is None or isinstance(value, (bool, int, float)):
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError(f"Non-finite value found at {location}")
        return
    raise TypeError(f"Unsupported artifact value at {location}: {type(value).__name__}")


def _raw_fold_returns(rows: Sequence[Mapping[str, str]]) -> dict[tuple[str, str, str], float]:
    output: dict[tuple[str, str, str], float] = {}
    for row in rows:
        candidate_id = str(row.get("candidate_id", ""))
        fold_id = str(row.get("fold_id", ""))
        if candidate_id not in CANDIDATE_ORDER[:3] or fold_id not in FOLD_ORDER:
            continue
        for scenario in SCENARIO_ORDER:
            column = f"{scenario}_total_return"
            key = (candidate_id, fold_id, scenario)
            if key in output:
                raise ValueError(f"Duplicate raw fold return: {key}")
            output[key] = _number(row.get(column), name=f"{key}.{column}")
    return output


def _ensemble_fold_returns(
    rows: Sequence[Mapping[str, str]],
) -> dict[tuple[str, str, str], float]:
    output: dict[tuple[str, str, str], float] = {}
    for row in rows:
        candidate_id = str(row.get("candidate_id", "C3_ENSEMBLE_50_50"))
        fold_id = str(row.get("fold_id", ""))
        scenario = str(row.get("scenario", ""))
        if candidate_id != "C3_ENSEMBLE_50_50" or fold_id not in FOLD_ORDER:
            continue
        if scenario not in SCENARIO_ORDER:
            continue
        key = (candidate_id, fold_id, scenario)
        if key in output:
            raise ValueError(f"Duplicate ensemble fold return: {key}")
        output[key] = _number(row.get("total_return"), name=f"{key}.total_return")
    return output


def load_review_inputs(input_dir: Path) -> dict[str, Any]:
    """Load, reconcile, and normalize one Strategy Research V2 output set."""

    directory = input_dir.expanduser().resolve()
    if not directory.is_dir() or directory.is_symlink():
        raise ValueError("Suite input must be a regular directory")
    for name in REQUIRED_FILES:
        path = directory / name
        if not path.is_file() or path.is_symlink():
            raise ValueError(f"Required suite file is missing or unsafe: {name}")

    protocol = _load_json(directory / "protocol_summary.json")
    shadow = _load_json(directory / "shadow_snapshot.json")
    quality = _load_json(directory / "data_quality.json")
    _assert_no_orders(protocol, name="protocol_summary.json")
    _assert_no_orders(shadow, name="shadow_snapshot.json")

    if protocol.get("protocol") != "STRATEGY_RESEARCH_V2":
        raise ValueError("Unexpected research protocol")
    if protocol.get("development_fold_ids") != ["D1", "D2"]:
        raise ValueError("Development status must be based on D1 and D2 only")
    if protocol.get("contaminated_diagnostic_fold_id") != "D3_CONTAMINATED":
        raise ValueError("D3_CONTAMINATED must remain the diagnostic-only fold")
    if protocol.get("contaminated_fold_influences_any_status") is not False:
        raise ValueError("The contaminated fold must not influence any status")
    decision = protocol.get("decision")
    if not isinstance(decision, Mapping):
        raise ValueError("Protocol decision is missing")
    if decision.get("promotion") != "NOT_AUTHORIZED":
        raise ValueError("The frozen protocol must not authorize promotion")
    if decision.get("active_champion") != "CASH_OBSERVE_ONLY":
        raise ValueError("The active champion must remain CASH_OBSERVE_ONLY")

    candidates = protocol.get("candidates")
    if not isinstance(candidates, list):
        raise ValueError("Protocol candidate rows are missing")
    by_candidate = {
        str(row.get("candidate_id")): row
        for row in candidates
        if isinstance(row, Mapping)
    }
    if set(by_candidate) != set(CANDIDATE_ORDER):
        raise ValueError("Protocol must contain exactly C0, C1, C2, and C3")
    for candidate_id in CHALLENGERS:
        row = by_candidate[candidate_id]
        d3 = row.get("D3_CONTAMINATED")
        if not isinstance(d3, Mapping) or d3.get(
            "influences_development_status"
        ) is not False:
            raise ValueError(f"{candidate_id} improperly uses D3 for development")
        gates = row.get("gates")
        if not isinstance(gates, list) or not gates:
            raise ValueError(f"{candidate_id} has no auditable gate rows")
        expected_status = (
            "INSUFFICIENT_EVIDENCE"
            if not bool(row.get("complete_four_market_evidence"))
            or int(row.get("portfolio_round_trip_count", 0))
            < int(protocol["thresholds"]["minimum_total_trades"])
            else "PASS"
            if all(bool(gate.get("passed")) for gate in gates)
            else "FAIL"
        )
        if row.get("development_status") != expected_status:
            raise ValueError(f"{candidate_id} development status does not match its gates")

    returns = _raw_fold_returns(_load_csv(directory / "suite_folds.csv"))
    returns.update(
        _ensemble_fold_returns(_load_csv(directory / "ensemble_c3_folds.csv"))
    )
    expected_keys = {
        (candidate_id, fold_id, scenario)
        for candidate_id in CANDIDATE_ORDER
        for fold_id in FOLD_ORDER
        for scenario in SCENARIO_ORDER
    }
    missing = expected_keys.difference(returns)
    extra = set(returns).difference(expected_keys)
    if missing or extra:
        raise ValueError(
            f"Fold return matrix is incomplete (missing={sorted(missing)}, extra={sorted(extra)})"
        )

    ranking = decision.get("ranking")
    if not isinstance(ranking, list) or not ranking:
        raise ValueError("The frozen development ranking is missing")
    ranked_ids = [str(row.get("candidate_id")) for row in ranking]
    pass_ids = [
        candidate_id
        for candidate_id in CHALLENGERS
        if by_candidate[candidate_id]["development_status"] == "PASS"
    ]
    expected_ranked_ids = sorted(
        pass_ids,
        key=lambda candidate_id: (
            -min(
                returns[(candidate_id, "D1", "cost_stress_2x")],
                returns[(candidate_id, "D2", "cost_stress_2x")],
            ),
            _number(
                by_candidate[candidate_id]["worst_fold_drawdown_base_or_2x"],
                name=f"{candidate_id}.worst_fold_drawdown_base_or_2x",
            ),
            candidate_id,
        ),
    )
    if ranked_ids != expected_ranked_ids:
        raise ValueError("Development ranking does not match the frozen D1/D2 rule")
    if [int(row.get("rank", 0)) for row in ranking] != list(
        range(1, len(ranking) + 1)
    ):
        raise ValueError("Development ranking must have contiguous one-based ranks")
    for row in ranking:
        candidate_id = str(row["candidate_id"])
        expected_minimum = min(
            returns[(candidate_id, "D1", "cost_stress_2x")],
            returns[(candidate_id, "D2", "cost_stress_2x")],
        )
        if row.get("minimum_D1_D2_cost_stress_2x_return") is not None:
            _assert_close(
                _number(
                    row["minimum_D1_D2_cost_stress_2x_return"],
                    name=f"{candidate_id}.ranking_minimum",
                ),
                expected_minimum,
                name=f"{candidate_id} ranking minimum",
            )
    selected = decision.get("selected_development_challenger")
    if selected != ranked_ids[0]:
        raise ValueError("Selected development challenger must be ranking row 1")
    if decision.get("candidate_selection") != "DEVELOPMENT_CHALLENGER_SELECTED":
        raise ValueError("Unexpected development-selection state")

    for candidate_id in CANDIDATE_ORDER[:3]:
        c0_or_challenger = by_candidate[candidate_id]
        if candidate_id == "C0_CONTROL_ER72":
            fold_metrics = c0_or_challenger.get("fold_metrics", {})
            for fold_id in FOLD_ORDER:
                reported = fold_metrics.get(fold_id, {})
                _assert_close(
                    returns[(candidate_id, fold_id, "base")],
                    _number(
                        reported.get("base_total_return"),
                        name=f"{candidate_id}.{fold_id}.base_total_return",
                    ),
                    name=f"{candidate_id}.{fold_id}.base",
                )
        else:
            d3 = c0_or_challenger["D3_CONTAMINATED"]
            for scenario in SCENARIO_ORDER:
                _assert_close(
                    returns[(candidate_id, "D3_CONTAMINATED", scenario)],
                    _number(
                        d3.get(f"{scenario}_total_return"),
                        name=f"{candidate_id}.D3.{scenario}",
                    ),
                    name=f"{candidate_id}.D3.{scenario}",
                )

    c3_d3 = by_candidate["C3_ENSEMBLE_50_50"]["D3_CONTAMINATED"]
    for scenario in SCENARIO_ORDER:
        _assert_close(
            returns[("C3_ENSEMBLE_50_50", "D3_CONTAMINATED", scenario)],
            _number(c3_d3.get(f"{scenario}_total_return"), name=f"C3.D3.{scenario}"),
            name=f"C3.D3.{scenario}",
        )

    ledgers = shadow.get("ledgers")
    combined = shadow.get("combined")
    if not isinstance(ledgers, list) or not isinstance(combined, Mapping):
        raise ValueError("Shadow snapshot is missing ledger reconciliation data")
    expected_loss = sum(
        max(0.0, -_number(row.get("net_pnl_quote"), name="ledger.net_pnl_quote"))
        for row in ledgers
    )
    expected_fees = sum(
        _number(row.get("fee_quote"), name="ledger.fee_quote") for row in ledgers
    )
    expected_net = sum(
        _number(row.get("net_pnl_quote"), name="ledger.net_pnl_quote")
        for row in ledgers
    )
    _assert_close(
        _number(combined.get("loss_quote", 0.0), name="combined.loss_quote"),
        expected_loss,
        name="shadow combined loss",
    )
    _assert_close(
        _number(combined.get("fee_quote", 0.0), name="combined.fee_quote"),
        expected_fees,
        name="shadow combined fees",
    )
    _assert_close(
        _number(combined.get("net_pnl_quote", 0.0), name="combined.net_pnl_quote"),
        expected_net,
        name="shadow combined net PnL",
    )
    for row in ledgers:
        delta = _number(
            row.get("fill_to_state_reconciliation_delta_quote", 0.0),
            name="ledger reconciliation delta",
        )
        if abs(delta) > 1e-4:
            raise ValueError("Shadow fill and state PnL do not reconcile")

    markets = quality.get("markets")
    if not isinstance(markets, list) or not markets:
        raise ValueError("Data-quality market rows are missing")
    if int(quality.get("total_invalid_count", -1)) != 0:
        raise ValueError("Candle inputs contain invalid values")
    if quality.get("all_protocol_windows_complete") is not True:
        raise ValueError("Candle inputs do not cover all protocol windows")

    return {
        "protocol": protocol,
        "shadow": shadow,
        "quality": quality,
        "candidates": by_candidate,
        "returns": returns,
    }


def _return_rows(review: Mapping[str, Any]) -> list[dict[str, Any]]:
    candidates = review["candidates"]
    returns = review["returns"]
    rows: list[dict[str, Any]] = []
    for candidate_id in CANDIDATE_ORDER:
        candidate = candidates[candidate_id]
        for fold_id in FOLD_ORDER:
            for scenario in SCENARIO_ORDER:
                rows.append(
                    {
                        "candidate_id": candidate_id,
                        "candidate": CANDIDATE_LABELS[candidate_id],
                        "candidate_fold": (
                            f"{SHORT_CANDIDATE_LABELS[candidate_id]} · "
                            f"{'D3*' if fold_id == 'D3_CONTAMINATED' else fold_id}"
                        ),
                        "fold_id": fold_id,
                        "fold": FOLD_LABELS[fold_id],
                        "fold_role": (
                            "개발 상태 산정"
                            if fold_id in {"D1", "D2"}
                            else "오염·진단 전용, 상태 미반영"
                        ),
                        "scenario": SCENARIO_LABELS[scenario],
                        "scenario_id": scenario,
                        "total_return": returns[
                            (candidate_id, fold_id, scenario)
                        ],
                        "total_return_display": _percent(
                            returns[(candidate_id, fold_id, scenario)]
                        ),
                        "development_status": candidate["development_status"],
                        "promotion_status": candidate["promotion_status"],
                    }
                )
    return rows


def _candidate_rows(review: Mapping[str, Any]) -> list[dict[str, Any]]:
    candidates = review["candidates"]
    returns = review["returns"]
    decision = review["protocol"]["decision"]
    rank_by_candidate = {
        str(row["candidate_id"]): int(row["rank"])
        for row in decision["ranking"]
    }
    selected = decision["selected_development_challenger"]
    rows: list[dict[str, Any]] = []
    for candidate_id in CANDIDATE_ORDER:
        candidate = candidates[candidate_id]
        d1_base = returns[(candidate_id, "D1", "base")]
        d2_base = returns[(candidate_id, "D2", "base")]
        d1_stress = returns[(candidate_id, "D1", "cost_stress_2x")]
        d2_stress = returns[(candidate_id, "D2", "cost_stress_2x")]
        dev_base = _compound((d1_base, d2_base))
        dev_stress = _compound((d1_stress, d2_stress))
        if candidate_id in CHALLENGERS:
            _assert_close(
                dev_base,
                _number(
                    candidate.get("base_compounded_return"),
                    name=f"{candidate_id}.base_compounded_return",
                ),
                name=f"{candidate_id} compounded base return",
            )
            _assert_close(
                dev_stress,
                _number(
                    candidate.get("cost_stress_2x_compounded_return"),
                    name=f"{candidate_id}.cost_stress_2x_compounded_return",
                ),
                name=f"{candidate_id} compounded cost-stress return",
            )
        profit_factor = candidate.get("combined_base_profit_factor")
        if profit_factor == "UNBOUNDED":
            profit_factor_display = "∞"
            profit_factor_numeric = None
        else:
            profit_factor_numeric = _optional_number(profit_factor)
            profit_factor_display = (
                f"{profit_factor_numeric:.4f}"
                if profit_factor_numeric is not None
                else "—"
            )
        rows.append(
            {
                "candidate_id": candidate_id,
                "candidate": CANDIDATE_LABELS[candidate_id],
                "role": candidate["protocol_role"],
                "development_status": candidate["development_status"],
                "promotion_status": candidate["promotion_status"],
                "development_selection": (
                    "SELECTED"
                    if candidate_id == selected
                    else f"RANKED_{rank_by_candidate[candidate_id]}"
                    if candidate_id in rank_by_candidate
                    else "COMPARISON_ONLY"
                    if candidate_id == "C0_CONTROL_ER72"
                    else "NOT_SELECTED"
                ),
                "d1_base": d1_base,
                "d1_cost_stress_2x": d1_stress,
                "d2_base": d2_base,
                "d2_cost_stress_2x": d2_stress,
                "dev_compounded_base": dev_base,
                "dev_compounded_cost_stress_2x": dev_stress,
                "d3_base": returns[(candidate_id, "D3_CONTAMINATED", "base")],
                "d3_cost_stress_2x": returns[
                    (candidate_id, "D3_CONTAMINATED", "cost_stress_2x")
                ],
                "d1_base_display": _percent(d1_base),
                "d1_stress_display": _percent(d1_stress),
                "d2_base_display": _percent(d2_base),
                "d2_stress_display": _percent(d2_stress),
                "dev_base_display": _percent(dev_base),
                "dev_stress_display": _percent(dev_stress),
                "d3_base_display": _percent(
                    returns[(candidate_id, "D3_CONTAMINATED", "base")]
                ),
                "d3_stress_display": _percent(
                    returns[
                        (candidate_id, "D3_CONTAMINATED", "cost_stress_2x")
                    ]
                ),
                "combined_base_profit_factor": profit_factor_numeric,
                "profit_factor_display": profit_factor_display,
                "worst_drawdown": _optional_number(
                    candidate.get("worst_fold_drawdown_base_or_2x")
                ),
                "worst_drawdown_display": _percent(
                    _optional_number(
                        candidate.get("worst_fold_drawdown_base_or_2x")
                    )
                ),
                "nonnegative_market_count": candidate.get(
                    "nonnegative_market_count"
                ),
                "portfolio_round_trip_count": candidate.get(
                    "portfolio_round_trip_count"
                ),
            }
        )
    return rows


def _loss_component_rows(review: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for ledger in review["shadow"]["ledgers"]:
        market = str(ledger["market"])
        loss = _number(ledger["loss_quote"], name=f"{market}.loss_quote")
        fees = _number(ledger["fee_quote"], name=f"{market}.fee_quote")
        common = {
            "market": market,
            "total_loss_quote": loss,
            "fee_share": fees / loss if loss > 0 else None,
            "round_trip_count": int(ledger["round_trip_count"]),
            "average_hold_seconds": ledger.get("average_hold_seconds"),
            "runtime_error_count": int(ledger["runtime_error_count"]),
            "simulated": True,
        }
        rows.extend(
            (
                {
                    **common,
                    "component": "taker 수수료",
                    "loss_contribution_quote": fees,
                },
                {
                    **common,
                    "component": "수수료 전 가격·체결 기여",
                    "loss_contribution_quote": loss - fees,
                },
            )
        )
    if not rows:
        rows.extend(
            (
                {
                    "market": "ledger 없음",
                    "component": "taker 수수료",
                    "loss_contribution_quote": 0.0,
                    "total_loss_quote": 0.0,
                    "fee_share": None,
                    "round_trip_count": 0,
                    "average_hold_seconds": None,
                    "runtime_error_count": 0,
                    "simulated": True,
                },
                {
                    "market": "ledger 없음",
                    "component": "수수료 전 가격·체결 기여",
                    "loss_contribution_quote": 0.0,
                    "total_loss_quote": 0.0,
                    "fee_share": None,
                    "round_trip_count": 0,
                    "average_hold_seconds": None,
                    "runtime_error_count": 0,
                    "simulated": True,
                },
            )
        )
    return rows


def _source_objects(generated_at: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    manifest_sources = [
        {
            "id": "strategy_suite",
            "label": "Strategy Research V2 authoritative outputs",
            "path": "protocol_summary.json",
        },
        {
            "id": "shadow_ledger",
            "label": "Read-only BTC·ETH simulated shadow snapshot",
            "path": "shadow_snapshot.json",
        },
        {
            "id": "upbit_candles",
            "label": "Cached public OHLCV data-quality review",
            "path": "data_quality.csv",
        },
    ]
    sources = [
        {
            "id": "strategy_suite",
            "label": "Strategy Research V2 authoritative outputs",
            "path": "protocol_summary.json",
            "query": {
                "engine": "SQLite",
                "sql": (
                    "SELECT timestamp, market, open, high, low, close, volume, "
                    "quote_volume FROM candles WHERE market IN "
                    "('KRW-BTC','KRW-ETH','KRW-XRP','KRW-SOL') "
                    "AND interval_minutes = 60 ORDER BY market, timestamp;"
                ),
                "description": (
                    "The frozen Python runner loads this cached public-candle relation, "
                    "applies causal candidate signals and one shared execution model, and "
                    "writes protocol_summary.json plus the reviewed fold CSV files."
                ),
                "executed_at": generated_at,
                "tables_used": ["candles"],
                "filters": [
                    "Markets fixed to KRW-BTC, KRW-ETH, KRW-XRP, KRW-SOL",
                    "60-minute closed candles",
                    "D1 and D2 determine development status",
                    "D3_CONTAMINATED is diagnostic only and cannot affect status",
                    "Base and cost-stress runs reuse the same signal manifest",
                ],
                "metric_definitions": [
                    "Fold return = final equal-weight four-sleeve equity index - 1",
                    "Development compounded return = (1 + D1) × (1 + D2) - 1",
                    "Cost stress doubles modeled fees and slippage without refitting signals",
                    "PASS requires every frozen D1/D2 gate to pass",
                ],
            },
        },
        {
            "id": "shadow_ledger",
            "label": "Read-only BTC·ETH simulated shadow snapshot",
            "path": "shadow_snapshot.json",
            "query": {
                "engine": "SQLite",
                "sql": (
                    "WITH ordered_fills AS (SELECT side, filled_quote, fee_quote, "
                    "created_wall_ns, LAG(side) OVER (ORDER BY created_wall_ns, "
                    "fill_id) AS previous_side, LAG(filled_quote) OVER "
                    "(ORDER BY created_wall_ns, fill_id) AS buy_quote, "
                    "LAG(fee_quote) OVER (ORDER BY created_wall_ns, fill_id) AS "
                    "buy_fee FROM shadow_fills) SELECT COUNT(*) AS round_trips, "
                    "SUM(filled_quote - buy_quote - fee_quote - buy_fee) AS net_pnl "
                    "FROM ordered_fills WHERE side = 'sell' AND previous_side = 'buy';"
                ),
                "description": (
                    "The same read-only fill-pairing and reconciliation logic is applied "
                    "to each isolated simulated ledger; no credential value is read."
                ),
                "executed_at": generated_at,
                "tables_used": ["shadow_runs", "shadow_state", "shadow_fills"],
                "filters": [
                    "All simulated fills in each available ledger",
                    "FIFO buy/sell round trips",
                    "No unmatched fill is counted as a completed round trip",
                ],
                "metric_definitions": [
                    "Net PnL = sell quote - sell fee - buy quote - buy fee",
                    "Loss = max(0, -net PnL) per ledger",
                    "Non-fee contribution = loss - recorded fees",
                    "Fee share = recorded fees / loss when loss is positive",
                ],
            },
        },
        {
            "id": "upbit_candles",
            "label": "Cached public OHLCV data-quality review",
            "path": "data_quality.csv",
            "query": {
                "engine": "SQLite",
                "sql": (
                    "SELECT timestamp, market, open, high, low, close, volume, "
                    "quote_volume FROM candles WHERE market IN "
                    "('KRW-BTC','KRW-ETH','KRW-XRP','KRW-SOL') "
                    "AND interval_minutes = 60 ORDER BY market, timestamp;"
                ),
                "description": (
                    "Reviewed cached public candles used by the frozen research suite."
                ),
                "executed_at": generated_at,
                "tables_used": ["candles"],
                "filters": ["60-minute public candles", "Four frozen KRW markets"],
                "metric_definitions": [
                    "Estimated missing intervals sum the absent 60-minute slots across gaps",
                    "Invalid count covers non-finite values, price-shape errors, and negative volumes",
                ],
            },
        },
    ]
    return manifest_sources, sources


def build_artifact(input_dir: Path) -> dict[str, Any]:
    """Return a validated-by-construction canonical report payload."""

    review = load_review_inputs(input_dir)
    protocol = review["protocol"]
    shadow = review["shadow"]
    candidate_rows = _candidate_rows(review)
    return_rows = _return_rows(review)
    loss_rows = _loss_component_rows(review)

    combined = shadow["combined"]
    combined_loss = _number(combined.get("loss_quote", 0.0), name="combined loss")
    combined_fees = _number(combined.get("fee_quote", 0.0), name="combined fees")
    fee_share = combined_fees / combined_loss if combined_loss > 0 else None
    non_fee_contribution = combined_loss - combined_fees
    pass_count = sum(
        review["candidates"][candidate_id]["development_status"] == "PASS"
        for candidate_id in CHALLENGERS
    )
    d3_positive_count = sum(
        review["returns"][(candidate_id, "D3_CONTAMINATED", "base")] > 0
        for candidate_id in CHALLENGERS
    )
    promotion_count = sum(
        review["candidates"][candidate_id]["promotion_status"]
        not in {"NOT_AUTHORIZED", "NOT_ELIGIBLE"}
        for candidate_id in CANDIDATE_ORDER
    )
    selected_candidate_id = protocol["decision"][
        "selected_development_challenger"
    ]
    selected_candidate_label = CANDIDATE_LABELS[selected_candidate_id]
    generated_at = datetime.now(UTC).isoformat(timespec="seconds")
    manifest_sources, sources = _source_objects(generated_at)

    status_text = ", ".join(
        f"{SHORT_CANDIDATE_LABELS[candidate_id]} {review['candidates'][candidate_id]['development_status']}"
        for candidate_id in CHALLENGERS
    )
    d3_text = ", ".join(
        f"{SHORT_CANDIDATE_LABELS[candidate_id]} {_percent(review['returns'][(candidate_id, 'D3_CONTAMINATED', 'base')])}"
        for candidate_id in CHALLENGERS
    )
    fee_sentence = (
        f"그중 recorded taker 수수료는 **{_won(combined_fees)}({fee_share:.1%})**이고, "
        f"수수료 전 가격·체결 기여도 **{_won(non_fee_contribution)} 손실**입니다."
        if fee_share is not None and non_fee_contribution >= 0
        else f"recorded taker 수수료는 **{_won(combined_fees)}**입니다."
    )
    d3_summary = (
        "세 후보가 모두 최신 진단 구간에서 손실입니다."
        if d3_positive_count == 0
        else f"세 후보 중 {d3_positive_count}개만 최신 진단 구간에서 양수입니다."
    )

    headline_shadow = [
        {
            "loss_display": _won(combined_loss),
            "fee_share": fee_share,
            "fee_share_display": _percent(fee_share, 1),
            "round_trip_count": int(combined.get("round_trip_count", 0)),
            "orders_sent": 0,
        }
    ]
    headline_strategy = [
        {
            "development_pass_count": pass_count,
            "challenger_count": len(CHALLENGERS),
            "d3_positive_count": d3_positive_count,
            "promotion_count": promotion_count,
            "selected_challenger": selected_candidate_label,
            "active_champion": protocol["decision"]["active_champion"],
            "orders_sent": 0,
        }
    ]

    artifact = {
        "surface": "report",
        "manifest": {
            "version": 1,
            "surface": "report",
            "title": "전략 자가검수 및 시뮬레이션 보고",
            "description": (
                "기존 shadow 손실을 검산하고, 네 시장의 세 가지 사전 고정 "
                "다각화 후보를 개발·비용 스트레스·오염 진단 구간으로 분리해 검토한 보고입니다."
            ),
            "generatedAt": generated_at,
            "sources": manifest_sources,
            "cards": [
                {
                    "id": "shadow_loss",
                    "description": "BTC·ETH simulated shadow ledger의 누적 손실",
                    "dataset": "headline_shadow",
                    "sourceId": "shadow_ledger",
                    "metrics": [
                        {"label": "Shadow 누적 손실", "field": "loss_display"},
                    ],
                },
                {
                    "id": "shadow_fee_share",
                    "description": "누적 손실 중 ledger에 기록된 taker 수수료 비중",
                    "dataset": "headline_shadow",
                    "sourceId": "shadow_ledger",
                    "metrics": [
                        {
                            "label": "손실 중 수수료 비중",
                            "field": "fee_share",
                            "format": "percent",
                        }
                    ],
                },
                {
                    "id": "development_passes",
                    "description": "D1·D2의 모든 frozen gate를 통과한 challenger 수",
                    "dataset": "headline_strategy",
                    "sourceId": "strategy_suite",
                    "metrics": [
                        {"label": "개발 gate PASS", "field": "development_pass_count"},
                        {"label": "평가 challenger", "field": "challenger_count"},
                    ],
                },
                {
                    "id": "selected_challenger",
                    "description": "D1·D2의 frozen ranking rule로 선택한 다음 forward-paper challenger",
                    "dataset": "headline_strategy",
                    "sourceId": "strategy_suite",
                    "metrics": [
                        {
                            "label": "개발 선택 후보",
                            "field": "selected_challenger",
                        }
                    ],
                },
                {
                    "id": "d3_positive",
                    "description": "상태에 반영하지 않은 D3* 기본비용 수익이 양수인 challenger 수",
                    "dataset": "headline_strategy",
                    "sourceId": "strategy_suite",
                    "metrics": [
                        {"label": "D3* 양수 후보", "field": "d3_positive_count"},
                        {"label": "평가 challenger", "field": "challenger_count"},
                    ],
                },
                {
                    "id": "promotions",
                    "description": "이번 검수로 운영 승격이 허가된 전략 수",
                    "dataset": "headline_strategy",
                    "sourceId": "strategy_suite",
                    "metrics": [
                        {"label": "승격 허가", "field": "promotion_count"},
                    ],
                },
            ],
            "charts": [
                {
                    "id": "shadow_loss_components",
                    "title": "시장별 simulated shadow 손실 구성",
                    "subtitle": (
                        "양수는 손실 기여, 음수는 수수료를 일부 상쇄한 수수료 전 기여입니다."
                    ),
                    "type": "stackedBar",
                    "dataset": "loss_components",
                    "sourceId": "shadow_ledger",
                    "intent": "composition",
                    "question": "누적 shadow 손실은 수수료와 수수료 전 기여로 어떻게 나뉘는가?",
                    "rationale": "시장별 총손실의 가산 구성을 직접 검산합니다.",
                    "encodings": {
                        "x": {"field": "market", "type": "nominal", "label": "시장"},
                        "y": {
                            "field": "loss_contribution_quote",
                            "type": "quantitative",
                            "format": "number",
                            "label": "손실 기여",
                            "unit": "KRW",
                        },
                        "color": {
                            "field": "component",
                            "type": "nominal",
                            "label": "구성",
                        },
                        "tooltip": [
                            {
                                "field": "total_loss_quote",
                                "type": "quantitative",
                                "format": "number",
                                "label": "시장 총손실",
                                "unit": "KRW",
                            },
                            {
                                "field": "round_trip_count",
                                "type": "quantitative",
                                "format": "number",
                                "label": "왕복 거래",
                            },
                            {
                                "field": "average_hold_seconds",
                                "type": "quantitative",
                                "format": "number",
                                "label": "평균 보유(초)",
                            },
                        ],
                    },
                    "valueFormat": "number",
                    "unit": "KRW",
                    "layout": "full",
                },
                {
                    "id": "candidate_fold_returns",
                    "title": "후보·구간별 portfolio 수익률",
                    "subtitle": "D3*는 이미 관찰한 오염 구간이며 상태·순위·승격에 쓰지 않았습니다.",
                    "type": "bar",
                    "dataset": "candidate_fold_returns",
                    "sourceId": "strategy_suite",
                    "intent": "comparison",
                    "question": "후보별 수익은 개발 구간과 비용 2배에서 반복되는가?",
                    "rationale": "각 후보·구간에서 기본비용과 비용 2배를 나란히 비교합니다.",
                    "encodings": {
                        "x": {
                            "field": "candidate_fold",
                            "type": "nominal",
                            "label": "후보 · 구간",
                        },
                        "y": {
                            "field": "total_return",
                            "type": "quantitative",
                            "format": "percent",
                            "label": "Portfolio 수익률",
                        },
                        "color": {
                            "field": "scenario",
                            "type": "nominal",
                            "label": "비용 시나리오",
                        },
                        "tooltip": [
                            {"field": "candidate", "type": "nominal", "label": "후보"},
                            {"field": "fold", "type": "nominal", "label": "구간"},
                            {
                                "field": "total_return",
                                "type": "quantitative",
                                "format": "percent",
                                "label": "수익률",
                            },
                            {
                                "field": "development_status",
                                "type": "nominal",
                                "label": "개발 상태",
                            },
                        ],
                    },
                    "valueFormat": "percent",
                    "layout": "full",
                    "referenceLines": [
                        {
                            "value": 0,
                            "label": "손익분기",
                            "style": "dashed",
                        }
                    ],
                    "settings": {"groupMode": "grouped", "sort": "none"},
                },
            ],
            "tables": [
                {
                    "id": "candidate_audit",
                    "title": "후보별 정확 수익률·gate 요약",
                    "subtitle": (
                        "D1·D2 복리는 개발 판단용이고 D3*는 분리된 진단값입니다. "
                        "C0는 비교 전용이라 gate를 평가하지 않았습니다."
                    ),
                    "dataset": "candidate_summary",
                    "sourceId": "strategy_suite",
                    "density": "compact",
                    "layout": "full",
                    "columns": [
                        {"field": "candidate", "label": "후보", "type": "text"},
                        {
                            "field": "development_status",
                            "label": "개발 상태",
                            "type": "text",
                        },
                        {
                            "field": "development_selection",
                            "label": "개발 선택",
                            "type": "text",
                        },
                        {"field": "d1_base_display", "label": "D1 기본", "type": "text"},
                        {
                            "field": "d1_stress_display",
                            "label": "D1 비용2x",
                            "type": "text",
                        },
                        {"field": "d2_base_display", "label": "D2 기본", "type": "text"},
                        {
                            "field": "d2_stress_display",
                            "label": "D2 비용2x",
                            "type": "text",
                        },
                        {
                            "field": "dev_base_display",
                            "label": "D1·D2 복리",
                            "type": "text",
                        },
                        {
                            "field": "dev_stress_display",
                            "label": "복리 비용2x",
                            "type": "text",
                        },
                        {"field": "profit_factor_display", "label": "PF", "type": "text"},
                        {
                            "field": "worst_drawdown_display",
                            "label": "최악 MDD",
                            "type": "text",
                        },
                        {
                            "field": "nonnegative_market_count",
                            "label": "비음수 시장",
                            "type": "number",
                            "format": "number",
                        },
                        {
                            "field": "portfolio_round_trip_count",
                            "label": "개발 왕복",
                            "type": "number",
                            "format": "number",
                        },
                        {
                            "field": "d3_base_display",
                            "label": "D3* 기본",
                            "type": "text",
                        },
                        {
                            "field": "d3_stress_display",
                            "label": "D3* 비용2x",
                            "type": "text",
                        },
                        {
                            "field": "promotion_status",
                            "label": "승격",
                            "type": "text",
                        },
                    ],
                }
            ],
            "blocks": [
                {
                    "id": "title",
                    "type": "markdown",
                    "body": "# 전략 자가검수 및 시뮬레이션 보고",
                },
                {
                    "id": "executive_summary",
                    "type": "markdown",
                    "body": (
                        "## Executive Summary\n\n"
                        f"- **현재 결론은 승격 0건, observe/cash 유지입니다.** "
                        f"개발 상태는 {status_text}이고 다음 forward-paper 후보로 **{selected_candidate_label}**를 선택했지만, 운영 승격은 허가하지 않았습니다.\n"
                        f"- **기존 BTC·ETH shadow의 합산 손실은 {_won(combined_loss)}입니다.** "
                        f"{fee_sentence}\n"
                        f"- **최신 D3*는 낙관론을 지지하지 않습니다.** {d3_summary} "
                        f"기본비용 결과는 {d3_text}입니다.\n"
                        "- **다각화는 후보 수를 늘리는 것보다 검증구간을 분리하는 문제입니다.** "
                        "D3*를 보며 재튜닝하지 않고, 사전 등록한 후보를 새 T0 이후 90일·365일로 확인해야 합니다."
                    ),
                },
                {
                    "id": "headline_metrics",
                    "type": "metric-strip",
                    "cardIds": [
                        "shadow_loss",
                        "shadow_fee_share",
                        "development_passes",
                        "selected_challenger",
                        "d3_positive",
                        "promotions",
                    ],
                },
                {
                    "id": "loss_finding",
                    "type": "markdown",
                    "sourceId": "shadow_ledger",
                    "body": (
                        "## 손실은 수수료만의 문제가 아니었습니다\n\n"
                        f"합산 {_won(combined_loss)} 손실 중 수수료는 {_won(combined_fees)}이고, "
                        f"수수료를 다시 더해도 {_won(non_fee_contribution)}의 손실 기여가 남습니다. "
                        f"완료 왕복은 {int(combined.get('round_trip_count', 0)):,}회입니다. "
                        "즉 diagnostic 초단기 신호는 거래비용을 많이 냈을 뿐 아니라, 비용 전 방향성도 충분하지 않았습니다. "
                        "이 결과는 해당 diagnostic 정책을 알파 전략으로 승격하지 말아야 한다는 근거입니다."
                    ),
                },
                {
                    "id": "loss_chart_block",
                    "type": "chart",
                    "chartId": "shadow_loss_components",
                    "layout": "full",
                },
                {
                    "id": "strategy_finding",
                    "type": "markdown",
                    "sourceId": "strategy_suite",
                    "body": (
                        "## 개발구간 개선과 최신구간 손실이 동시에 관찰됐습니다\n\n"
                        f"Frozen gate 결과는 {status_text}입니다. 그러나 D3* 기본비용은 {d3_text}이고, "
                        "이 구간은 후보 설계 전에 이미 본 데이터라 선택에 사용할 수 없습니다. "
                        "개발구간의 양수 수익만 보고 승격하면 과거 구간 선택 편향을 반복하고, "
                        "D3*에 맞춰 다시 바꾸면 미래 검증 시점이 리셋됩니다."
                    ),
                },
                {
                    "id": "return_chart_block",
                    "type": "chart",
                    "chartId": "candidate_fold_returns",
                    "layout": "full",
                },
                {
                    "id": "candidate_table_block",
                    "type": "table",
                    "tableId": "candidate_audit",
                    "layout": "full",
                },
                {
                    "id": "recommendations",
                    "type": "markdown",
                    "body": (
                        "## 권고: 전략을 바꾸되 지금은 돈을 걸지 않습니다\n\n"
                        "1. 운영 champion은 `CASH_OBSERVE_ONLY`로 유지하고 diagnostic shadow를 수익 전략으로 재가동하지 않습니다.\n"
                        f"2. {selected_candidate_label}를 다음 forward-paper challenger로 사전 등록하고 C3는 고정 예비 후보로 보존하되, D3*에 맞춘 파라미터 변경은 하지 않습니다.\n"
                        "3. 새 T0 이후 90일 중간점검과 365일 확인을 모두 통과하기 전에는 승격하지 않습니다. 변경이 생기면 T0를 다시 시작합니다.\n"
                        "4. 다음 연구 버전은 추세와 상관이 낮은 후보, 거래비용보다 큰 최소 edge, 시장별 위험예산을 명시하고 같은 four-market·base·2x 기준으로 검증합니다.\n"
                        "5. runtime 재시작 문제와 전략 손익은 별도 운영 지표로 감시하며, 실제 주문 경로는 계속 비활성으로 둡니다."
                    ),
                },
                {
                    "id": "further_questions",
                    "type": "markdown",
                    "body": (
                        "## 추가로 답해야 할 질문\n\n"
                        "- D3*에서 네 시장이 동시에 약해진 원인은 추세 반전, 변동성 regime, 또는 long/flat 구조 중 무엇인가?\n"
                        "- 추세와 낮은 상관을 가지면서도 거래비용을 이기는 후보를 사전에 어떻게 정의할 것인가?\n"
                        "- 실제 forward 기간의 체결비용·신호 빈도·시장별 기여가 backtest 가정과 얼마나 일치하는가?\n"
                        "- 90일 중간점검에서 중단만 허용하고 파라미터 수정은 금지하는 운영 규칙을 어떻게 자동화할 것인가?"
                    ),
                },
                {
                    "id": "caveats",
                    "type": "markdown",
                    "body": (
                        "## 한계와 가정\n\n"
                        "- D1·D2는 개발 판단용 과거 구간이며 진정한 out-of-sample 확인이 아닙니다. D3*도 설계 전에 본 오염 구간이라 진단에만 사용했습니다.\n"
                        "- 네 시장은 동일 비중 long/flat sleeve이며, 숏·현금 이자·시장충격은 모델링하지 않았습니다. 비용 2배는 민감도 검사이지 미래 비용의 상한이 아닙니다.\n"
                        "- 공개 60분 candle은 거래가 없을 때 생성되지 않을 수 있습니다. 검증 코드는 gap을 넘어 rolling feature를 이어 붙이지 않았습니다.\n"
                        "- Shadow 체결은 전부 simulated이고 실제 주문은 0건입니다. 이 보고서는 수익 보장이나 투자 권유가 아니며, 20%는 목표 검증선일 뿐 보장 수익률이 아닙니다."
                    ),
                },
            ],
        },
        "snapshot": {
            "version": 1,
            "generatedAt": generated_at,
            "status": "ready",
            "datasets": {
                "headline_shadow": headline_shadow,
                "headline_strategy": headline_strategy,
                "loss_components": loss_rows,
                "candidate_fold_returns": return_rows,
                "candidate_summary": candidate_rows,
                "data_quality": review["quality"]["markets"],
            },
        },
        "sources": sources,
    }
    _assert_portable(artifact)
    # Strict serialization is part of the contract: NaN and infinity are invalid.
    json.dumps(artifact, ensure_ascii=False, allow_nan=False)
    return artifact


def write_artifact(input_dir: Path, output_path: Path | None = None) -> Path:
    directory = input_dir.expanduser().resolve()
    destination = (
        output_path.expanduser().resolve()
        if output_path is not None
        else directory / "artifact.json"
    )
    if destination.parent != directory:
        raise ValueError("artifact.json must be written inside the suite output directory")
    artifact = build_artifact(directory)
    content = (
        json.dumps(
            artifact,
            ensure_ascii=False,
            allow_nan=False,
            indent=2,
            sort_keys=False,
        )
        + "\n"
    ).encode("utf-8")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".artifact.json.", suffix=".tmp", dir=directory
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        if destination.is_symlink():
            raise ValueError("artifact.json destination cannot be a symbolic link")
        os.replace(temporary, destination)
        os.chmod(destination, 0o600)
    finally:
        if temporary.exists():
            temporary.unlink()
    return destination


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the canonical Strategy Research V2 report artifact."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        required=True,
        help="directory produced by run_strategy_research_v2.py",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="artifact.json path (must remain inside --input-dir)",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    output = write_artifact(args.input_dir, args.output)
    print(
        json.dumps(
            {
                "completed": True,
                "artifact": output.name,
                "surface": "report",
                "orders_sent": 0,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
