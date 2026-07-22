#!/usr/bin/env python3
"""Create and execute the reproducible Strategy Research V2 notebook.

The notebook is deliberately self-contained with respect to its calculations:
it reads only sibling JSON/CSV files from the frozen suite output directory.
Execution uses ``nbclient`` and fails closed if a reconciliation or protocol
invariant does not hold.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import tempfile
from pathlib import Path
from typing import Any, Mapping, Sequence


SECTION_HEADINGS = (
    "TL;DR",
    "Context & Methods",
    "Data",
    "Results",
    "Takeaways",
)
REQUIRED_FILES = (
    "protocol_summary.json",
    "shadow_snapshot.json",
    "data_quality.json",
    "suite_folds.csv",
    "ensemble_c3_folds.csv",
)

_WINDOWS_ABSOLUTE_PATH = re.compile(r"[A-Za-z]:[\\/]")
_LOCAL_PATH_FRAGMENT = re.compile(
    r"(?:^|[\s'\"])/(?:Users|home|private/var/folders|tmp)/"
)
_SECRET_FRAGMENT = re.compile(
    r"hooks\.slack\.com|(?:api|access|secret)[_-]?key\s*[:=]|bearer\s+[A-Za-z0-9._~-]+",
    re.IGNORECASE,
)


def _markdown_cell(source: str) -> dict[str, Any]:
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": source,
    }


def _code_cell(source: str) -> dict[str, Any]:
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": source,
    }


def build_notebook_payload() -> dict[str, Any]:
    """Return the notebook document without importing notebook dependencies."""

    setup_and_tldr = r'''from pathlib import Path
import json
import math

import pandas as pd
from IPython.display import Markdown, display

DATA_DIR = Path(".")

def load_json(name):
    with (DATA_DIR / name).open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    assert isinstance(value, dict), f"{name} must contain one JSON object"
    return value

def pct(value):
    return f"{float(value):+.4%}"

def won(value):
    return f"₩{float(value):,.2f}"

def close(actual, expected, *, tolerance=1e-7):
    return math.isclose(float(actual), float(expected), rel_tol=1e-9, abs_tol=tolerance)

protocol = load_json("protocol_summary.json")
shadow = load_json("shadow_snapshot.json")
quality = load_json("data_quality.json")
folds = pd.read_csv(DATA_DIR / "suite_folds.csv")
ensemble_folds = pd.read_csv(DATA_DIR / "ensemble_c3_folds.csv")

assert protocol["orders_sent"] == 0
assert protocol["live_order_routing"] is False
assert shadow["orders_sent"] == 0
assert shadow["live_order_routing"] is False
assert protocol["decision"]["promotion"] == "NOT_AUTHORIZED"
assert protocol["decision"]["active_champion"] == "CASH_OBSERVE_ONLY"

candidate_map = {row["candidate_id"]: row for row in protocol["candidates"]}
challengers = ["C1_SLOW_ER168", "C2_TREND_336_168", "C3_ENSEMBLE_50_50"]
statuses = ", ".join(
    f"{candidate_id.split('_', 1)[0]} {candidate_map[candidate_id]['development_status']}"
    for candidate_id in challengers
)
selected = protocol["decision"]["selected_development_challenger"]
d3_values = ", ".join(
    f"{candidate_id.split('_', 1)[0]} {pct(candidate_map[candidate_id]['D3_CONTAMINATED']['base_total_return'])}"
    for candidate_id in challengers
)
combined = shadow["combined"]
fee_share = (
    combined["fee_quote"] / combined["loss_quote"]
    if combined["loss_quote"] > 0
    else float("nan")
)

display(Markdown(
    f"""- **운영 결론:** 승격은 허가되지 않았고 champion은 `CASH_OBSERVE_ONLY`입니다.
- **개발 검수:** {statuses}; frozen ranking의 다음 forward-paper 후보는 **{selected}**입니다.
- **최신 오염 진단 D3\*:** 기본비용 수익률은 {d3_values}이며 상태·순위에 반영하지 않았습니다.
- **기존 shadow:** 누적 손실 {won(combined['loss_quote'])}, 수수료 {won(combined['fee_quote'])} ({fee_share:.1%}), 완료 왕복 {combined['round_trip_count']:,}회입니다.
- **주문 안전:** 모든 결과는 simulated이며 `orders_sent=0`입니다."""
))'''

    context_code = r'''assert protocol["protocol"] == "STRATEGY_RESEARCH_V2"
assert protocol["development_fold_ids"] == ["D1", "D2"]
assert protocol["contaminated_diagnostic_fold_id"] == "D3_CONTAMINATED"
assert protocol["contaminated_fold_influences_any_status"] is False

context = pd.DataFrame(
    [
        {"구간": "D1", "역할": "개발 gate", "상태/순위 반영": True},
        {"구간": "D2", "역할": "개발 gate", "상태/순위 반영": True},
        {"구간": "D3*", "역할": "오염 진단 전용", "상태/순위 반영": False},
    ]
)
display(context)

method = pd.DataFrame(
    [
        {"항목": "시장", "고정값": "KRW-BTC / ETH / XRP / SOL, 동일 25% sleeve"},
        {"항목": "실행", "고정값": "60분 candle, causal signal, next-open, long/flat"},
        {"항목": "비용", "고정값": "기본비용 + 동일 signal의 비용 2배 stress"},
        {"항목": "선택", "고정값": "PASS 후보 중 D1/D2 최저 2x 수익 최대화; D3* 제외"},
        {"항목": "승격", "고정값": "이번 연구에서는 NOT_AUTHORIZED"},
    ]
)
display(method)'''

    data_code = r'''market_quality = pd.DataFrame(quality["markets"])
assert quality["all_protocol_windows_complete"] is True
assert quality["total_invalid_count"] == 0
assert market_quality["market"].nunique() == 4
assert int(market_quality["invalid_total_count"].sum()) == 0

quality_view = market_quality[
    [
        "market",
        "row_count",
        "first_timestamp",
        "last_timestamp",
        "gap_count",
        "estimated_missing_interval_count",
        "maximum_gap_minutes",
        "invalid_total_count",
        "D1_complete",
        "D2_complete",
        "D3_CONTAMINATED_complete",
    ]
].copy()
display(quality_view)

ledgers = shadow["ledgers"]
expected_net = sum(float(row["net_pnl_quote"]) for row in ledgers)
expected_loss = sum(max(0.0, -float(row["net_pnl_quote"])) for row in ledgers)
expected_fees = sum(float(row["fee_quote"]) for row in ledgers)
assert close(combined["net_pnl_quote"], expected_net)
assert close(combined["loss_quote"], expected_loss)
assert close(combined["fee_quote"], expected_fees)
assert all(
    abs(float(row["fill_to_state_reconciliation_delta_quote"])) <= 1e-4
    for row in ledgers
)

loss_check = pd.DataFrame(
    [
        {
            "누적 손실": combined["loss_quote"],
            "recorded 수수료": combined["fee_quote"],
            "수수료 전 가격·체결 손실 기여": combined["loss_quote"] - combined["fee_quote"],
            "수수료 비중": fee_share,
            "완료 왕복": combined["round_trip_count"],
            "fill/state 검산": "PASS",
        }
    ]
)
loss_check_display = loss_check.copy()
for column in ["누적 손실", "recorded 수수료", "수수료 전 가격·체결 손실 기여"]:
    loss_check_display[column] = loss_check_display[column].map(lambda value: f"₩{value:,.2f}")
loss_check_display["수수료 비중"] = loss_check_display["수수료 비중"].map(lambda value: f"{value:.1%}")
display(loss_check_display)'''

    results_code = r'''candidate_order = [
    "C0_CONTROL_ER72",
    "C1_SLOW_ER168",
    "C2_TREND_336_168",
    "C3_ENSEMBLE_50_50",
]
fold_order = ["D1", "D2", "D3_CONTAMINATED"]
scenarios = ["base", "cost_stress_2x"]

return_lookup = {}
for row in folds.to_dict("records"):
    candidate_id = row["candidate_id"]
    fold_id = row["fold_id"]
    if candidate_id not in candidate_order[:3] or fold_id not in fold_order:
        continue
    for scenario in scenarios:
        return_lookup[(candidate_id, fold_id, scenario)] = float(
            row[f"{scenario}_total_return"]
        )
for row in ensemble_folds.to_dict("records"):
    key = ("C3_ENSEMBLE_50_50", row["fold_id"], row["scenario"])
    if row["fold_id"] in fold_order and row["scenario"] in scenarios:
        return_lookup[key] = float(row["total_return"])

expected_return_keys = {
    (candidate_id, fold_id, scenario)
    for candidate_id in candidate_order
    for fold_id in fold_order
    for scenario in scenarios
}
assert set(return_lookup) == expected_return_keys

thresholds = protocol["thresholds"]
gate_audit = []
for candidate_id in challengers:
    candidate = candidate_map[candidate_id]
    gates_match = all(isinstance(row["passed"], bool) for row in candidate["gates"])
    expected_status = (
        "INSUFFICIENT_EVIDENCE"
        if not candidate["complete_four_market_evidence"]
        or int(candidate["portfolio_round_trip_count"]) < int(thresholds["minimum_total_trades"])
        else "PASS"
        if all(row["passed"] for row in candidate["gates"])
        else "FAIL"
    )
    assert gates_match
    assert candidate["development_status"] == expected_status
    assert candidate["D3_CONTAMINATED"]["influences_development_status"] is False
    for scenario in scenarios:
        assert close(
            return_lookup[(candidate_id, "D3_CONTAMINATED", scenario)],
            candidate["D3_CONTAMINATED"][f"{scenario}_total_return"],
        )
    gate_audit.append(
        {
            "candidate_id": candidate_id,
            "recomputed_status": expected_status,
            "reported_status": candidate["development_status"],
            "all_gate_rows_boolean": gates_match,
            "D3*_excluded": True,
        }
    )

ranking = protocol["decision"]["ranking"]
ranked_ids = [row["candidate_id"] for row in ranking]
pass_ids = [
    candidate_id
    for candidate_id in challengers
    if candidate_map[candidate_id]["development_status"] == "PASS"
]
expected_ranked_ids = sorted(
    pass_ids,
    key=lambda candidate_id: (
        -min(
            return_lookup[(candidate_id, "D1", "cost_stress_2x")],
            return_lookup[(candidate_id, "D2", "cost_stress_2x")],
        ),
        float(candidate_map[candidate_id]["worst_fold_drawdown_base_or_2x"]),
        candidate_id,
    ),
)
assert ranked_ids == expected_ranked_ids
assert [row["rank"] for row in ranking] == list(range(1, len(ranking) + 1))
for row in ranking:
    expected_minimum = min(
        return_lookup[(row["candidate_id"], "D1", "cost_stress_2x")],
        return_lookup[(row["candidate_id"], "D2", "cost_stress_2x")],
    )
    if "minimum_D1_D2_cost_stress_2x_return" in row:
        assert close(row["minimum_D1_D2_cost_stress_2x_return"], expected_minimum)
assert selected == ranked_ids[0]
assert candidate_map[selected]["development_status"] == "PASS"

rows = []
for candidate_id in candidate_order:
    candidate = candidate_map[candidate_id]
    d1_base = return_lookup[(candidate_id, "D1", "base")]
    d2_base = return_lookup[(candidate_id, "D2", "base")]
    d1_stress = return_lookup[(candidate_id, "D1", "cost_stress_2x")]
    d2_stress = return_lookup[(candidate_id, "D2", "cost_stress_2x")]
    dev_base = (1 + d1_base) * (1 + d2_base) - 1
    dev_stress = (1 + d1_stress) * (1 + d2_stress) - 1
    if candidate_id in challengers:
        assert close(dev_base, candidate["base_compounded_return"])
        assert close(dev_stress, candidate["cost_stress_2x_compounded_return"])
    rows.append(
        {
            "candidate_id": candidate_id,
            "status": candidate["development_status"],
            "D1 base": d1_base,
            "D1 2x": d1_stress,
            "D2 base": d2_base,
            "D2 2x": d2_stress,
            "D1·D2 base 복리": dev_base,
            "D1·D2 2x 복리": dev_stress,
            "D3* base (미반영)": return_lookup[(candidate_id, "D3_CONTAMINATED", "base")],
            "D3* 2x (미반영)": return_lookup[(candidate_id, "D3_CONTAMINATED", "cost_stress_2x")],
            "promotion": candidate["promotion_status"],
        }
    )

candidate_results = pd.DataFrame(rows)
percent_columns = [column for column in candidate_results.columns if "D1" in column or "D2" in column or "D3" in column]
candidate_results_display = candidate_results.copy()
for column in percent_columns:
    candidate_results_display[column] = candidate_results_display[column].map(lambda value: f"{value:+.4%}")
display(candidate_results_display)
display(pd.DataFrame(gate_audit))

verification = pd.DataFrame(
    [
        {"검산": "D1/D2 gate 재계산", "결과": "PASS"},
        {"검산": "D3* 상태·순위 제외", "결과": "PASS"},
        {"검산": "Shadow loss/fee/fill-state", "결과": "PASS"},
        {"검산": "실주문 경로", "결과": "orders_sent=0 / routing=false"},
    ]
)
display(verification)'''

    takeaways_code = r'''selected_d3 = candidate_map[selected]["D3_CONTAMINATED"]
reserve = ranked_ids[1] if len(ranked_ids) > 1 else "없음"
display(Markdown(
    f"""1. **운영은 바꾸지 않습니다.** `CASH_OBSERVE_ONLY`, promotion `NOT_AUTHORIZED`, `orders_sent=0`을 유지합니다.
2. **다음 연구 동작은 명확합니다.** `{selected}`를 새 T0의 forward-paper challenger로 등록하고, `{reserve}`는 고정 예비 후보로 보존합니다.
3. **D3\*를 보고 튜닝하지 않습니다.** 선택 후보의 D3\* 기본/비용2x는 {pct(selected_d3['base_total_return'])} / {pct(selected_d3['cost_stress_2x_total_return'])}였지만, 이미 본 구간이므로 선택·수정에 사용하면 검증 시점이 리셋됩니다.
4. **90일/365일 확인이 필요합니다.** 비용 2배, 네 시장 기여, drawdown, 거래 수 gate를 그대로 유지하고 모든 변경은 새 연구 버전으로 분리합니다.
5. **수익 보장은 없습니다.** 20%는 목표 검증선이지 보장 수익률이 아니며, 이 notebook의 체결은 전부 simulated입니다."""
))'''

    cells = [
        _markdown_cell("# TL;DR"),
        _code_cell(setup_and_tldr),
        _markdown_cell(
            "## Context & Methods\n\n"
            "이 notebook은 frozen Strategy Research V2 출력만 읽습니다. "
            "D1·D2가 gate와 development ranking을 결정하고, 이미 관찰한 D3*는 진단 전용입니다."
        ),
        _code_cell(context_code),
        _markdown_cell(
            "## Data\n\n"
            "네 시장의 public 60분 OHLCV 품질과 BTC·ETH simulated shadow ledger를 독립 검산합니다."
        ),
        _code_cell(data_code),
        _markdown_cell(
            "## Results\n\n"
            "CSV fold 수익률에서 D1·D2 gate, D3* 비선정, ranking, shadow loss/fee breakdown을 다시 계산합니다."
        ),
        _code_cell(results_code),
        _markdown_cell("## Takeaways"),
        _code_cell(takeaways_code),
    ]
    for index, cell in enumerate(cells, start=1):
        cell["id"] = f"strategy-v2-{index:02d}"
    return {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3 (ipykernel)",
                "language": "python",
                "name": "python3",
            },
            "language_info": {"name": "python", "version": "3"},
            "coinpilot": {
                "analysis": "STRATEGY_RESEARCH_V2",
                "research_only": True,
                "orders_sent": 0,
                "sources": list(REQUIRED_FILES),
            },
        },
        "cells": cells,
    }


def _assert_safe_notebook(value: Any, *, location: str = "$") -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            _assert_safe_notebook(item, location=f"{location}.{key}")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _assert_safe_notebook(item, location=f"{location}[{index}]")
        return
    if isinstance(value, str):
        if (
            _WINDOWS_ABSOLUTE_PATH.search(value)
            or _LOCAL_PATH_FRAGMENT.search(value)
            or "file://" in value
        ):
            raise ValueError(f"Machine-local path found in notebook at {location}")
        if _SECRET_FRAGMENT.search(value):
            raise ValueError(f"Credential-like text found in notebook at {location}")
        return
    if value is None or isinstance(value, (bool, int, float)):
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError(f"Non-finite value found in notebook at {location}")
        return
    raise TypeError(f"Unsupported notebook value at {location}: {type(value).__name__}")


def execute_notebook(
    input_dir: Path,
    *,
    output_name: str = "analysis.ipynb",
    kernel_name: str = "python3",
    timeout: int = 600,
) -> Path:
    directory = input_dir.expanduser().resolve()
    if not directory.is_dir() or directory.is_symlink():
        raise ValueError("Suite input must be a regular directory")
    for name in REQUIRED_FILES:
        path = directory / name
        if not path.is_file() or path.is_symlink():
            raise ValueError(f"Required suite file is missing or unsafe: {name}")
    if Path(output_name).name != output_name or not output_name.endswith(".ipynb"):
        raise ValueError("--output-name must be one .ipynb filename")
    destination = directory / output_name
    if destination.is_symlink():
        raise ValueError("Notebook destination cannot be a symbolic link")

    try:
        import nbformat
        from nbclient import NotebookClient
    except ImportError as error:  # pragma: no cover - environment dependent
        raise RuntimeError(
            "Notebook execution requires the project report extra: pip install -e '.[report]'"
        ) from error

    notebook = nbformat.from_dict(build_notebook_payload())
    client = NotebookClient(
        notebook,
        timeout=timeout,
        kernel_name=kernel_name,
        allow_errors=False,
        record_timing=False,
        resources={"metadata": {"path": str(directory)}},
    )
    executed = client.execute()
    for cell in executed.cells:
        cell.metadata.pop("execution", None)

    plain = json.loads(nbformat.writes(executed, version=4))
    _assert_safe_notebook(plain)
    content = (json.dumps(plain, ensure_ascii=False, indent=1, allow_nan=False) + "\n").encode(
        "utf-8"
    )
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{output_name}.", suffix=".tmp", dir=directory
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
        os.chmod(destination, 0o600)
    finally:
        if temporary.exists():
            temporary.unlink()
    return destination


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build and execute the Strategy Research V2 analysis notebook."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        required=True,
        help="directory produced by run_strategy_research_v2.py",
    )
    parser.add_argument(
        "--output-name",
        default="analysis.ipynb",
        help="notebook filename inside --input-dir",
    )
    parser.add_argument("--kernel-name", default="python3")
    parser.add_argument("--timeout", type=int, default=600)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.timeout < 1:
        raise ValueError("--timeout must be positive")
    output = execute_notebook(
        args.input_dir,
        output_name=args.output_name,
        kernel_name=args.kernel_name,
        timeout=args.timeout,
    )
    print(
        json.dumps(
            {
                "completed": True,
                "executed_top_to_bottom": True,
                "notebook": output.name,
                "orders_sent": 0,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
