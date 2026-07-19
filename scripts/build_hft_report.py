"""Build the canonical Data Analytics artifact for the CoinPilot HFT study."""

from __future__ import annotations

import csv
import json
import math
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
CAPTURE = ROOT / "artifacts/hft/live-krw-btc-20260719-60s.jsonl"
QUALITY = ROOT / "artifacts/hft/live-krw-btc-20260719-60s.quality.json"
PUBLIC_TRADES = ROOT / "artifacts/hft/live-krw-btc-20260719-60s.trades.csv"
SIMULATION = ROOT / "artifacts/hft/simulation/simulation-summary.json"
SIM_EXECUTIONS = ROOT / "artifacts/hft/simulation/simulated-executions.csv"
SIM_EQUITY = ROOT / "artifacts/hft/simulation/simulation-equity.csv"
ASSESSMENT = ROOT / "artifacts/research-assessment.json"
DATABASE = ROOT / "var/coinpilot.db"
REPORT_DIR = ROOT / "artifacts/hft-report"
ARTIFACT = REPORT_DIR / "artifact.json"
METHOD = REPORT_DIR / "analysis-method.md"


SCENARIO_LABELS = {
    "null_alpha_base": "무알파·기본비용",
    "weak_alpha_base": "약한 알파·기본",
    "weak_alpha_cost_2x": "약한 알파·비용 2배",
    "weak_alpha_latency_5": "약한 알파·지연 5",
    "illustrative_strong_alpha": "강한 알파 예시",
}


def _utc_ms(value: int) -> str:
    return datetime.fromtimestamp(value / 1_000, UTC).isoformat(
        timespec="milliseconds"
    )


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _candle_inventory() -> dict[str, Any]:
    with sqlite3.connect(DATABASE) as connection:
        rows = connection.execute(
            """
            SELECT timestamp
            FROM candles
            WHERE market = 'KRW-BTC' AND interval_minutes = 60
            ORDER BY timestamp
            """
        ).fetchall()
        paper = connection.execute(
            """
            SELECT event_type, COUNT(*), MIN(timestamp), MAX(timestamp)
            FROM paper_events
            GROUP BY event_type
            ORDER BY event_type
            """
        ).fetchall()
    timestamps = [
        datetime.fromisoformat(str(row[0]).replace("Z", "+00:00"))
        for row in rows
    ]
    gaps = sum(
        (current - previous).total_seconds() != 3_600
        for previous, current in zip(timestamps, timestamps[1:])
    )
    missing_hours = sum(
        (current - previous).total_seconds() / 3_600 - 1
        for previous, current in zip(timestamps, timestamps[1:])
        if (current - previous).total_seconds() != 3_600
    )
    return {
        "rows": len(rows),
        "first": timestamps[0].isoformat() if timestamps else None,
        "last": timestamps[-1].isoformat() if timestamps else None,
        "gaps": gaps,
        "missing_hours": int(missing_hours),
        "paper": paper,
    }


def _capture_rows() -> tuple[
    dict[str, Any],
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    quality_doc = _load_json(QUALITY)
    quality = quality_doc["quality"]
    events = [
        json.loads(line)
        for line in CAPTURE.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    books = [row for row in events if row["event_type"] == "orderbook"]
    trades = [row for row in events if row["event_type"] == "trade"]
    spreads = [
        (row["best_ask_price"] - row["best_bid_price"])
        / ((row["best_ask_price"] + row["best_bid_price"]) / 2)
        * 10_000
        for row in books
    ]
    ordered_spreads = sorted(spreads)
    p95_position = (len(ordered_spreads) - 1) * 0.95
    p95_lower = math.floor(p95_position)
    p95_upper = math.ceil(p95_position)
    p95_fraction = p95_position - p95_lower
    p95_spread = (
        ordered_spreads[p95_lower] * (1 - p95_fraction)
        + ordered_spreads[p95_upper] * p95_fraction
    )
    live = {
        **quality,
        "first_exchange_utc": _utc_ms(
            min(row["exchange_timestamp_ms"] for row in events)
        ),
        "last_exchange_utc": _utc_ms(
            max(row["exchange_timestamp_ms"] for row in events)
        ),
        "median_spread_bps": median(spreads),
        "p95_spread_bps": p95_spread,
        "buy_trades": sum(
            row["aggressor_side"] == "buy" for row in trades
        ),
        "sell_trades": sum(
            row["aggressor_side"] == "sell" for row in trades
        ),
        "trade_volume_btc": sum(row["trade_volume"] for row in trades),
        "trade_notional_krw": sum(
            row["trade_volume"] * row["trade_price"] for row in trades
        ),
        "l1_insufficient_for_250k_count": sum(
            row["best_ask_price"] * row["best_ask_size"] < 250_000
            for row in books
        ),
    }
    live["l1_insufficient_for_250k_share"] = (
        live["l1_insufficient_for_250k_count"] / len(books)
        if books
        else None
    )
    quality_rows = [
        {
            "sort_order": 1,
            "metric": "관측 구간",
            "result": f"{live['coverage_seconds']:.3f}초",
            "interpretation": "수집·스키마 스모크 테스트만 가능",
        },
        {
            "sort_order": 2,
            "metric": "전체 이벤트",
            "result": (
                f"{live['valid_events']:,}건 · "
                f"{live['overall_event_rate_hz']:.3f}Hz"
            ),
            "interpretation": "호가와 공개 시장 체결의 합계",
        },
        {
            "sort_order": 3,
            "metric": "호가 이벤트",
            "result": (
                f"{live['orderbook_events']:,}건 · "
                f"{live['orderbook_event_rate_hz']:.3f}Hz"
            ),
            "interpretation": "30단계 집계 주문장",
        },
        {
            "sort_order": 4,
            "metric": "공개 시장 체결",
            "result": (
                f"{live['trade_events']:,}건 · "
                f"{live['trade_event_rate_hz']:.3f}Hz"
            ),
            "interpretation": "CoinPilot 계좌 체결이 아님",
        },
        {
            "sort_order": 5,
            "metric": "공격 방향",
            "result": f"매수 {live['buy_trades']}건 / 매도 {live['sell_trades']}건",
            "interpretation": "건수는 60초 표본에 한정",
        },
        {
            "sort_order": 6,
            "metric": "공개 체결량·대금",
            "result": (
                f"{live['trade_volume_btc']:.8f} BTC · "
                f"{live['trade_notional_krw']:,.0f}원"
            ),
            "interpretation": "시장 전체 공개 체결의 합계",
        },
        {
            "sort_order": 7,
            "metric": "스프레드",
            "result": (
                f"중앙 {live['median_spread_bps']:.3f}bp · "
                f"p95 {live['p95_spread_bps']:.3f}bp"
            ),
            "interpretation": "최우선 ask-bid 기준",
        },
        {
            "sort_order": 8,
            "metric": "25만원 L1 부족",
            "result": f"{live['l1_insufficient_for_250k_share']:.1%}",
            "interpretation": "전량 touch 체결 가정이 낙관적인 비율",
        },
        {
            "sort_order": 9,
            "metric": "무결성 오류",
            "result": (
                f"필드 {live['field_error_count']} · 중복 "
                f"{live['duplicate_trade_sequence_id_count']} · 역행 "
                f"{live['receive_timestamp_regression_count'] + live['exchange_timestamp_regression_count']}"
            ),
            "interpretation": "이번 표본에서 모두 0",
        },
        {
            "sort_order": 10,
            "metric": "clock-mixed 시차",
            "result": (
                f"p50 {live['receive_minus_exchange_lag_ms_p50']:.1f}ms · "
                f"p95 {live['receive_minus_exchange_lag_ms_p95']:.1f}ms"
            ),
            "interpretation": "시계 오프셋 포함·순수 네트워크 지연 아님",
        },
    ]
    trade_sample = [
        {
            "exchange_time_utc": datetime.fromtimestamp(
                row["exchange_timestamp_ms"] / 1_000,
                UTC,
            ).strftime("%H:%M:%S.%f")[:-3],
            "sequence_id": str(row["sequence_id"]),
            "aggressor_side": (
                "매수 체결" if row["aggressor_side"] == "buy" else "매도 체결"
            ),
            "trade_price_krw": row["trade_price"],
            "trade_volume_btc": row["trade_volume"],
            "best_bid_ask_krw": (
                f"{row['best_bid_price']:,.0f} / "
                f"{row['best_ask_price']:,.0f}"
            ),
            "record_kind": "시장 공개 체결·내 주문 아님",
        }
        for row in trades[:10]
    ]
    return live, quality_rows, trade_sample


def _simulation_rows() -> tuple[
    dict[str, Any],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    study = _load_json(SIMULATION)
    scenario_rows: list[dict[str, Any]] = []
    for order, scenario in enumerate(study["scenarios"], start=1):
        metrics = scenario["metrics"]
        trades = int(metrics["trade_count"])
        notional = (
            float(metrics["turnover"])
            * float(metrics["initial_cash"])
            / (trades * 2)
            if trades
            else 0.0
        )
        mid_gross_pnl = (
            float(metrics["gross_pnl_before_fees"])
            + float(metrics["estimated_spread_cost"])
        )
        denominator = trades * notional
        edge_bps = (
            mid_gross_pnl / denominator * 10_000
            if denominator
            else None
        )
        hurdle_bps = (
            (
                float(metrics["total_fees"])
                + float(metrics["estimated_spread_cost"])
            )
            / denominator
            * 10_000
            if denominator
            else None
        )
        scenario_rows.append(
            {
                "sort_order": order,
                "scenario": scenario["name"],
                "scenario_label": SCENARIO_LABELS[scenario["name"]],
                "description": scenario["description"],
                "alpha_bps_per_event": scenario["alpha_bps_per_event"],
                "net_return": metrics["net_return"],
                "net_pnl_krw": metrics["net_pnl"],
                "mid_gross_edge_bps_per_trade": edge_bps,
                "cost_hurdle_bps_per_trade": hurdle_bps,
                "edge_hurdle_display": (
                    f"{edge_bps:.2f} / {hurdle_bps:.2f}bp"
                    if edge_bps is not None and hurdle_bps is not None
                    else "N/A"
                ),
                "fees_krw": metrics["total_fees"],
                "spread_cost_krw": metrics["estimated_spread_cost"],
                "trade_count": trades,
                "profit_factor": metrics["profit_factor"],
                "max_drawdown": metrics["max_drawdown"],
                "latency_steps": metrics["latency_steps"],
                "turnover": metrics["turnover"],
                "avg_net_pnl_per_trade": (
                    float(metrics["net_pnl"]) / trades if trades else None
                ),
                "evidence": "결정론적 합성·수익성 증거 아님",
            }
        )

    equity_source = _read_csv(SIM_EQUITY)
    equity_rows = [
        {
            "scenario": row["scenario"],
            "scenario_label": SCENARIO_LABELS[row["scenario"]],
            "event_index": int(row["event_index"]),
            "equity_krw": float(row["equity"]),
            "drawdown": float(row["drawdown"]),
            "simulated": True,
        }
        for row in equity_source
        if row["scenario"]
        in {
            "null_alpha_base",
            "weak_alpha_base",
            "weak_alpha_cost_2x",
        }
    ]
    execution_source = _read_csv(SIM_EXECUTIONS)
    execution_rows = [
        {
            "trade_index": int(row["trade_index"]),
            "scenario_label": SCENARIO_LABELS[row["scenario"]],
            "entry_signal_seq": int(row["entry_decision_sequence"]),
            "entry_fill_seq": int(row["entry_fill_sequence"]),
            "exit_fill_seq": int(row["exit_fill_sequence"]),
            "fill_path": (
                f"{int(row['entry_fill_sequence'])} → "
                f"{int(row['exit_fill_sequence'])}"
            ),
            "entry_price": float(row["entry_price"]),
            "exit_price": float(row["exit_price"]),
            "price_path": (
                f"{float(row['entry_price']):,.0f} → "
                f"{float(row['exit_price']):,.0f}"
            ),
            "fees_krw": float(row["fees"]),
            "net_pnl_krw": float(row["net_pnl"]),
            "holding_steps": int(row["holding_steps"]),
            "exit_reason": row["exit_reason"],
            "record_kind": "모의체결",
        }
        for row in execution_source
        if row["scenario"] == "weak_alpha_base"
    ][:12]
    return study, scenario_rows, equity_rows, execution_rows


def _inventory_rows(
    candle: dict[str, Any],
    live: dict[str, Any],
    simulation_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    paper_count = sum(int(row[1]) for row in candle["paper"])
    paper_fills = sum(
        int(row[1])
        for row in candle["paper"]
        if str(row[0]).lower() in {"fill", "buy_fill", "sell_fill"}
    )
    return [
        {
            "sort_order": 1,
            "dataset": "60분봉 OHLCV",
            "source_kind": "실측 공개 데이터",
            "rows": candle["rows"],
            "period": f"{candle['first']} ~ {candle['last']}",
            "execution_count": 0,
            "quality_note": (
                f"시간 간격 이상 {candle['gaps']}개·누락 {candle['missing_hours']}시간"
            ),
            "use": "중저빈도 연구·HFT 근거 아님",
        },
        {
            "sort_order": 2,
            "dataset": "기존 paper ledger",
            "source_kind": "내부 모의 계좌",
            "rows": paper_count,
            "period": "2026-07-18",
            "execution_count": paper_fills,
            "quality_note": "초기화 이벤트만 존재",
            "use": "실제/forward 체결 증거 없음",
        },
        {
            "sort_order": 3,
            "dataset": "HFT 공개 호가",
            "source_kind": "실측 공개 WebSocket",
            "rows": live["orderbook_events"],
            "period": (
                f"{live['first_exchange_utc']} ~ "
                f"{live['last_exchange_utc']}"
            ),
            "execution_count": 0,
            "quality_note": (
                "30단계 집계·개별 queue ID 없음·25만원 L1 부족 "
                f"{live['l1_insufficient_for_250k_share']:.1%}"
            ),
            "use": "수집 스모크·스프레드/깊이 관측",
        },
        {
            "sort_order": 4,
            "dataset": "HFT 공개 체결",
            "source_kind": "실측 시장 전체 체결",
            "rows": live["trade_events"],
            "period": "동일 60초 표본",
            "execution_count": 0,
            "quality_note": "내 주문 체결이 아님",
            "use": "signed flow와 체결 강도 관측",
        },
        {
            "sort_order": 5,
            "dataset": "HFT 시뮬레이션 체결",
            "source_kind": "결정론적 합성",
            "rows": sum(row["trade_count"] for row in simulation_rows),
            "period": "시나리오별 10,000 이벤트·약 16.7분",
            "execution_count": sum(
                row["trade_count"] for row in simulation_rows
            ),
            "quality_note": "ask 매수/bid 매도 taker-only",
            "use": "비용·지연 메커니즘 검증만",
        },
        {
            "sort_order": 6,
            "dataset": "실제 거래소 내 주문 체결",
            "source_kind": "없음",
            "rows": 0,
            "period": "없음",
            "execution_count": 0,
            "quality_note": "private 주문·myOrder 미구현",
            "use": "수익성·실행 품질 평가 불가",
        },
    ]


def _development_rows() -> list[dict[str, Any]]:
    return [
        {
            "order": 1,
            "stage": "연속 수집기",
            "deliverable": "원시 호가·체결 JSONL, reconnect/gap/clock 로그",
            "minimum_data": "KRW-BTC 30일 연속·최소 500만 이벤트",
            "pass_gate": "파싱 오류 0, 중복/역행 설명 가능, 누락구간 명시",
            "current_status": "20~60초 스모크만 완료",
        },
        {
            "order": 2,
            "stage": "시계·품질 계층",
            "deliverable": "NTP 오프셋과 monotonic inter-arrival 분리",
            "minimum_data": "재연결·부하·일중 시간대 포함",
            "pass_gate": "p99 처리지연 측정 가능, 음수 clock-mixed 값 해소",
            "current_status": "미완료",
        },
        {
            "order": 3,
            "stage": "이벤트 특징·라벨",
            "deliverable": "L1/L5 불균형, microprice, signed flow, 미래 mid-return",
            "minimum_data": "훈련/검증/봉인 holdout 시간 분리",
            "pass_gate": "누수 테스트·purge 통과, 단순 기준 대비 순증분",
            "current_status": "합성 signal만 존재",
        },
        {
            "order": 4,
            "stage": "보수적 재생 엔진",
            "deliverable": "taker, 깊이 sweep, impact, event-time latency, 2×비용",
            "minimum_data": "실측 이벤트와 장애구간",
            "pass_gate": "기본·2×비용 OOS PnL>0, PF≥1.15, MDD≤15%",
            "current_status": "taker L1·합성 비용/지연 완료",
        },
        {
            "order": 5,
            "stage": "maker/queue 연구",
            "deliverable": "내 주문 상태·부분체결·취소/재주문 원장",
            "minimum_data": "private myOrder shadow 기록",
            "pass_gate": "queue 가정이 실측 체결률·취소율과 보정됨",
            "current_status": "차단·공개 L2만으로 불가",
        },
        {
            "order": 6,
            "stage": "shadow paper",
            "deliverable": "주문은 보내지 않는 실시간 의사결정·가상 체결 원장",
            "minimum_data": "30일 연속 및 1,000회 왕복 중 더 긴 조건",
            "pass_gate": "비용 후 PnL>0, 원장/리스크 위반 0, 지연 가정 내",
            "current_status": "미완료",
        },
        {
            "order": 7,
            "stage": "극소액 canary",
            "deliverable": "별도 승인 live adapter, 수동 arm, kill switch",
            "minimum_data": "앞선 모든 gate와 별도 보안 리뷰",
            "pass_gate": "출금권한 없음, 손실 cap, reconciliation 100%",
            "current_status": "live hard lock 유지",
        },
    ]


def _sql_text(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return repr(value)
    return "'" + str(value).replace("'", "''") + "'"


def _rows_sql(rows: list[dict[str, Any]]) -> str:
    if not rows:
        raise ValueError("A source-backed artifact table cannot be empty")
    fields = list(rows[0])
    selects: list[str] = []
    for index, row in enumerate(rows):
        expressions = [
            f'{_sql_text(row[field])} AS "{field}"'
            if index == 0
            else _sql_text(row[field])
            for field in fields
        ]
        selects.append("SELECT " + ", ".join(expressions))
    return " UNION ALL ".join(selects)


def _materialize_rows_query(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    with sqlite3.connect(":memory:") as connection:
        cursor = connection.execute(_rows_sql(rows))
        columns = [description[0] for description in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _build_sources(
    generated_at: str,
    dataset_sql: dict[str, str],
) -> list[dict[str, Any]]:
    return [
        {
            "id": "data_inventory",
            "label": "CoinPilot 로컬 데이터 인벤토리",
            "path": "artifacts/hft-report/analysis-method.md",
            "query": {
                "engine": "sqlite",
                "language": "sql",
                "executed_at": generated_at,
                "description": (
                    "로컬 캔들·paper 이벤트 범위와 HFT 생성 산출물의 "
                    "행 수를 재현합니다."
                ),
                "sql": dataset_sql["data_inventory"],
                "tables_used": ["candles", "paper_events"],
                "filters": [
                    "market = KRW-BTC",
                    "interval_minutes = 60",
                    "generated HFT artifact row counts are read from their "
                    "declared relative files",
                ],
                "metric_definitions": [
                    "execution_count counts only own-account or explicitly "
                    "simulated fills; public market trades are never own fills."
                ],
            },
        },
        {
            "id": "capture_quality",
            "label": "60초 공개 WebSocket 품질 프로파일",
            "path": "artifacts/hft/live-krw-btc-20260719-60s.quality.json",
            "query": {
                "engine": "sqlite",
                "language": "sql",
                "executed_at": generated_at,
                "description": (
                    "정규화된 공개 호가·체결 JSONL을 검증하고 이벤트율, "
                    "스프레드, 중복, 역행, clock-mixed 시차를 집계합니다."
                ),
                "sql": dataset_sql["live_quality"],
                "tables_used": [
                    "artifacts/hft/live-krw-btc-20260719-60s.quality.json"
                ],
                "filters": [
                    "KRW-BTC only",
                    "2026-07-19 09:05:06Z through 09:06:06Z",
                    "public orderbook and trade streams only",
                ],
                "metric_definitions": [
                    "coverage_seconds: last local receive timestamp minus first",
                    "clock_mixed lag: local wall-clock receive time minus "
                    "exchange event timestamp; includes clock offset",
                    "spread_bps: (best ask - best bid) / mid × 10,000",
                ],
            },
        },
        {
            "id": "public_trade_sample",
            "label": "공개 시장 체결 표본",
            "path": "artifacts/hft/live-krw-btc-20260719-60s.trades.csv",
            "query": {
                "engine": "sqlite",
                "language": "sql",
                "executed_at": generated_at,
                "description": "검토된 공개 시장 체결 중 시간순 첫 10건입니다.",
                "sql": dataset_sql["public_trade_sample"],
                "tables_used": [
                    "artifacts/hft/live-krw-btc-20260719-60s.trades.csv"
                ],
                "filters": [
                    "first 10 public KRW-BTC trade events in capture order",
                    "own_execution = false",
                ],
                "metric_definitions": [
                    "aggressor_side maps Upbit BID to buyer-initiated and ASK "
                    "to seller-initiated."
                ],
            },
        },
        {
            "id": "simulation_summary",
            "label": "결정론적 합성 HFT 시나리오",
            "path": "artifacts/hft/simulation/simulation-summary.json",
            "query": {
                "engine": "sqlite",
                "language": "sql",
                "executed_at": generated_at,
                "description": (
                    "동일 seed의 합성 이벤트를 long-only taker 재생하고 "
                    "기본·2×비용·지연 시나리오를 비교합니다."
                ),
                "sql": dataset_sql["simulation_metrics"],
                "tables_used": [
                    "artifacts/hft/simulation/simulation-summary.json"
                ],
                "filters": [
                    "10,000 events at 100ms synthetic spacing",
                    "initial cash 10,000,000 KRW",
                    "order notional 250,000 KRW",
                    "buy at ask and sell at bid",
                    "no maker fills and no annualization",
                ],
                "metric_definitions": [
                    "net_return: ending synthetic equity / initial cash - 1",
                    "mid_gross_edge_bps_per_trade: quote PnL plus estimated "
                    "spread cost, divided by entry notionals",
                    "cost_hurdle_bps_per_trade: fees plus estimated spread cost, "
                    "divided by entry notionals",
                ],
            },
        },
        {
            "id": "simulation_equity",
            "label": "합성 HFT 자산곡선",
            "path": "artifacts/hft/simulation/simulation-equity.csv",
        },
        {
            "id": "simulation_executions",
            "label": "합성 HFT 모의체결 원장",
            "path": "artifacts/hft/simulation/simulated-executions.csv",
            "query": {
                "engine": "sqlite",
                "language": "sql",
                "executed_at": generated_at,
                "description": (
                    "약한 알파 기본 시나리오의 검토된 첫 12개 모의 왕복체결입니다."
                ),
                "sql": dataset_sql["simulated_execution_sample"],
                "tables_used": [
                    "artifacts/hft/simulation/simulated-executions.csv"
                ],
                "filters": [
                    "scenario = weak_alpha_base",
                    "first 12 round trips",
                    "simulated = true",
                ],
                "metric_definitions": [
                    "net_pnl_krw is quote-price gross PnL less both taker fees."
                ],
            },
        },
        {
            "id": "development_sequence",
            "label": "HFT 개발 순서와 승격 gate",
            "path": "artifacts/hft-report/analysis-method.md",
            "query": {
                "engine": "sqlite",
                "language": "sql",
                "executed_at": generated_at,
                "description": "제안된 HFT 개발 단계와 각 단계의 승격 gate입니다.",
                "sql": dataset_sql["development_sequence"],
                "tables_used": [
                    "artifacts/hft-report/analysis-method.md"
                ],
                "filters": ["ordered from prerequisite to live canary"],
                "metric_definitions": [
                    "minimum_data and pass_gate are proposed operating gates, "
                    "not observed performance."
                ],
            },
        },
        {
            "id": "hourly_assessment",
            "label": "기존 60분 전략 연구 평가",
            "path": "artifacts/research-assessment.json",
        },
        {
            "id": "upbit_orderbook",
            "label": "Upbit WebSocket 호가 명세",
            "href": "https://docs.upbit.com/kr/reference/websocket-orderbook",
        },
        {
            "id": "upbit_trade",
            "label": "Upbit WebSocket 체결 명세",
            "href": "https://docs.upbit.com/kr/reference/websocket-trade",
        },
        {
            "id": "upbit_limits",
            "label": "Upbit 요청 수 제한",
            "href": "https://docs.upbit.com/kr/reference/rate-limits",
        },
        {
            "id": "upbit_order",
            "label": "Upbit 주문 생성 명세",
            "href": "https://docs.upbit.com/kr/kr/reference/new-order",
        },
    ]


def build_artifact() -> dict[str, Any]:
    generated_at = datetime.now(UTC).isoformat()
    candle = _candle_inventory()
    live, raw_live_quality_rows, raw_trade_sample = _capture_rows()
    study, raw_scenario_rows, equity_rows, execution_rows = _simulation_rows()
    scenario_rows = _materialize_rows_query(raw_scenario_rows)
    live_quality_rows = _materialize_rows_query(raw_live_quality_rows)
    trade_sample = _materialize_rows_query(raw_trade_sample)
    execution_rows = _materialize_rows_query(execution_rows)
    development_rows = _materialize_rows_query(_development_rows())
    inventory_rows = _materialize_rows_query(
        _inventory_rows(candle, live, scenario_rows)
    )
    assessment = _load_json(ASSESSMENT)
    holdout = assessment["phases"]["HOLDOUT_CONFIRMATION"]["metrics"]
    dataset_sql = {
        "data_inventory": _rows_sql(inventory_rows),
        "live_quality": _rows_sql(live_quality_rows),
        "public_trade_sample": _rows_sql(trade_sample),
        "simulation_metrics": _rows_sql(scenario_rows),
        "simulated_execution_sample": _rows_sql(execution_rows),
        "development_sequence": _rows_sql(development_rows),
    }
    sources = _build_sources(generated_at, dataset_sql)
    simulation_source = next(
        source for source in sources if source["id"] == "simulation_summary"
    )
    scenario_by_name = {
        row["scenario"]: row for row in scenario_rows
    }
    weak_row = scenario_by_name["weak_alpha_base"]
    latency_row = scenario_by_name["weak_alpha_latency_5"]
    title = "CoinPilot HFT 접근성·시뮬레이션 보고서"

    manifest = {
        "version": 1,
        "surface": "report",
        "title": title,
        "description": (
            "현재 데이터 이력, 공개 호가·체결 품질, 보수적 HFT 재생 결과와 "
            "필수 개발 순서를 구분해 제시합니다."
        ),
        "generatedAt": generated_at,
        "charts": [
            {
                "id": "scenario_return",
                "title": "합성 HFT 시나리오별 이벤트 구간 순수익률",
                "subtitle": (
                    "약한 합성 엣지는 기본 비용도 넘지 못했고, 4bp/이벤트 "
                    "강한 가정에서만 소폭 양수였습니다."
                ),
                "type": "bar",
                "dataset": "simulation_metrics",
                "sourceId": "simulation_summary",
                "source": simulation_source,
                "valueFormat": "percent",
                "encodings": {
                    "x": {
                        "field": "scenario_label",
                        "type": "nominal",
                        "label": "시나리오",
                    },
                    "y": {
                        "field": "net_return",
                        "type": "quantitative",
                        "label": "순수익률",
                        "format": "percent",
                    },
                    "tooltip": [
                        {
                            "field": "net_pnl_krw",
                            "type": "quantitative",
                            "label": "순손익",
                            "format": "currency",
                        },
                        {
                            "field": "cost_hurdle_bps_per_trade",
                            "type": "quantitative",
                            "label": "거래당 비용 허들",
                        },
                        {
                            "field": "trade_count",
                            "type": "quantitative",
                            "label": "왕복거래",
                        },
                    ],
                },
                "layout": "full",
            },
        ],
        "tables": [
            {
                "id": "data_inventory",
                "title": "현재 데이터·체결 이력",
                "subtitle": (
                    "공개 시장 체결과 내 계좌 체결, 모의체결을 서로 다른 "
                    "증거로 분리했습니다."
                ),
                "dataset": "data_inventory",
                "sourceId": "data_inventory",
                "defaultSort": {"field": "sort_order", "direction": "asc"},
                "columns": [
                    {"field": "sort_order", "label": "순서", "format": "number"},
                    {"field": "dataset", "label": "데이터", "type": "text"},
                    {
                        "field": "source_kind",
                        "label": "출처 구분",
                        "type": "text",
                    },
                    {"field": "rows", "label": "행", "format": "number"},
                    {
                        "field": "execution_count",
                        "label": "내/모의 체결",
                        "format": "number",
                    },
                    {
                        "field": "quality_note",
                        "label": "품질·한계",
                        "type": "text",
                    },
                ],
            },
            {
                "id": "live_quality",
                "title": "KRW-BTC 공개 WebSocket 60초 품질",
                "subtitle": (
                    "파싱 오류는 없었지만 한 번의 짧은 표본은 시장 국면과 "
                    "수익성을 대표하지 않습니다."
                ),
                "dataset": "live_quality",
                "sourceId": "capture_quality",
                "defaultSort": {"field": "sort_order", "direction": "asc"},
                "columns": [
                    {"field": "sort_order", "label": "순서", "format": "number"},
                    {"field": "metric", "label": "지표", "type": "text"},
                    {"field": "result", "label": "결과", "type": "text"},
                    {
                        "field": "interpretation",
                        "label": "해석",
                        "type": "text",
                    },
                ],
            },
            {
                "id": "public_trade_sample",
                "title": "공개 시장 체결 표본 10건",
                "subtitle": (
                    "시장 전체의 공개 체결이며 CoinPilot 계좌의 주문 체결은 "
                    "아닙니다."
                ),
                "dataset": "public_trade_sample",
                "sourceId": "public_trade_sample",
                "defaultSort": {
                    "field": "exchange_time_utc",
                    "direction": "asc",
                },
                "columns": [
                    {
                        "field": "exchange_time_utc",
                        "label": "거래소 시각 UTC",
                        "type": "text",
                    },
                    {
                        "field": "aggressor_side",
                        "label": "공격 방향",
                        "type": "text",
                    },
                    {
                        "field": "trade_price_krw",
                        "label": "체결가",
                        "format": "currency",
                    },
                    {
                        "field": "trade_volume_btc",
                        "label": "체결량",
                        "format": "number",
                        "unit": "BTC",
                    },
                    {
                        "field": "best_bid_ask_krw",
                        "label": "최우선 bid / ask",
                        "type": "text",
                    },
                    {
                        "field": "sequence_id",
                        "label": "체결 ID",
                        "type": "text",
                    },
                ],
            },
            {
                "id": "simulation_metrics",
                "title": "합성 HFT 시나리오 결과",
                "subtitle": (
                    "10,000개 합성 이벤트의 단기 결과이며 연환산하거나 "
                    "실시장 수익으로 해석할 수 없습니다."
                ),
                "dataset": "simulation_metrics",
                "sourceId": "simulation_summary",
                "defaultSort": {"field": "sort_order", "direction": "asc"},
                "columns": [
                    {"field": "sort_order", "label": "순서", "format": "number"},
                    {
                        "field": "scenario_label",
                        "label": "시나리오",
                        "type": "text",
                    },
                    {
                        "field": "alpha_bps_per_event",
                        "label": "합성 알파",
                        "format": "number",
                        "unit": "bp/이벤트",
                    },
                    {
                        "field": "net_return",
                        "label": "이벤트 구간 순수익률",
                        "format": "percent",
                        "movement": True,
                    },
                    {
                        "field": "edge_hurdle_display",
                        "label": "gross edge / 비용 허들",
                        "type": "text",
                    },
                    {
                        "field": "trade_count",
                        "label": "왕복거래",
                        "format": "number",
                    },
                    {
                        "field": "avg_net_pnl_per_trade",
                        "label": "거래당 순손익",
                        "format": "currency",
                        "movement": True,
                    },
                    {
                        "field": "profit_factor",
                        "label": "Profit factor",
                        "format": "number",
                    },
                ],
            },
            {
                "id": "simulated_execution_sample",
                "title": "약한 알파 기본 시나리오 모의체결 12건",
                "subtitle": "모든 매수는 ask, 매도는 bid에서 체결한 것으로 계산했습니다.",
                "dataset": "simulated_execution_sample",
                "sourceId": "simulation_executions",
                "defaultSort": {"field": "trade_index", "direction": "asc"},
                "columns": [
                    {
                        "field": "trade_index",
                        "label": "거래",
                        "format": "number",
                    },
                    {
                        "field": "fill_path",
                        "label": "진입 → 청산 seq",
                        "type": "text",
                    },
                    {
                        "field": "price_path",
                        "label": "ask 매수 → bid 매도",
                        "type": "text",
                    },
                    {
                        "field": "fees_krw",
                        "label": "수수료",
                        "format": "currency",
                    },
                    {
                        "field": "net_pnl_krw",
                        "label": "순손익",
                        "format": "currency",
                        "movement": True,
                    },
                    {
                        "field": "holding_steps",
                        "label": "보유",
                        "format": "number",
                        "unit": "이벤트",
                    },
                    {
                        "field": "record_kind",
                        "label": "구분",
                        "type": "text",
                    },
                ],
            },
            {
                "id": "development_sequence",
                "title": "필수 개발 순서와 승격 기준",
                "subtitle": (
                    "maker 전략과 live 주문은 연속 데이터·보수적 replay "
                    "gate 뒤로 배치했습니다."
                ),
                "dataset": "development_sequence",
                "sourceId": "development_sequence",
                "defaultSort": {"field": "order", "direction": "asc"},
                "columns": [
                    {"field": "order", "label": "순서", "format": "number"},
                    {"field": "stage", "label": "단계", "type": "text"},
                    {
                        "field": "pass_gate",
                        "label": "통과 기준",
                        "type": "text",
                    },
                    {
                        "field": "current_status",
                        "label": "현재",
                        "type": "text",
                    },
                ],
            },
        ],
        "sources": sources,
        "blocks": [
            {"id": "title", "type": "markdown", "body": f"# {title}"},
            {
                "id": "technical_summary",
                "type": "markdown",
                "body": (
                    "## Technical Summary\n\n"
                    "- **결론: 지금은 HFT 수익 전략이 아니라 HFT 데이터·실행 "
                    "연구 트랙을 시작할 단계입니다.** 실제 주문 체결은 0건이고, "
                    "과거 호가·틱 이력도 이번 60초 표본 이전에는 없습니다.\n"
                    "- 60초 공개 표본은 597건(호가 477, 공개 체결 120)이었고 "
                    "파싱 오류·중복·타임스탬프 역행·비정상 스프레드는 0건이었습니다. "
                    "이는 수집기 스모크 테스트 통과일 뿐 수익성 증거가 아닙니다.\n"
                    "- 약한 합성 엣지의 taker 재생은 -1.081%, 동일 결정의 "
                    "수수료 2배는 -2.106%였습니다. 거래당 mid 기준 gross edge "
                    f"{weak_row['mid_gross_edge_bps_per_trade']:.2f}bp가 "
                    f"비용 허들 {weak_row['cost_hurdle_bps_per_trade']:.2f}bp에 "
                    "크게 못 미쳤습니다.\n"
                    "- 기존 365일 시간봉 holdout도 "
                    f"{holdout['annualized_return']:.2%}, PF "
                    f"{holdout['profit_factor']:.3f}로 20% 목표 gate를 실패했습니다. "
                    "HFT가 이를 자동으로 보완한다는 증거는 없습니다."
                ),
            },
            {
                "id": "history_finding",
                "type": "markdown",
                "body": (
                    "## 현재 이력은 시간봉 중심이고 실제 체결은 없다\n\n"
                    f"로컬 DB에는 KRW-BTC 60분봉 {candle['rows']:,}개와 paper "
                    f"이벤트 {sum(int(row[1]) for row in candle['paper'])}건이 "
                    "있지만, paper 이벤트는 모두 계좌 초기화입니다. 기존 백테스트 "
                    "거래 CSV도 `SimulatedBroker` 결과이며 거래소 체결이 아닙니다. "
                    "따라서 지금 보여줄 수 있는 체결은 ① 공개 시장 전체의 실측 "
                    "체결과 ② 명시적으로 표시한 합성 모의체결뿐입니다."
                ),
            },
            {
                "id": "inventory_table",
                "type": "table",
                "tableId": "data_inventory",
                "layout": "full",
            },
            {
                "id": "live_finding",
                "type": "markdown",
                "sourceId": "capture_quality",
                "body": (
                    "## 60초 실측은 수집 가능성만 확인했다\n\n"
                    f"표본 구간은 {live['first_exchange_utc']}부터 "
                    f"{live['last_exchange_utc']}까지입니다. 전체 이벤트율은 "
                    f"{live['overall_event_rate_hz']:.2f}Hz, 공개 체결률은 "
                    f"{live['trade_event_rate_hz']:.2f}Hz였고, 중앙 스프레드는 "
                    f"{live['median_spread_bps']:.3f}bp, p95는 "
                    f"{live['p95_spread_bps']:.3f}bp였습니다. 매수 25만원을 "
                    "최우선 ask 한 단계에서 전량 소화하지 못하는 스냅샷은 "
                    f"{live['l1_insufficient_for_250k_share']:.1%}였습니다.\n\n"
                    f"`received−exchange` p50은 "
                    f"{live['receive_minus_exchange_lag_ms_p50']:.1f}ms로 "
                    "음수입니다. 이는 네트워크가 음의 지연이라는 뜻이 아니라 "
                    "로컬 시계와 거래소 타임스탬프 기준 차이가 섞였다는 경고입니다. "
                    "NTP 오프셋을 따로 기록하기 전에는 latency 모델 보정에 쓰면 "
                    "안 됩니다."
                ),
            },
            {
                "id": "live_quality_table",
                "type": "table",
                "tableId": "live_quality",
                "layout": "full",
            },
            {
                "id": "public_trade_table",
                "type": "table",
                "tableId": "public_trade_sample",
                "layout": "full",
            },
            {
                "id": "simulation_finding",
                "type": "markdown",
                "sourceId": "simulation_summary",
                "body": (
                    "## 합성 HFT에서도 비용이 신호보다 컸다\n\n"
                    "10,000개 이벤트를 100ms 간격으로 생성하고, 현재 이벤트의 "
                    "L1 불균형과 signed flow만으로 신호를 만들었습니다. 결정 뒤 "
                    "1 이벤트 후 매수는 ask, 매도는 bid에서 전량 체결하고 양방향 "
                    "수수료를 반영했습니다. 가격 방향은 불리하게 잡았지만 합성 "
                    "L1에서 전량 체결되고 시장충격이 없다는 유동성 가정은 "
                    "낙관적입니다.\n\n"
                    "무알파는 -1.133%, 약한 0.18bp/이벤트 알파는 -1.081%였습니다. "
                    "동일 약한 이벤트와 동일 결정을 쓰면서 수수료만 2배로 하자 "
                    "-2.106%가 됐습니다. 4bp/이벤트라는 매우 강한 합성 관계에서야 "
                    "+0.032%로 간신히 양수가 됐습니다. 이 강한 케이스는 필요한 "
                    "허들의 크기를 보여주는 장치일 뿐 실제로 발견한 알파가 아닙니다.\n\n"
                    f"지연 5 이벤트의 총손실 {latency_row['net_return']:.3%}가 "
                    "작아 보이는 것은 왕복거래가 "
                    f"{weak_row['trade_count']}건에서 {latency_row['trade_count']}건으로 "
                    "줄었기 때문입니다. 거래당 손실은 "
                    f"{weak_row['avg_net_pnl_per_trade']:,.2f}원에서 "
                    f"{latency_row['avg_net_pnl_per_trade']:,.2f}원으로 오히려 "
                    "악화했습니다."
                ),
            },
            {
                "id": "scenario_return_chart",
                "type": "chart",
                "chartId": "scenario_return",
                "layout": "full",
            },
            {
                "id": "simulation_table",
                "type": "table",
                "tableId": "simulation_metrics",
                "layout": "full",
            },
            {
                "id": "execution_finding",
                "type": "markdown",
                "sourceId": "simulation_executions",
                "body": (
                    "## 모의체결 원장은 체결 가정을 감사하기 위한 것이다\n\n"
                    "원장에는 신호 시퀀스, 지연 뒤 체결 시퀀스, ask 매수가, bid "
                    "매도가, 양방향 수수료, 순손익과 청산 이유를 남겼습니다. "
                    "공개 집계 호가에는 개별 주문 ID와 queue position이 없으므로 "
                    "첫 버전은 maker 체결을 가정하지 않았습니다. 아래 행은 모두 "
                    "`모의체결`이며 거래소 계좌 내역이 아닙니다."
                ),
            },
            {
                "id": "execution_table",
                "type": "table",
                "tableId": "simulated_execution_sample",
                "layout": "full",
            },
            {
                "id": "methodology",
                "type": "markdown",
                "body": (
                    "## 방법론과 실행 모델\n\n"
                    "신호는 `0.70 × L1 호가 불균형 + 0.30 × signed trade flow`이며 "
                    "현재 이벤트 필드만 사용합니다. 진입 기준은 0.45, 청산 기준은 "
                    "0.0, 최대 보유는 25 이벤트입니다. 기본 주문 금액은 25만원, "
                    "초기자금은 1,000만원, 수수료 가정은 편도 0.05%입니다. "
                    "스프레드는 호가 체결가에 직접 반영하고 별도로 추정 비용을 "
                    "표시했습니다. 수수료 2배는 신호·결정·이벤트 해시를 그대로 "
                    "재사용합니다. 현재는 depth sweep·부분체결·impact가 없어 "
                    "실측 replay로 승격하기 전 반드시 보강해야 합니다."
                ),
            },
            {
                "id": "development_finding",
                "type": "markdown",
                "body": (
                    "## 필수 개발 순서는 수집 → 품질 → 재생 → shadow다\n\n"
                    "다음 단계는 더 복잡한 AI가 아니라 연속 원시 데이터입니다. "
                    "maker 전략은 공개 L2만으로 queue 선순위를 알 수 없어 뒤로 "
                    "미뤄야 합니다. 최소 30일의 연속 수집과 여러 변동성 국면을 "
                    "확보한 뒤, 시간 분리 walk-forward와 2×비용을 통과하고, "
                    "주문을 전송하지 않는 shadow paper에서 운영 오류 0건을 "
                    "확인해야 합니다."
                ),
            },
            {
                "id": "development_table",
                "type": "table",
                "tableId": "development_sequence",
                "layout": "full",
            },
            {
                "id": "venue_constraints",
                "type": "markdown",
                "body": (
                    "## 현실적인 접근은 초고속 HFT보다 저지연 이벤트 트레이딩이다\n\n"
                    "Upbit 공개 WebSocket은 최대 30단계의 가격·잔량과 체결 ID를 "
                    "제공하지만 개별 주문 queue는 제공하지 않습니다. 주문 API는 "
                    "IOC/FOK/post-only와 취소 후 재주문을 지원하더라도, 인터넷 "
                    "구간·Python 런타임·거래소 rate limit 환경에서 마이크로초 "
                    "경쟁형 HFT를 기대하기는 어렵습니다. 이 프로젝트의 현실적 "
                    "범위는 수초~수분의 저지연 이벤트 전략, 체결 비용 최소화, "
                    "shadow execution입니다."
                ),
            },
            {
                "id": "promotion_gates",
                "type": "markdown",
                "body": (
                    "## 20% 목표와 실거래 승격 기준\n\n"
                    "연 20%는 목표 gate로 유지하되 이번 합성 이벤트 수익률에는 "
                    "적용하지 않습니다. 실제 승격은 ① 사전등록된 OOS에서 기본·"
                    "2×비용 모두 PnL>0, PF≥1.15, MDD≤15%, ② 30일·1,000회 이상 "
                    "shadow 왕복거래 중 더 긴 조건에서 비용 후 PnL>0, ③ 중복 "
                    "체결·음수 잔고·원장 불일치·리스크 위반 0건을 요구합니다. "
                    "통과 전에는 `live` hard lock을 유지합니다."
                ),
            },
            {
                "id": "limitations",
                "type": "markdown",
                "body": (
                    "## 한계와 신뢰도\n\n"
                    "- **높은 신뢰:** 공개 메시지 스키마 파싱, 60초 표본의 행 수와 "
                    "품질 카운트, 합성 replay의 결정론적 회계.\n"
                    "- **낮은 신뢰:** 60초 표본이 장기 스프레드·체결 강도를 대표한다는 "
                    "가정, 합성 알파 크기와 실제 시장의 관계.\n"
                    "- **평가 불가:** 실제 maker fill rate, queue priority, 시장충격, "
                    "장기 HFT OOS 수익률, 연 20% 달성 가능성.\n"
                    "- 공개 체결 ID는 JavaScript 안전 정수 범위를 넘으므로 보고서 "
                    "표에서는 문자열로 보존했습니다. 다른 소비자도 lossless 정수 "
                    "파서를 사용해야 합니다.\n"
                    "- 공개 시장 체결은 내 주문 체결이 아니며, 합성 모의체결은 "
                    "실제 체결 이력으로 사용할 수 없습니다."
                ),
            },
            {
                "id": "further_questions",
                "type": "markdown",
                "body": (
                    "## 다음 연구 질문\n\n"
                    "- 실측 30일에서 L1/L5 불균형과 signed flow가 100ms~60초 "
                    "미래 mid-return을 비용 후에도 예측하는가?\n"
                    "- 시간대·변동성·스프레드 분위별로 신호와 비용 허들이 어떻게 "
                    "달라지는가?\n"
                    "- depth sweep과 보수적 impact를 넣어도 기본·2×비용 OOS가 "
                    "양수인가?\n"
                    "- private myOrder shadow 기록으로 maker queue 모델을 "
                    "실측 보정할 수 있는가?"
                ),
            },
        ],
    }
    snapshot = {
        "version": 1,
        "status": "ready",
        "generatedAt": generated_at,
        "datasets": {
            "data_inventory": inventory_rows,
            "live_quality": live_quality_rows,
            "public_trade_sample": trade_sample,
            "simulation_metrics": scenario_rows,
            "simulation_equity": equity_rows,
            "simulated_execution_sample": execution_rows,
            "development_sequence": development_rows,
        },
    }
    return {
        "surface": "report",
        "manifest": manifest,
        "snapshot": snapshot,
        "sources": sources,
    }


def _method_text(artifact: dict[str, Any]) -> str:
    snapshot = artifact["snapshot"]
    return f"""# HFT report reproducibility notes

Generated at: {snapshot["generatedAt"]}

## Inputs

- `var/coinpilot.db`: KRW-BTC 60-minute candles and paper ledger.
- `artifacts/research-assessment.json`: frozen 365-day hourly-strategy assessment.
- `artifacts/hft/live-krw-btc-20260719-60s.jsonl`: normalized public Upbit
  orderbook/trade capture.
- `artifacts/hft/live-krw-btc-20260719-60s.quality.json`: validated capture
  quality profile and SHA-256.
- `artifacts/hft/simulation/simulation-summary.json`: deterministic synthetic
  replay results.
- `artifacts/hft/simulation/simulated-executions.csv`: simulated fills only.

## Quality contract

The public feed must parse under schema version 1, retain exchange and local
receive clocks separately, keep trade sequence IDs, and count invalid spreads,
duplicates, and timestamp regressions. `received_at - exchange_timestamp` is
reported as clock-mixed lag; it is not treated as pure network latency.

## Simulation contract

All decisions use the current event only. With `latency_steps=1`, a decision at
event `i` fills at event `i+1`; buys cross at best ask and sells cross at best
bid. The cost-stress case reuses the exact weak-alpha event hash and decisions.
Every execution row is labeled `simulated=true`.

## Cost decomposition

`mid gross PnL = quote-price gross PnL + estimated half-spread costs on both
sides`.

`cost hurdle = taker fees + estimated half-spread costs on both sides`.

The per-trade basis-point values divide those amounts by total entry notionals.
Synthetic event-window returns are not annualized.
"""


def main() -> None:
    required = [
        CAPTURE,
        QUALITY,
        PUBLIC_TRADES,
        SIMULATION,
        SIM_EXECUTIONS,
        SIM_EQUITY,
        ASSESSMENT,
        DATABASE,
    ]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise SystemExit("Missing required inputs: " + ", ".join(missing))
    artifact = build_artifact()
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    METHOD.write_text(_method_text(artifact), encoding="utf-8")
    ARTIFACT.write_text(
        json.dumps(
            artifact,
            allow_nan=False,
            ensure_ascii=False,
            indent=2,
            sort_keys=False,
        )
        + "\n",
        encoding="utf-8",
    )
    print(ARTIFACT)


if __name__ == "__main__":
    main()
