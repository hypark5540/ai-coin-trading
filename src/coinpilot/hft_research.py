"""Reproducible HFT research runs and artifact writers.

The public market-data capture and the synthetic replay are intentionally
separate.  Captured Upbit rows are observations; generated rows and every fill
from :mod:`coinpilot.hft_sim` are simulations.  This module preserves that
provenance in every emitted artifact and never sends an order.
"""

from __future__ import annotations

import csv
import dataclasses
import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

from coinpilot.hft_data import (
    HFTDataQualityProfile,
    profile_hft_jsonl,
    read_hft_jsonl,
)
from coinpilot.hft_sim import (
    HFTSimulationConfig,
    HFTSimulationResult,
    MicrostructureEvent,
    generate_synthetic_microstructure,
    simulate_hft_replay,
)


HFT_STUDY_SCHEMA_VERSION = 1
_HFT_STUDY_ARTIFACT_FILENAMES = {
    "summary": "simulation-summary.json",
    "simulated_executions": "simulated-executions.csv",
    "decisions": "simulation-decisions.csv",
    "equity": "simulation-equity.csv",
}


@dataclass(frozen=True, slots=True)
class HFTScenarioResult:
    name: str
    description: str
    alpha_bps_per_event: float
    event_sha256: str
    result: HFTSimulationResult

    def summary_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "alpha_bps_per_event": self.alpha_bps_per_event,
            "event_sha256": self.event_sha256,
            "metrics": dict(self.result.metrics),
        }


@dataclass(frozen=True, slots=True)
class HFTStudyRun:
    created_at: str
    event_count: int
    seed: int
    interval_ns: int
    base_spread_bps: float
    volatility_bps: float
    weak_alpha_bps: float
    illustrative_strong_alpha_bps: float
    scenarios: tuple[HFTScenarioResult, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": HFT_STUDY_SCHEMA_VERSION,
            "created_at": self.created_at,
            "source_kind": "synthetic",
            "purpose": (
                "Mechanics and cost/latency sensitivity only; not evidence of "
                "historical or future HFT profitability."
            ),
            "generator": {
                "event_count": self.event_count,
                "seed": self.seed,
                "interval_ns": self.interval_ns,
                "base_spread_bps": self.base_spread_bps,
                "volatility_bps": self.volatility_bps,
                "weak_alpha_bps": self.weak_alpha_bps,
                "illustrative_strong_alpha_bps": (
                    self.illustrative_strong_alpha_bps
                ),
            },
            "scenarios": [scenario.summary_dict() for scenario in self.scenarios],
            "warnings": [
                "All executions in this study are simulated taker fills.",
                "Synthetic event returns must not be annualized.",
                "The illustrative strong-alpha case is a hurdle demonstration, "
                "not an estimate of an obtainable signal.",
            ],
        }


def _canonical_event_hash(events: Iterable[MicrostructureEvent]) -> str:
    digest = hashlib.sha256()
    for event in events:
        row = dataclasses.asdict(event)
        digest.update(
            json.dumps(
                row,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        )
        digest.update(b"\n")
    return digest.hexdigest()


def file_sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run_hft_scenario_suite(
    *,
    event_count: int = 10_000,
    seed: int = 23,
    base_config: HFTSimulationConfig | None = None,
    interval_ns: int = 100_000_000,
    volatility_bps: float = 0.80,
    base_spread_bps: float = 1.20,
    weak_alpha_bps: float = 0.18,
    illustrative_strong_alpha_bps: float = 4.0,
) -> HFTStudyRun:
    """Run deterministic null, weak-edge, cost, latency, and hurdle cases."""

    config = (base_config or HFTSimulationConfig()).validate()
    null_events = generate_synthetic_microstructure(
        event_count,
        seed=seed,
        scenario="null_alpha",
        interval_ns=interval_ns,
        volatility_bps=volatility_bps,
        base_spread_bps=base_spread_bps,
        weak_alpha_bps=0.0,
    )
    weak_events = generate_synthetic_microstructure(
        event_count,
        seed=seed,
        scenario="weak_alpha",
        interval_ns=interval_ns,
        volatility_bps=volatility_bps,
        base_spread_bps=base_spread_bps,
        weak_alpha_bps=weak_alpha_bps,
    )
    strong_events = generate_synthetic_microstructure(
        event_count,
        seed=seed,
        scenario="weak_alpha",
        interval_ns=interval_ns,
        volatility_bps=volatility_bps,
        base_spread_bps=base_spread_bps,
        weak_alpha_bps=illustrative_strong_alpha_bps,
    )

    weak_base = simulate_hft_replay(weak_events, config)
    weak_cost_2x = simulate_hft_replay(
        weak_events,
        dataclasses.replace(
            config,
            taker_fee_rate=config.taker_fee_rate * 2.0,
        ),
    )
    if weak_base.decisions != weak_cost_2x.decisions:
        raise RuntimeError(
            "Cost stress must reuse the exact same weak-alpha decisions"
        )

    latency_steps = max(5, config.latency_steps)
    scenarios = (
        HFTScenarioResult(
            name="null_alpha_base",
            description="예측 엣지 0, 기본 taker 비용",
            alpha_bps_per_event=0.0,
            event_sha256=_canonical_event_hash(null_events),
            result=simulate_hft_replay(null_events, config),
        ),
        HFTScenarioResult(
            name="weak_alpha_base",
            description="이벤트당 약한 합성 엣지, 기본 taker 비용",
            alpha_bps_per_event=weak_alpha_bps,
            event_sha256=_canonical_event_hash(weak_events),
            result=weak_base,
        ),
        HFTScenarioResult(
            name="weak_alpha_cost_2x",
            description="동일 약한 엣지·동일 결정, taker 수수료 2배",
            alpha_bps_per_event=weak_alpha_bps,
            event_sha256=_canonical_event_hash(weak_events),
            result=weak_cost_2x,
        ),
        HFTScenarioResult(
            name="weak_alpha_latency_5",
            description="동일 약한 엣지, 주문 지연 5 이벤트",
            alpha_bps_per_event=weak_alpha_bps,
            event_sha256=_canonical_event_hash(weak_events),
            result=simulate_hft_replay(
                weak_events,
                dataclasses.replace(config, latency_steps=latency_steps),
            ),
        ),
        HFTScenarioResult(
            name="illustrative_strong_alpha",
            description="손익분기 감각을 위한 강한 합성 엣지 예시",
            alpha_bps_per_event=illustrative_strong_alpha_bps,
            event_sha256=_canonical_event_hash(strong_events),
            result=simulate_hft_replay(strong_events, config),
        ),
    )
    return HFTStudyRun(
        created_at=datetime.now(UTC).isoformat(),
        event_count=event_count,
        seed=seed,
        interval_ns=interval_ns,
        base_spread_bps=base_spread_bps,
        volatility_bps=volatility_bps,
        weak_alpha_bps=weak_alpha_bps,
        illustrative_strong_alpha_bps=illustrative_strong_alpha_bps,
        scenarios=scenarios,
    )


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(
                value,
                handle,
                allow_nan=False,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    except Exception:
        Path(temporary_name).unlink(missing_ok=True)
        raise


def _atomic_csv(
    path: Path,
    *,
    fieldnames: list[str],
    rows: Iterable[Mapping[str, Any]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    except Exception:
        Path(temporary_name).unlink(missing_ok=True)
        raise


def hft_study_artifact_paths(
    output_dir: str | Path,
) -> dict[str, Path]:
    """Return the fixed member paths for one synthetic HFT study bundle."""

    directory = Path(output_dir)
    return {
        name: directory / filename
        for name, filename in _HFT_STUDY_ARTIFACT_FILENAMES.items()
    }


def write_hft_study_artifacts(
    study: HFTStudyRun,
    output_dir: str | Path,
    *,
    artifact_paths: Mapping[str, str | Path] | None = None,
) -> dict[str, Path]:
    """Write summary, simulated executions, decisions, and equity history."""

    paths = hft_study_artifact_paths(output_dir)
    if artifact_paths is not None:
        if set(artifact_paths) != set(paths):
            raise ValueError(
                "artifact_paths must define summary, simulated_executions, "
                "decisions, and equity"
            )
        paths = {
            name: Path(artifact_paths[name])
            for name in _HFT_STUDY_ARTIFACT_FILENAMES
        }
    if len(set(paths.values())) != len(paths):
        raise ValueError("HFT study artifact paths must be distinct")

    summary_path = paths["summary"]
    executions_path = paths["simulated_executions"]
    decisions_path = paths["decisions"]
    equity_path = paths["equity"]
    _atomic_json(summary_path, study.as_dict())

    execution_rows: list[dict[str, Any]] = []
    decision_rows: list[dict[str, Any]] = []
    equity_rows: list[dict[str, Any]] = []
    for scenario in study.scenarios:
        for trade_index, trade in enumerate(scenario.result.trades, start=1):
            execution_rows.append(
                {
                    "scenario": scenario.name,
                    "trade_index": trade_index,
                    "simulated": True,
                    "source_kind": "synthetic",
                    **dataclasses.asdict(trade),
                }
            )
        for decision_index, decision in enumerate(
            scenario.result.decisions,
            start=1,
        ):
            decision_rows.append(
                {
                    "scenario": scenario.name,
                    "decision_index": decision_index,
                    "simulated": True,
                    **dataclasses.asdict(decision),
                }
            )
        for point in scenario.result.equity_curve:
            equity_rows.append(
                {
                    "scenario": scenario.name,
                    "simulated": True,
                    **dataclasses.asdict(point),
                }
            )

    execution_fields = [
        "scenario",
        "trade_index",
        "simulated",
        "source_kind",
        "entry_decision_sequence",
        "entry_fill_sequence",
        "exit_decision_sequence",
        "exit_fill_sequence",
        "entry_price",
        "exit_price",
        "quantity",
        "entry_notional",
        "exit_notional",
        "gross_pnl_before_fees",
        "fees",
        "estimated_spread_cost",
        "net_pnl",
        "holding_steps",
        "exit_reason",
    ]
    _atomic_csv(
        executions_path,
        fieldnames=execution_fields,
        rows=execution_rows,
    )
    _atomic_csv(
        decisions_path,
        fieldnames=[
            "scenario",
            "decision_index",
            "simulated",
            "action",
            "reason",
            "decision_event_index",
            "decision_sequence",
            "due_event_index",
            "signal",
        ],
        rows=decision_rows,
    )
    _atomic_csv(
        equity_path,
        fieldnames=[
            "scenario",
            "simulated",
            "event_index",
            "sequence",
            "timestamp_ns",
            "equity",
            "cash",
            "quantity",
            "liquidation_price",
            "drawdown",
        ],
        rows=equity_rows,
    )
    return paths


def write_capture_companions(
    capture_path: str | Path,
    *,
    quality: HFTDataQualityProfile | None = None,
    quality_path: str | Path | None = None,
    trades_path: str | Path | None = None,
    reported_source_path: str | Path | None = None,
) -> dict[str, Path]:
    """Write a quality sidecar and an explicit real-trade observation table."""

    source = Path(capture_path)
    selected_quality = quality or profile_hft_jsonl(source)
    selected_quality_path = (
        source.with_suffix(".quality.json")
        if quality_path is None
        else Path(quality_path)
    )
    selected_trades_path = (
        source.with_suffix(".trades.csv")
        if trades_path is None
        else Path(trades_path)
    )
    if len({source, selected_quality_path, selected_trades_path}) != 3:
        raise ValueError("capture and companion artifact paths must be distinct")
    reported_source = (
        source
        if reported_source_path is None
        else Path(reported_source_path)
    )
    _atomic_json(
        selected_quality_path,
        {
            "schema_version": HFT_STUDY_SCHEMA_VERSION,
            "source_kind": "captured_public_feed",
            "source_path": str(reported_source),
            "source_sha256": file_sha256(source),
            "quality": selected_quality.to_dict(),
            "interpretation": (
                "receive_minus_exchange_lag_ms includes host/exchange clock "
                "offset and must not be interpreted as pure network latency."
            ),
        },
    )
    trade_rows = [
        {
            "market": event["market"],
            "exchange_timestamp_ms": event["exchange_timestamp_ms"],
            "received_at_ns": event["received_at_ns"],
            "sequence_id": event["sequence_id"],
            "aggressor_side": event["aggressor_side"],
            "trade_price": event["trade_price"],
            "trade_volume": event["trade_volume"],
            "best_bid_price": event["best_bid_price"],
            "best_ask_price": event["best_ask_price"],
            "observed_public_trade": True,
            "own_execution": False,
        }
        for event in read_hft_jsonl(source)
        if event["event_type"] == "trade"
    ]
    _atomic_csv(
        selected_trades_path,
        fieldnames=[
            "market",
            "exchange_timestamp_ms",
            "received_at_ns",
            "sequence_id",
            "aggressor_side",
            "trade_price",
            "trade_volume",
            "best_bid_price",
            "best_ask_price",
            "observed_public_trade",
            "own_execution",
        ],
        rows=trade_rows,
    )
    return {
        "capture": source,
        "quality": selected_quality_path,
        "public_trades": selected_trades_path,
    }
