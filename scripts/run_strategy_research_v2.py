#!/usr/bin/env python3
"""Run the frozen Strategy Research V2 protocol from cached candles.

This is a research-only, read-only input runner.  It deliberately has no
exchange client and never changes a service, a configuration file, or a shadow
ledger.  All output files are strict/portable and replaced atomically.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import sqlite3
import tempfile
from collections import deque
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from coinpilot.config import AppConfig, load_config
from coinpilot.data import CANDLE_COLUMNS, candle_data_hash, validate_candles
from coinpilot.portfolio_research import (
    FAIL,
    INSUFFICIENT_EVIDENCE,
    PASS,
    CalendarFold,
    PortfolioResearchGates,
    PortfolioResearchResult,
    run_portfolio_research,
)


SCRIPT_SCHEMA_VERSION = 1
EXPECTED_MARKETS = ("KRW-BTC", "KRW-ETH", "KRW-XRP", "KRW-SOL")
DEV_FOLDS = ("D1", "D2")
CONTAMINATED_FOLD = "D3_CONTAMINATED"
FOLDS = (
    CalendarFold("D1", "2024-07-19T07:00:00Z", "2025-01-19T07:00:00Z"),
    CalendarFold("D2", "2025-01-19T07:00:00Z", "2025-07-19T07:00:00Z"),
    CalendarFold(
        CONTAMINATED_FOLD,
        "2025-07-19T07:00:00Z",
        "2026-07-19T07:00:00Z",
    ),
)
CANDIDATE_FILES = {
    "C0_CONTROL_ER72": "c0-control-er72.toml",
    "C1_SLOW_ER168": "c1-slow-er168.toml",
    "C2_TREND_336_168": "c2-trend-336-168.toml",
}
MARKET_FILES = {
    "KRW-BTC": "krw-btc.toml",
    "KRW-ETH": "krw-eth.toml",
    "KRW-XRP": "krw-xrp.toml",
    "KRW-SOL": "krw-sol.toml",
}
ENSEMBLE_ID = "C3_ENSEMBLE_50_50"
COMPONENT_WEIGHTS = {"C1_SLOW_ER168": 0.5, "C2_TREND_336_168": 0.5}
PROTOCOL_THRESHOLDS = {
    "minimum_fold_return": 0.0,
    "minimum_profit_factor": 1.15,
    "maximum_worst_fold_drawdown": 0.10,
    "minimum_nonnegative_market_count": 3,
    "minimum_total_trades": 30,
    "maximum_halted_sleeves": 0,
}
FINGERPRINT_FILES = (
    "pyproject.toml",
    "scripts/run_strategy_research_v2.py",
    "src/coinpilot/backtest.py",
    "src/coinpilot/broker.py",
    "src/coinpilot/config.py",
    "src/coinpilot/data.py",
    "src/coinpilot/engine.py",
    "src/coinpilot/features.py",
    "src/coinpilot/model.py",
    "src/coinpilot/portfolio_research.py",
    "src/coinpilot/risk.py",
    "src/coinpilot/strategy.py",
)
_ABSOLUTE_WINDOWS_PATH = re.compile(r"^[A-Za-z]:[\\/]")


def _json_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        number = float(value)
        return number if math.isfinite(number) else None
    if value is pd.NA:
        return None
    return value


def _canonical_json_bytes(value: Any, *, pretty: bool = False) -> bytes:
    normalized = _json_value(value)
    _assert_portable(normalized)
    options: dict[str, Any] = {
        "allow_nan": False,
        "ensure_ascii": False,
        "sort_keys": True,
    }
    if pretty:
        options["indent"] = 2
    else:
        options["separators"] = (",", ":")
    return (json.dumps(normalized, **options) + "\n").encode("utf-8")


def _canonical_hash(value: Any) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _assert_portable(value: Any, *, location: str = "$") -> None:
    """Reject local paths and unsupported values before they reach artifacts."""

    if isinstance(value, Mapping):
        for key, item in value.items():
            _assert_portable(item, location=f"{location}.{key}")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _assert_portable(item, location=f"{location}[{index}]")
        return
    if isinstance(value, Path):
        raise ValueError(f"Portable artifact contains Path at {location}")
    if isinstance(value, str):
        if value.startswith(("/", "file://")) or _ABSOLUTE_WINDOWS_PATH.match(
            value
        ):
            raise ValueError(
                f"Portable artifact contains an absolute path at {location}"
            )
        return
    if value is None or isinstance(value, (str, int, float, bool)):
        return
    raise TypeError(
        f"Portable artifact contains unsupported {type(value).__name__} "
        f"at {location}"
    )


def _csv_bytes(frame: pd.DataFrame) -> bytes:
    portable = frame.copy()
    for column in portable.columns:
        portable[column] = portable[column].map(
            lambda value: json.dumps(
                _json_value(value),
                allow_nan=False,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            if isinstance(value, (dict, list, tuple))
            else value
        )
    text = portable.to_csv(index=False, lineterminator="\n", na_rep="")
    for value in portable.to_numpy().ravel():
        if isinstance(value, str):
            _assert_portable(value)
    return text.encode("utf-8")


class ArtifactWriter:
    """Collect portable outputs and atomically replace each file at commit."""

    def __init__(self, output_dir: Path) -> None:
        supplied = output_dir.expanduser()
        if supplied.is_symlink():
            raise ValueError("Output directory cannot be a symbolic link")
        self.output_dir = supplied.resolve()
        self._files: dict[str, tuple[bytes, str, str]] = {}

    def add_json(self, name: str, payload: Any, *, role: str) -> None:
        self._add(
            name,
            _canonical_json_bytes(payload, pretty=True),
            "application/json",
            role,
        )

    def add_csv(self, name: str, frame: pd.DataFrame, *, role: str) -> None:
        self._add(name, _csv_bytes(frame), "text/csv", role)

    def _add(self, name: str, content: bytes, content_type: str, role: str) -> None:
        path = Path(name)
        if path.is_absolute() or len(path.parts) != 1 or name in self._files:
            raise ValueError(f"Invalid or duplicate artifact name: {name!r}")
        self._files[name] = (content, content_type, role)

    def commit(self, *, receipt: Mapping[str, Any]) -> dict[str, Any]:
        if self.output_dir.is_symlink():
            raise ValueError("Output directory cannot be a symbolic link")
        self.output_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.output_dir, 0o700)
        entries = [
            {
                "file": name,
                "role": role,
                "content_type": content_type,
                "bytes": len(content),
                "sha256": _sha256(content),
            }
            for name, (content, content_type, role) in sorted(self._files.items())
        ]
        manifest = {
            "artifact_manifest_schema_version": SCRIPT_SCHEMA_VERSION,
            "research_only": True,
            "orders_sent": 0,
            "live_order_routing": False,
            "files": entries,
            "receipt": dict(receipt),
        }
        manifest_bytes = _canonical_json_bytes(manifest, pretty=True)
        for name, (content, _, _) in sorted(self._files.items()):
            self._atomic_replace(name, content)
        self._atomic_replace("manifest.json", manifest_bytes)
        return manifest

    def _atomic_replace(self, name: str, content: bytes) -> None:
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{name}.",
            suffix=".tmp",
            dir=self.output_dir,
        )
        temporary = Path(temporary_name)
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            destination = self.output_dir / name
            if destination.is_symlink():
                raise ValueError(f"Artifact destination cannot be a symlink: {name}")
            os.replace(temporary, destination)
            os.chmod(destination, 0o600)
        finally:
            if temporary.exists():
                temporary.unlink()


def _readonly_connection(path: Path) -> sqlite3.Connection:
    if not path.is_file() or path.is_symlink():
        raise ValueError("SQLite input must be an existing regular non-symlink file")
    connection = sqlite3.connect(
        f"{path.as_uri()}?mode=ro",
        uri=True,
        timeout=30.0,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only = ON")
    return connection


def _load_candles_readonly(config: AppConfig) -> pd.DataFrame:
    path = Path(config.data.database_path)
    with _readonly_connection(path) as connection:
        rows = connection.execute(
            """
            SELECT timestamp, market, open, high, low, close, volume, quote_volume
            FROM (
                SELECT timestamp, market, open, high, low, close, volume,
                       quote_volume
                FROM candles
                WHERE market = ? AND interval_minutes = ?
                ORDER BY timestamp DESC
                LIMIT ?
            )
            ORDER BY timestamp ASC
            """,
            (
                config.data.market,
                config.data.interval_minutes,
                config.data.candle_count,
            ),
        ).fetchall()
    if not rows:
        raise ValueError(f"No cached candles for {config.data.market}")
    frame = pd.DataFrame([dict(row) for row in rows], columns=CANDLE_COLUMNS)
    return validate_candles(frame)


def load_frozen_inputs(
    config_dir: Path,
) -> tuple[dict[str, pd.DataFrame], dict[str, AppConfig]]:
    supplied_directory = config_dir.expanduser()
    if supplied_directory.is_symlink():
        raise ValueError("Frozen config directory cannot be a symbolic link")
    directory = supplied_directory.resolve()
    if not directory.is_dir():
        raise ValueError("Frozen config directory must be a regular directory")

    market_configs: dict[str, AppConfig] = {}
    for expected_market, filename in MARKET_FILES.items():
        config = load_config(directory / filename)
        if config.data.market != expected_market:
            raise ValueError(
                f"{filename} market is {config.data.market}, expected "
                f"{expected_market}"
            )
        if config.data.interval_minutes != 60:
            raise ValueError(f"{filename} must use 60-minute candles")
        market_configs[expected_market] = config
    database_paths = {item.data.database_path for item in market_configs.values()}
    if len(database_paths) != 1:
        raise ValueError("Frozen market configs must share one candle database")

    candles = {
        market: _load_candles_readonly(config)
        for market, config in market_configs.items()
    }
    candidates: dict[str, AppConfig] = {}
    for candidate_id, filename in CANDIDATE_FILES.items():
        config = load_config(directory / filename)
        if config.data.interval_minutes != 60:
            raise ValueError(f"{filename} must use 60-minute candles")
        if config.data.database_path not in database_paths:
            raise ValueError(f"{filename} must use the frozen candle database")
        candidates[candidate_id] = config
    return candles, candidates


def build_data_quality(
    candles_by_market: Mapping[str, pd.DataFrame],
    *,
    interval_minutes: int = 60,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    interval = pd.Timedelta(minutes=interval_minutes)
    for market in EXPECTED_MARKETS:
        frame = candles_by_market[market]
        timestamps = pd.to_datetime(frame["timestamp"], utc=True)
        deltas = timestamps.diff().dropna()
        numeric = frame.loc[
            :, ["open", "high", "low", "close", "volume", "quote_volume"]
        ].apply(pd.to_numeric, errors="coerce")
        nonfinite = ~np.isfinite(numeric.to_numpy(dtype=float))
        invalid_prices = (
            (numeric[["open", "high", "low", "close"]] <= 0).any(axis=1)
            | (numeric["high"] < numeric["low"])
            | (numeric["high"] < numeric[["open", "close"]].max(axis=1))
            | (numeric["low"] > numeric[["open", "close"]].min(axis=1))
        )
        invalid_volumes = (numeric[["volume", "quote_volume"]] < 0).any(axis=1)
        gap_deltas = deltas.loc[deltas > interval]
        estimated_missing = int(
            sum(max(0, int(round(delta / interval)) - 1) for delta in gap_deltas)
        )
        fold_coverage: dict[str, bool] = {}
        for fold in FOLDS:
            start, end = pd.Timestamp(fold.start), pd.Timestamp(fold.end)
            selected = timestamps.loc[timestamps.ge(start) & timestamps.lt(end)]
            fold_coverage[fold.fold_id] = bool(
                len(selected) >= 2
                and selected.iloc[0] < start + interval
                and selected.iloc[-1] + interval >= end
            )
        rows.append(
            {
                "market": market,
                "interval_minutes": interval_minutes,
                "row_count": int(len(frame)),
                "first_timestamp": timestamps.iloc[0].isoformat(),
                "last_timestamp": timestamps.iloc[-1].isoformat(),
                "source_data_sha256": candle_data_hash(frame),
                "duplicate_timestamp_count": int(timestamps.duplicated().sum()),
                "out_of_order_timestamp_count": int((deltas <= pd.Timedelta(0)).sum()),
                "gap_count": int(len(gap_deltas)),
                "estimated_missing_interval_count": estimated_missing,
                "maximum_gap_minutes": (
                    float(gap_deltas.max() / pd.Timedelta(minutes=1))
                    if len(gap_deltas)
                    else 0.0
                ),
                "nonfinite_value_count": int(nonfinite.sum()),
                "invalid_price_row_count": int(invalid_prices.sum()),
                "invalid_volume_row_count": int(invalid_volumes.sum()),
                "invalid_total_count": int(
                    nonfinite.sum() + invalid_prices.sum() + invalid_volumes.sum()
                ),
                "D1_complete": fold_coverage["D1"],
                "D2_complete": fold_coverage["D2"],
                "D3_CONTAMINATED_complete": fold_coverage[CONTAMINATED_FOLD],
            }
        )
    summary = {
        "data_quality_schema_version": SCRIPT_SCHEMA_VERSION,
        "source": "cached_public_ohlcv",
        "network_access": False,
        "market_count": len(rows),
        "all_protocol_windows_complete": all(
            row[f"{fold_id}_complete"]
            for row in rows
            for fold_id in ("D1", "D2", CONTAMINATED_FOLD)
        ),
        "total_invalid_count": int(sum(row["invalid_total_count"] for row in rows)),
        "markets": rows,
        "gap_interpretation": (
            "A missing hourly candle can mean no trades occurred; rolling features "
            "restart at gaps and do not cross them."
        ),
    }
    summary["data_quality_sha256"] = _canonical_hash(summary)
    return summary


def _fold_lookup(rows: Sequence[Mapping[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    return {
        (str(row["candidate_id"]), str(row["fold_id"])): dict(row)
        for row in rows
    }


def _sleeve_lookup(
    rows: Sequence[Mapping[str, Any]],
) -> dict[tuple[str, str, str], dict[str, Any]]:
    return {
        (
            str(row["candidate_id"]),
            str(row["fold_id"]),
            str(row["market"]),
        ): dict(row)
        for row in rows
    }


def _curve_metrics(curve: pd.Series, *, calendar_days: float) -> dict[str, Any]:
    curve = pd.to_numeric(curve, errors="raise")
    final_equity = float(curve.iloc[-1])
    total_return = final_equity - 1.0
    annualized = (
        float(final_equity ** (365.25 / calendar_days) - 1)
        if final_equity > 0 and calendar_days > 0
        else None
    )
    drawdown = curve / curve.cummax() - 1
    daily = curve.resample("1D").last().pct_change(fill_method=None).dropna()
    daily_std = float(daily.std(ddof=0)) if len(daily) else 0.0
    return {
        "final_equity_index": final_equity,
        "total_return": total_return,
        "annualized_return": annualized,
        "max_drawdown": max(0.0, float(-drawdown.min())),
        "sharpe": (
            float(daily.mean() / daily_std * math.sqrt(365.25))
            if daily_std > 0
            else None
        ),
    }


def _ensemble_scaling_evidence(
    candidates: Mapping[str, AppConfig] | None,
) -> dict[str, Any]:
    evidence: dict[str, Any] = {
        "algebraic_equivalence": (
            "mean_market(0.5*C1_market + 0.5*C2_market) equals "
            "0.5*mean_market(C1_market) + 0.5*mean_market(C2_market)"
        ),
        "requires_capital_proportional_position_sizing": True,
        "requires_minimum_order_to_be_non_binding": True,
        "verified_from_frozen_configs": candidates is not None,
        "components": {},
    }
    if candidates is None:
        return evidence
    for candidate_id in COMPONENT_WEIGHTS:
        config = candidates[candidate_id]
        allocated_cash = (
            float(config.risk.initial_cash) * COMPONENT_WEIGHTS[candidate_id]
        )
        risk_sized_floor = (
            allocated_cash
            * float(config.risk.risk_per_trade)
            / float(config.risk.maximum_stop_pct)
        )
        maximum_position = allocated_cash * float(
            config.risk.max_position_fraction
        )
        conservative_order_quote = min(risk_sized_floor, maximum_position)
        non_binding = conservative_order_quote >= float(
            config.risk.minimum_order_quote
        )
        if not non_binding:
            raise ValueError(
                f"{candidate_id} minimum order can bind at the 50% allocation"
            )
        evidence["components"][candidate_id] = {
            "allocated_initial_cash_quote": allocated_cash,
            "conservative_order_quote": conservative_order_quote,
            "minimum_order_quote": float(config.risk.minimum_order_quote),
            "minimum_order_non_binding": non_binding,
        }
    return evidence


def build_c3_ensemble(
    result: PortfolioResearchResult,
    candidates: Mapping[str, AppConfig] | None = None,
) -> dict[str, Any]:
    """Derive the predeclared 50/50 C1+C2 ensemble without fitting again."""

    fold_rows = _fold_lookup(result.fold_results)
    sleeve_rows = _sleeve_lookup(result.sleeve_results)
    equity = pd.DataFrame(result.equity_curves)
    ensemble_curves: list[dict[str, Any]] = []
    ensemble_folds: list[dict[str, Any]] = []
    ensemble_sleeves: list[dict[str, Any]] = []

    for fold in FOLDS:
        calendar_days = float(
            (pd.Timestamp(fold.end) - pd.Timestamp(fold.start))
            / pd.Timedelta(days=1)
        )
        for scenario, prefix in (("base", "base"), ("cost_stress_2x", "cost_stress_2x")):
            components: list[pd.Series] = []
            for candidate_id in COMPONENT_WEIGHTS:
                selected = equity.loc[
                    equity["candidate_id"].eq(candidate_id)
                    & equity["fold_id"].eq(fold.fold_id)
                    & equity["scenario"].eq(scenario),
                    ["timestamp", "equity_index"],
                ].copy()
                if selected.empty:
                    raise ValueError(
                        f"Missing {candidate_id} {fold.fold_id} {scenario} curve"
                    )
                selected["timestamp"] = pd.to_datetime(selected["timestamp"], utc=True)
                if selected["timestamp"].duplicated().any():
                    raise ValueError("Component ensemble curve has duplicate timestamps")
                components.append(
                    pd.Series(
                        pd.to_numeric(selected["equity_index"], errors="raise").to_numpy(),
                        index=pd.DatetimeIndex(selected["timestamp"]),
                        name=candidate_id,
                    )
                )
            aligned = pd.concat(components, axis=1).sort_index().ffill().fillna(1.0)
            combined = sum(
                aligned[candidate_id] * weight
                for candidate_id, weight in COMPONENT_WEIGHTS.items()
            )
            combined.iloc[0] = 1.0
            metrics = _curve_metrics(combined, calendar_days=calendar_days)
            component_rows = [
                fold_rows[(candidate_id, fold.fold_id)]
                for candidate_id in COMPONENT_WEIGHTS
            ]
            if any(
                int(row["market_count"]) != len(EXPECTED_MARKETS)
                for row in component_rows
            ):
                raise ValueError(
                    "C3 requires complete four-market component curves in every fold"
                )
            gross_profit = sum(
                COMPONENT_WEIGHTS[str(row["candidate_id"])]
                * float(row.get(f"{prefix}_gross_profit") or 0.0)
                for row in component_rows
            )
            gross_loss = sum(
                COMPONENT_WEIGHTS[str(row["candidate_id"])]
                * float(row.get(f"{prefix}_gross_loss") or 0.0)
                for row in component_rows
            )
            metrics.update(
                {
                    "trade_count": int(
                        sum(int(row.get(f"{prefix}_trade_count") or 0) for row in component_rows)
                    ),
                    "gross_profit": gross_profit,
                    "gross_loss": gross_loss,
                    "profit_factor": gross_profit / gross_loss if gross_loss > 0 else None,
                    "total_fees": sum(
                        COMPONENT_WEIGHTS[str(row["candidate_id"])]
                        * float(row.get(f"{prefix}_total_fees") or 0.0)
                        for row in component_rows
                    ),
                    "total_slippage": sum(
                        COMPONENT_WEIGHTS[str(row["candidate_id"])]
                        * float(row.get(f"{prefix}_total_slippage") or 0.0)
                        for row in component_rows
                    ),
                    "halted_market_count": int(
                        sum(
                            int(row.get(f"{prefix}_halted_market_count") or 0)
                            for row in component_rows
                        )
                    ),
                }
            )
            curve_rows = [
                {
                    "candidate_id": ENSEMBLE_ID,
                    "fold_id": fold.fold_id,
                    "scenario": scenario,
                    "timestamp": timestamp.isoformat(),
                    "equity_index": float(value),
                }
                for timestamp, value in combined.items()
            ]
            ensemble_curves.extend(curve_rows)
            ensemble_folds.append(
                {
                    "candidate_id": ENSEMBLE_ID,
                    "fold_id": fold.fold_id,
                    "scenario": scenario,
                    "start": pd.Timestamp(fold.start).isoformat(),
                    "end_exclusive": pd.Timestamp(fold.end).isoformat(),
                    "calendar_days": calendar_days,
                    **metrics,
                    "equity_curve_sha256": _canonical_hash(curve_rows),
                }
            )

        for market in EXPECTED_MARKETS:
            c1 = sleeve_rows[("C1_SLOW_ER168", fold.fold_id, market)]
            c2 = sleeve_rows[("C2_TREND_336_168", fold.fold_id, market)]
            signal_manifest = {
                "weights": COMPONENT_WEIGHTS,
                "components": {
                    "C1_SLOW_ER168": {
                        "alignment_sha256": c1["prediction_alignment_sha256"],
                        "fits_sha256": c1["fit_manifest_sha256"],
                    },
                    "C2_TREND_336_168": {
                        "alignment_sha256": c2["prediction_alignment_sha256"],
                        "fits_sha256": c2["fit_manifest_sha256"],
                    },
                },
            }
            ensemble_sleeves.append(
                {
                    "candidate_id": ENSEMBLE_ID,
                    "fold_id": fold.fold_id,
                    "market": market,
                    "base_total_return": 0.5 * float(c1["base_total_return"])
                    + 0.5 * float(c2["base_total_return"]),
                    "cost_stress_2x_total_return": 0.5
                    * float(c1["cost_stress_2x_total_return"])
                    + 0.5 * float(c2["cost_stress_2x_total_return"]),
                    "base_trade_count": int(c1["base_trade_count"])
                    + int(c2["base_trade_count"]),
                    "cost_stress_2x_trade_count": int(
                        c1["cost_stress_2x_trade_count"]
                    )
                    + int(c2["cost_stress_2x_trade_count"]),
                    "base_final_halt_state": (
                        "HALTED"
                        if "HALTED"
                        in {
                            str(c1["base_final_halt_state"]).upper(),
                            str(c2["base_final_halt_state"]).upper(),
                        }
                        else "ACTIVE"
                    ),
                    "cost_stress_2x_final_halt_state": (
                        "HALTED"
                        if "HALTED"
                        in {
                            str(c1["cost_stress_2x_final_halt_state"]).upper(),
                            str(c2["cost_stress_2x_final_halt_state"]).upper(),
                        }
                        else "ACTIVE"
                    ),
                    "prediction_reused_for_cost_stress": bool(
                        c1["prediction_reused_for_cost_stress"]
                        and c2["prediction_reused_for_cost_stress"]
                    ),
                    "signal_manifest_sha256": _canonical_hash(signal_manifest),
                }
            )

    payload = {
        "ensemble_schema_version": SCRIPT_SCHEMA_VERSION,
        "candidate_id": ENSEMBLE_ID,
        "construction": "fixed_normalized_equity_blend",
        "within_market_component_weights": COMPONENT_WEIGHTS,
        "component_config_hashes": {
            candidate_id: result.candidate_hashes[candidate_id]
            for candidate_id in COMPONENT_WEIGHTS
        },
        "selection_dependent_weighting": False,
        "capital_scaling_evidence": _ensemble_scaling_evidence(candidates),
        "fold_metrics": ensemble_folds,
        "sleeve_metrics": ensemble_sleeves,
        "equity_curves": ensemble_curves,
        "research_only": True,
        "orders_sent": 0,
    }
    payload["ensemble_result_sha256"] = _canonical_hash(payload)
    return payload


def _compound(returns: Iterable[float]) -> float:
    return float(np.prod([1.0 + float(value) for value in returns]) - 1.0)


def _candidate_dev_evidence(
    candidate_id: str,
    *,
    result: PortfolioResearchResult,
    ensemble: Mapping[str, Any],
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]], bool]:
    if candidate_id == ENSEMBLE_ID:
        folds: dict[str, dict[str, Any]] = {}
        for row in ensemble["fold_metrics"]:
            fold_id, scenario = str(row["fold_id"]), str(row["scenario"])
            folds.setdefault(fold_id, {})[scenario] = dict(row)
        sleeves = [dict(row) for row in ensemble["sleeve_metrics"]]
        manifests_reused = all(
            bool(row["prediction_reused_for_cost_stress"]) for row in sleeves
        )
        return folds, sleeves, manifests_reused

    raw_folds = _fold_lookup(result.fold_results)
    folds = {}
    for fold in FOLDS:
        row = raw_folds[(candidate_id, fold.fold_id)]
        folds[fold.fold_id] = {
            "base": {
                key.removeprefix("base_"): value
                for key, value in row.items()
                if key.startswith("base_")
            },
            "cost_stress_2x": {
                key.removeprefix("cost_stress_2x_"): value
                for key, value in row.items()
                if key.startswith("cost_stress_2x_")
            },
            "market_count": row["market_count"],
            "prediction_bundle_sha256": row["prediction_bundle_sha256"],
        }
    sleeves = [
        dict(row)
        for row in result.sleeve_results
        if str(row["candidate_id"]) == candidate_id
    ]
    unique_manifests: dict[str, set[tuple[str, str]]] = {}
    for row in sleeves:
        unique_manifests.setdefault(str(row["market"]), set()).add(
            (
                str(row["prediction_alignment_sha256"]),
                str(row["fit_manifest_sha256"]),
            )
        )
    manifests_reused = all(
        bool(row["prediction_reused_for_cost_stress"]) for row in sleeves
    ) and all(len(values) == 1 for values in unique_manifests.values())
    return folds, sleeves, manifests_reused


def _evaluate_candidate(
    candidate_id: str,
    *,
    result: PortfolioResearchResult,
    ensemble: Mapping[str, Any],
) -> dict[str, Any]:
    folds, sleeves, manifests_reused = _candidate_dev_evidence(
        candidate_id,
        result=result,
        ensemble=ensemble,
    )
    dev_rows = {fold_id: folds[fold_id] for fold_id in DEV_FOLDS}
    complete = all(
        int(row.get("market_count", len(EXPECTED_MARKETS)))
        == len(EXPECTED_MARKETS)
        for row in dev_rows.values()
    )
    base_gross_profit = sum(
        float(row["base"].get("gross_profit") or 0.0) for row in dev_rows.values()
    )
    base_gross_loss = sum(
        float(row["base"].get("gross_loss") or 0.0) for row in dev_rows.values()
    )
    profit_factor = (
        base_gross_profit / base_gross_loss
        if base_gross_loss > 0
        else ("UNBOUNDED" if base_gross_profit > 0 else None)
    )
    worst_drawdown = max(
        float(row[scenario].get("max_drawdown") or 0.0)
        for row in dev_rows.values()
        for scenario in ("base", "cost_stress_2x")
    )
    total_trades = sum(
        int(row["base"].get("trade_count") or 0) for row in dev_rows.values()
    )
    halted_sleeves = sum(
        int(row[scenario].get("halted_market_count") or 0)
        for row in dev_rows.values()
        for scenario in ("base", "cost_stress_2x")
    )
    market_returns: dict[str, float] = {}
    for market in EXPECTED_MARKETS:
        selected = [
            row
            for row in sleeves
            if str(row["market"]) == market and str(row["fold_id"]) in DEV_FOLDS
        ]
        by_fold = {str(row["fold_id"]): row for row in selected}
        if set(by_fold) == set(DEV_FOLDS):
            market_returns[market] = _compound(
                float(by_fold[fold_id]["base_total_return"])
                for fold_id in DEV_FOLDS
            )
    nonnegative_markets = sorted(
        market for market, value in market_returns.items() if value >= 0
    )

    gate_rows = [
        {
            "gate": f"{fold_id.lower()}_base_portfolio_return_positive",
            "actual": float(dev_rows[fold_id]["base"]["total_return"]),
            "operator": ">",
            "threshold": 0.0,
            "passed": float(dev_rows[fold_id]["base"]["total_return"]) > 0,
        }
        for fold_id in DEV_FOLDS
    ]
    gate_rows.extend(
        {
            "gate": f"{fold_id.lower()}_cost_stress_2x_portfolio_return_positive",
            "actual": float(dev_rows[fold_id]["cost_stress_2x"]["total_return"]),
            "operator": ">",
            "threshold": 0.0,
            "passed": float(dev_rows[fold_id]["cost_stress_2x"]["total_return"])
            > 0,
        }
        for fold_id in DEV_FOLDS
    )
    pf_passed = profit_factor == "UNBOUNDED" or (
        isinstance(profit_factor, float)
        and profit_factor >= PROTOCOL_THRESHOLDS["minimum_profit_factor"]
    )
    gate_rows.extend(
        (
            {
                "gate": "combined_base_profit_factor",
                "actual": profit_factor,
                "operator": ">=",
                "threshold": PROTOCOL_THRESHOLDS["minimum_profit_factor"],
                "passed": pf_passed,
            },
            {
                "gate": "worst_fold_drawdown_base_or_2x",
                "actual": worst_drawdown,
                "operator": "<=",
                "threshold": PROTOCOL_THRESHOLDS["maximum_worst_fold_drawdown"],
                "passed": worst_drawdown
                <= PROTOCOL_THRESHOLDS["maximum_worst_fold_drawdown"],
            },
            {
                "gate": "nonnegative_market_count",
                "actual": len(nonnegative_markets),
                "operator": ">=",
                "threshold": PROTOCOL_THRESHOLDS[
                    "minimum_nonnegative_market_count"
                ],
                "passed": len(nonnegative_markets)
                >= PROTOCOL_THRESHOLDS["minimum_nonnegative_market_count"],
            },
            {
                "gate": "portfolio_round_trip_count",
                "actual": total_trades,
                "operator": ">=",
                "threshold": PROTOCOL_THRESHOLDS["minimum_total_trades"],
                "passed": total_trades
                >= PROTOCOL_THRESHOLDS["minimum_total_trades"],
            },
            {
                "gate": "no_halted_sleeves",
                "actual": halted_sleeves,
                "operator": "==",
                "threshold": 0,
                "passed": halted_sleeves == 0,
            },
            {
                "gate": "base_and_2x_share_immutable_signal_manifest",
                "actual": manifests_reused,
                "operator": "==",
                "threshold": True,
                "passed": manifests_reused,
            },
        )
    )
    if not complete or total_trades < PROTOCOL_THRESHOLDS["minimum_total_trades"]:
        status = INSUFFICIENT_EVIDENCE
    else:
        status = PASS if all(row["passed"] for row in gate_rows) else FAIL
    d3 = folds[CONTAMINATED_FOLD]
    return {
        "candidate_id": candidate_id,
        "protocol_role": "CHALLENGER",
        "development_status": status,
        "promotion_status": "NOT_AUTHORIZED",
        "development_fold_ids": list(DEV_FOLDS),
        "complete_four_market_evidence": complete,
        "base_compounded_return": _compound(
            float(dev_rows[fold_id]["base"]["total_return"])
            for fold_id in DEV_FOLDS
        ),
        "cost_stress_2x_compounded_return": _compound(
            float(dev_rows[fold_id]["cost_stress_2x"]["total_return"])
            for fold_id in DEV_FOLDS
        ),
        "development_selection_score_min_d1_d2_2x_return": min(
            float(dev_rows[fold_id]["cost_stress_2x"]["total_return"])
            for fold_id in DEV_FOLDS
        ),
        "combined_base_profit_factor": profit_factor,
        "worst_fold_drawdown_base_or_2x": worst_drawdown,
        "nonnegative_market_count": len(nonnegative_markets),
        "nonnegative_markets": nonnegative_markets,
        "market_compounded_returns": market_returns,
        "portfolio_round_trip_count": total_trades,
        "halted_sleeve_count": halted_sleeves,
        "gates": gate_rows,
        "D3_CONTAMINATED": {
            "influences_development_status": False,
            "base_total_return": d3["base"]["total_return"],
            "cost_stress_2x_total_return": d3["cost_stress_2x"]["total_return"],
            "base_profit_factor": d3["base"].get("profit_factor"),
            "worst_drawdown_base_or_2x": max(
                float(d3["base"].get("max_drawdown") or 0.0),
                float(d3["cost_stress_2x"].get("max_drawdown") or 0.0),
            ),
            "base_trade_count": d3["base"].get("trade_count"),
            "halted_sleeve_count": int(
                d3["base"].get("halted_market_count") or 0
            )
            + int(d3["cost_stress_2x"].get("halted_market_count") or 0),
        },
    }


def _development_ranking(
    candidates: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Apply the frozen D1/D2-only selection rule to passing challengers."""

    eligible = [
        candidate
        for candidate in candidates
        if candidate.get("protocol_role") == "CHALLENGER"
        and candidate.get("development_status") == PASS
    ]
    ordered = sorted(
        eligible,
        key=lambda candidate: (
            -float(
                candidate[
                    "development_selection_score_min_d1_d2_2x_return"
                ]
            ),
            float(candidate["worst_fold_drawdown_base_or_2x"]),
            str(candidate["candidate_id"]),
        ),
    )
    return [
        {
            "rank": index,
            "candidate_id": candidate["candidate_id"],
            "minimum_D1_D2_cost_stress_2x_return": candidate[
                "development_selection_score_min_d1_d2_2x_return"
            ],
            "worst_fold_drawdown_base_or_2x": candidate[
                "worst_fold_drawdown_base_or_2x"
            ],
        }
        for index, candidate in enumerate(ordered, start=1)
    ]


def build_protocol_summary(
    result: PortfolioResearchResult,
    ensemble: Mapping[str, Any],
    *,
    code_fingerprint: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    raw_folds = _fold_lookup(result.fold_results)
    c0_folds = {
        fold_id: {
            "base_total_return": raw_folds[("C0_CONTROL_ER72", fold_id)][
                "base_total_return"
            ],
            "cost_stress_2x_total_return": raw_folds[
                ("C0_CONTROL_ER72", fold_id)
            ]["cost_stress_2x_total_return"],
        }
        for fold_id in (*DEV_FOLDS, CONTAMINATED_FOLD)
    }
    candidates = [
        {
            "candidate_id": "C0_CONTROL_ER72",
            "protocol_role": "COMPARISON_ONLY",
            "development_status": "NOT_EVALUATED",
            "promotion_status": "NOT_ELIGIBLE",
            "fold_metrics": c0_folds,
            "D3_CONTAMINATED_influences_development_status": False,
        }
    ]
    candidates.extend(
        _evaluate_candidate(candidate_id, result=result, ensemble=ensemble)
        for candidate_id in (
            "C1_SLOW_ER168",
            "C2_TREND_336_168",
            ENSEMBLE_ID,
        )
    )
    ranking = _development_ranking(candidates)
    selected = ranking[0]["candidate_id"] if ranking else None
    payload = {
        "protocol_summary_schema_version": SCRIPT_SCHEMA_VERSION,
        "protocol": "STRATEGY_RESEARCH_V2",
        "development_fold_ids": list(DEV_FOLDS),
        "contaminated_diagnostic_fold_id": CONTAMINATED_FOLD,
        "contaminated_fold_influences_any_status": False,
        "thresholds": PROTOCOL_THRESHOLDS,
        "calculation_code_fingerprint": dict(code_fingerprint or {}),
        "decision": {
            "candidate_selection": (
                "DEVELOPMENT_CHALLENGER_SELECTED" if selected else "NO_PASSING_CHALLENGER"
            ),
            "selected_development_challenger": selected,
            "ranking": ranking,
            "selection_rule": (
                "Among PASS challengers C1-C3, maximize the minimum D1/D2 "
                "2x-cost portfolio return; break ties by lower worst-fold "
                "drawdown, then candidate_id. C0 and D3 are excluded."
            ),
            "promotion": "NOT_AUTHORIZED",
            "active_champion": "CASH_OBSERVE_ONLY",
            "interpretation": (
                "Development selection identifies the next forward-paper "
                "challenger only; it does not authorize activation, live routing, "
                "or promotion."
            ),
        },
        "candidates": candidates,
        "orders_sent": 0,
        "live_order_routing": False,
    }
    payload["protocol_summary_sha256"] = _canonical_hash(payload)
    return payload


def build_code_fingerprint(repository: Path) -> dict[str, Any]:
    """Hash the fixed calculation surface without recording local paths/diffs."""

    root = repository.expanduser().resolve()
    file_hashes: dict[str, str] = {}
    for relative_name in FINGERPRINT_FILES:
        path = root / relative_name
        if not path.is_file() or path.is_symlink():
            raise ValueError(f"Missing regular fingerprint input: {relative_name}")
        file_hashes[relative_name] = _sha256(path.read_bytes())
    payload = {
        "algorithm": "sha256",
        "files": file_hashes,
    }
    payload["aggregate_sha256"] = _canonical_hash(payload)
    return payload


def _shadow_schema_columns(
    connection: sqlite3.Connection, table: str
) -> set[str]:
    return {
        str(row["name"])
        for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
    }


def snapshot_shadow_ledger(label: str, path: Path) -> dict[str, Any]:
    """Read one installed shadow ledger and pair buy/sell fills FIFO."""

    required = {
        "shadow_runs": {"run_id", "market", "started_wall_ns", "halt_reason"},
        "shadow_state": {
            "run_id",
            "realized_pnl_quote",
            "cumulative_fees_quote",
            "last_equity_quote",
        },
        "shadow_fills": {
            "fill_id",
            "side",
            "filled_base",
            "filled_quote",
            "fee_quote",
            "book_wall_ns",
            "created_wall_ns",
        },
    }
    with _readonly_connection(path) as connection:
        connection.execute("BEGIN")
        for table, columns in required.items():
            if not columns.issubset(_shadow_schema_columns(connection, table)):
                raise ValueError(f"Shadow ledger {label} has an incompatible schema")
        fills = connection.execute(
            """
            SELECT fill_id, side, filled_base, filled_quote, fee_quote,
                   book_wall_ns, created_wall_ns
            FROM shadow_fills
            ORDER BY COALESCE(book_wall_ns, created_wall_ns), created_wall_ns,
                     fill_id
            """
        ).fetchall()
        latest = connection.execute(
            """
            SELECT r.market, s.realized_pnl_quote, s.cumulative_fees_quote,
                   s.last_equity_quote
            FROM shadow_runs AS r
            JOIN shadow_state AS s ON s.run_id = r.run_id
            ORDER BY r.started_wall_ns DESC, r.run_id DESC
            LIMIT 1
            """
        ).fetchone()
        run_stats = connection.execute(
            """
            SELECT COUNT(*) AS run_count,
                   SUM(CASE WHEN halt_reason = 'graceful:runtime_error'
                            THEN 1 ELSE 0 END) AS runtime_errors
            FROM shadow_runs
            """
        ).fetchone()
        connection.rollback()
    if latest is None:
        raise ValueError(f"Shadow ledger {label} has no runs")

    open_buys: deque[sqlite3.Row] = deque()
    trades: list[dict[str, float]] = []
    unmatched_sells = 0
    for fill in fills:
        if str(fill["side"]) == "buy":
            open_buys.append(fill)
            continue
        if not open_buys:
            unmatched_sells += 1
            continue
        buy = open_buys.popleft()
        quantity_delta = abs(float(buy["filled_base"]) - float(fill["filled_base"]))
        quantity_scale = max(float(buy["filled_base"]), float(fill["filled_base"]))
        if quantity_delta > max(1e-12, quantity_scale * 1e-8):
            raise ValueError(
                f"Shadow ledger {label} contains a non-1:1 fill pair"
            )
        buy_time = int(buy["book_wall_ns"] or buy["created_wall_ns"])
        sell_time = int(fill["book_wall_ns"] or fill["created_wall_ns"])
        trades.append(
            {
                "net_pnl_quote": float(fill["filled_quote"])
                - float(fill["fee_quote"])
                - float(buy["filled_quote"])
                - float(buy["fee_quote"]),
                "hold_seconds": (sell_time - buy_time) / 1_000_000_000,
            }
        )
    fill_net_pnl = float(sum(item["net_pnl_quote"] for item in trades))
    fees = float(sum(float(fill["fee_quote"]) for fill in fills))
    realized = float(latest["realized_pnl_quote"])
    holds = [item["hold_seconds"] for item in trades]
    return {
        "ledger_label": label,
        "market": str(latest["market"]),
        "run_count": int(run_stats["run_count"] or 0),
        "runtime_error_count": int(run_stats["runtime_errors"] or 0),
        "fill_count": len(fills),
        "round_trip_count": len(trades),
        "unmatched_buy_count": len(open_buys),
        "unmatched_sell_count": unmatched_sells,
        "net_pnl_quote": fill_net_pnl,
        "loss_quote": max(0.0, -fill_net_pnl),
        "fee_quote": fees,
        "average_hold_seconds": float(np.mean(holds)) if holds else None,
        "median_hold_seconds": float(np.median(holds)) if holds else None,
        "state_realized_pnl_quote": realized,
        "fill_to_state_reconciliation_delta_quote": fill_net_pnl - realized,
        "last_equity_quote": float(latest["last_equity_quote"]),
        "state_cumulative_fees_quote": float(latest["cumulative_fees_quote"]),
        "orders_sent": 0,
    }


def build_shadow_snapshot(paths: Mapping[str, Path]) -> dict[str, Any]:
    ledgers: list[dict[str, Any]] = []
    unavailable: list[str] = []
    for label, path in sorted(paths.items()):
        if not path.is_file():
            unavailable.append(label)
            continue
        ledgers.append(snapshot_shadow_ledger(label, path))
    payload = {
        "shadow_snapshot_schema_version": SCRIPT_SCHEMA_VERSION,
        "read_only": True,
        "available_ledger_count": len(ledgers),
        "unavailable_ledger_labels": unavailable,
        "ledgers": ledgers,
        "combined": {
            "net_pnl_quote": float(sum(row["net_pnl_quote"] for row in ledgers)),
            "loss_quote": float(sum(row["loss_quote"] for row in ledgers)),
            "fee_quote": float(sum(row["fee_quote"] for row in ledgers)),
            "round_trip_count": int(sum(row["round_trip_count"] for row in ledgers)),
            "runtime_error_count": int(
                sum(row["runtime_error_count"] for row in ledgers)
            ),
            "fill_count": int(sum(row["fill_count"] for row in ledgers)),
            "average_hold_seconds": (
                float(
                    sum(
                        float(row["average_hold_seconds"])
                        * int(row["round_trip_count"])
                        for row in ledgers
                        if row["average_hold_seconds"] is not None
                    )
                    / sum(int(row["round_trip_count"]) for row in ledgers)
                )
                if sum(int(row["round_trip_count"]) for row in ledgers) > 0
                else None
            ),
        },
        "simulated": True,
        "orders_sent": 0,
        "live_order_routing": False,
    }
    payload["shadow_snapshot_sha256"] = _canonical_hash(payload)
    return payload


def _flatten_protocol_candidates(protocol: Mapping[str, Any]) -> pd.DataFrame:
    rows = []
    for candidate in protocol["candidates"]:
        rows.append(
            {
                key: value
                for key, value in candidate.items()
                if key not in {"gates", "D3_CONTAMINATED", "fold_metrics"}
                and not isinstance(value, (dict, list))
            }
        )
    return pd.DataFrame(rows)


def _protocol_gate_frame(protocol: Mapping[str, Any]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"candidate_id": candidate["candidate_id"], **gate}
            for candidate in protocol["candidates"]
            for gate in candidate.get("gates", [])
        ]
    )


def run_suite(
    *,
    config_dir: Path,
    output_dir: Path,
    shadow_paths: Mapping[str, Path] | None,
) -> dict[str, Any]:
    candles, candidates = load_frozen_inputs(config_dir)
    repository = Path(__file__).resolve().parents[1]
    code_fingerprint = build_code_fingerprint(repository)
    quality = build_data_quality(candles)
    if quality["total_invalid_count"] != 0:
        raise ValueError("Cached candle data failed the frozen data-quality checks")
    if not quality["all_protocol_windows_complete"]:
        raise ValueError("Cached candles do not cover every frozen protocol window")

    # This is intentionally the only portfolio simulation call in the suite.
    raw_result = run_portfolio_research(
        candles,
        candidates,
        FOLDS,
        gates=PortfolioResearchGates(
            minimum_folds=3,
            minimum_markets_per_fold=4,
            minimum_total_trades=30,
            minimum_nonnegative_market_count=3,
            minimum_annualized_return=0.0,
            minimum_cost_stress_annualized_return=0.0,
            minimum_positive_fold_share=2 / 3,
            minimum_cost_stress_positive_fold_share=2 / 3,
            minimum_profit_factor=1.15,
            maximum_worst_fold_drawdown=0.10,
        ),
    )
    quality_hashes = {
        str(row["market"]): str(row["source_data_sha256"])
        for row in quality["markets"]
    }
    if raw_result.source_hashes != quality_hashes:
        raise RuntimeError("Data-quality and simulation source hashes differ")
    raw_frames = raw_result.csv_frames()
    raw_frame_bytes = {
        name: _csv_bytes(frame) for name, frame in raw_frames.items()
    }
    raw_payload = {
        "suite_wrapper_schema_version": SCRIPT_SCHEMA_VERSION,
        "single_simulation_call": True,
        "protocol_gate_source": "protocol_summary.json",
        "raw_runner_gates_include_contaminated_fold": True,
        "frame_sha256": {
            name: _sha256(content) for name, content in raw_frame_bytes.items()
        },
        "calculation_code_fingerprint": code_fingerprint,
        "suite": raw_result.as_dict(),
        "orders_sent": 0,
    }
    ensemble = build_c3_ensemble(raw_result, candidates)
    protocol = build_protocol_summary(
        raw_result,
        ensemble,
        code_fingerprint=code_fingerprint,
    )
    shadow = build_shadow_snapshot(shadow_paths or {})

    writer = ArtifactWriter(output_dir)
    writer.add_json("suite.json", raw_payload, role="raw_research_suite")
    for name, frame in raw_frames.items():
        writer.add_csv(
            f"suite_{name}.csv",
            frame,
            role=f"raw_research_suite_{name}",
        )
    writer.add_json("data_quality.json", quality, role="data_quality_summary")
    writer.add_csv(
        "data_quality.csv",
        pd.DataFrame(quality["markets"]),
        role="data_quality_markets",
    )
    writer.add_json("ensemble_c3.json", ensemble, role="fixed_ensemble_summary")
    writer.add_csv(
        "ensemble_c3_folds.csv",
        pd.DataFrame(ensemble["fold_metrics"]),
        role="fixed_ensemble_fold_metrics",
    )
    writer.add_csv(
        "ensemble_c3_sleeves.csv",
        pd.DataFrame(ensemble["sleeve_metrics"]),
        role="fixed_ensemble_sleeve_metrics",
    )
    writer.add_csv(
        "ensemble_c3_equity.csv",
        pd.DataFrame(ensemble["equity_curves"]),
        role="fixed_ensemble_equity",
    )
    writer.add_json("protocol_summary.json", protocol, role="authoritative_protocol_summary")
    writer.add_csv(
        "protocol_candidates.csv",
        _flatten_protocol_candidates(protocol),
        role="protocol_candidate_statuses",
    )
    writer.add_csv(
        "protocol_gates.csv",
        _protocol_gate_frame(protocol),
        role="protocol_gate_checks",
    )
    writer.add_csv(
        "protocol_ranking.csv",
        pd.DataFrame(protocol["decision"]["ranking"]),
        role="frozen_development_ranking",
    )
    writer.add_json("shadow_snapshot.json", shadow, role="optional_shadow_diagnostic")
    manifest = writer.commit(
        receipt={
            "raw_suite_result_sha256": raw_result.result_hash,
            "data_quality_sha256": quality["data_quality_sha256"],
            "ensemble_result_sha256": ensemble["ensemble_result_sha256"],
            "protocol_summary_sha256": protocol["protocol_summary_sha256"],
            "shadow_snapshot_sha256": shadow["shadow_snapshot_sha256"],
            "calculation_code_aggregate_sha256": code_fingerprint[
                "aggregate_sha256"
            ],
        }
    )
    return {
        "manifest": manifest,
        "protocol": protocol,
        "orders_sent": 0,
    }


def _default_shadow_paths() -> dict[str, Path]:
    root = Path.home() / "Library" / "Application Support" / "Coinpilot" / "instances"
    return {
        "btc": root / "btc" / "data" / "shadow.db",
        "eth": root / "eth" / "data" / "shadow.db",
    }


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    repository = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(
        description="Run the frozen, research-only Strategy V2 suite."
    )
    parser.add_argument(
        "--config-dir",
        type=Path,
        default=repository / "configs" / "research-v2",
        help="directory containing the frozen V2 TOML files",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="directory for strict JSON/CSV artifacts",
    )
    parser.add_argument(
        "--skip-shadow-snapshot",
        action="store_true",
        help="do not inspect the two optional installed shadow ledgers",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    shadow_paths = {} if args.skip_shadow_snapshot else _default_shadow_paths()
    result = run_suite(
        config_dir=args.config_dir,
        output_dir=args.output_dir,
        shadow_paths=shadow_paths,
    )
    statuses = {
        row["candidate_id"]: row["development_status"]
        for row in result["protocol"]["candidates"]
    }
    print(
        json.dumps(
            {
                "completed": True,
                "development_statuses": statuses,
                "selected_development_challenger": result["protocol"]["decision"][
                    "selected_development_challenger"
                ],
                "promotion": "NOT_AUTHORIZED",
                "orders_sent": 0,
            },
            allow_nan=False,
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
