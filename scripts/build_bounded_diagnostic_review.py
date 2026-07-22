#!/usr/bin/env python3
"""Build a reproducible loss review and bounded-shadow deployment report.

The historical inputs are frozen SQLite/config snapshots under ``artifacts``.
The D2 section records a point-in-time, public-data-only runtime snapshot.  No
credential is read and no live-order client is imported or invoked.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import io
import json
import math
import os
import sqlite3
import subprocess
import tempfile
import tomllib
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


KST = ZoneInfo("Asia/Seoul")
MARKETS = ("BTC", "ETH", "XRP", "SOL")
D2_PORTS = {"BTC": 8774, "ETH": 8775, "XRP": 8776, "SOL": 8777}
ROUND_TRIP_SQL = """
WITH fills AS (
    SELECT f.*, d.reason, d.signal,
           json_extract(d.features_json, '$.spread_bps') AS spread_bps
    FROM shadow_fills AS f
    JOIN shadow_orders AS o USING (order_id)
    JOIN shadow_decisions AS d USING (decision_id)
),
buys AS (
    SELECT *, row_number() OVER (
        ORDER BY created_wall_ns, fill_id
    ) AS trade_no
    FROM fills WHERE side = 'buy'
),
sells AS (
    SELECT *, row_number() OVER (
        ORDER BY created_wall_ns, fill_id
    ) AS trade_no
    FROM fills WHERE side = 'sell'
),
trades AS (
    SELECT b.trade_no,
           b.created_wall_ns AS entry_ns,
           s.created_wall_ns AS exit_ns,
           (s.book_monotonic_ns - b.book_monotonic_ns) / 1e9 AS hold_s,
           s.filled_quote - b.filled_quote AS gross_pnl,
           b.fee_quote + s.fee_quote AS fees,
           s.filled_quote - s.fee_quote
             - b.filled_quote - b.fee_quote AS net_pnl,
           (s.vwap_price / b.vwap_price - 1) * 10000 AS gross_bps,
           b.spread_bps AS entry_spread_bps,
           s.spread_bps AS exit_spread_bps,
           s.reason AS exit_reason
    FROM buys AS b JOIN sells AS s USING (trade_no)
)
SELECT count(*) AS round_trips,
       COALESCE(sum(net_pnl > 0), 0) AS net_wins,
       COALESCE(sum(gross_pnl > 0), 0) AS gross_wins,
       COALESCE(sum(gross_pnl), 0.0) AS gross_pnl,
       COALESCE(sum(fees), 0.0) AS closed_fees,
       COALESCE(sum(net_pnl), 0.0) AS closed_net_pnl,
       avg(net_pnl) AS avg_net_pnl,
       avg(gross_bps) AS avg_gross_bps,
       avg(hold_s) AS avg_hold_s,
       avg(CASE WHEN entry_spread_bps IS NOT NULL
                     AND exit_spread_bps IS NOT NULL
                THEN (entry_spread_bps + exit_spread_bps) / 2.0 END)
           AS average_round_trip_spread_bps,
       min(entry_ns) AS first_fill_ns,
       max(exit_ns) AS last_fill_ns
FROM trades
"""


def _args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--instances-root",
        type=Path,
        default=(
            Path.home()
            / "Library/Application Support/Coinpilot/instances"
        ),
    )
    return parser.parse_args()


def _sha256(path: Path) -> str | None:
    if not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(
                value,
                handle,
                ensure_ascii=False,
                indent=2,
                sort_keys=False,
                allow_nan=False,
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _connect_readonly(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def _iso_kst(wall_ns: int | None) -> str | None:
    if wall_ns is None:
        return None
    return datetime.fromtimestamp(
        wall_ns / 1e9,
        tz=UTC,
    ).astimezone(KST).isoformat(timespec="microseconds")


def _logical_hash(connection: sqlite3.Connection) -> str:
    digest = hashlib.sha256()
    for statement in connection.iterdump():
        digest.update(statement.encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def _historical_market(source_dir: Path, symbol: str) -> dict[str, Any]:
    name = symbol.lower()
    db_path = source_dir / f"{name}-shadow.db"
    config_path = source_dir / f"{name}-config.toml"
    config = tomllib.loads(config_path.read_text(encoding="utf-8"))
    initial_cash = float(config["shadow"]["initial_cash"])
    with _connect_readonly(db_path) as connection:
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
        foreign_keys = len(connection.execute("PRAGMA foreign_key_check").fetchall())
        metrics = dict(connection.execute(ROUND_TRIP_SQL).fetchone())
        latest = dict(
            connection.execute(
                """
                SELECT r.run_id, r.status AS run_status, r.started_wall_ns,
                       r.ended_wall_ns, r.code_version, r.halt_reason,
                       s.lifecycle_status, s.cash_quote, s.base_quantity,
                       s.realized_pnl_quote, s.cumulative_fees_quote,
                       s.last_equity_quote, s.peak_equity_quote,
                       s.max_drawdown, s.updated_wall_ns
                FROM shadow_runs AS r
                JOIN shadow_state AS s USING (run_id)
                ORDER BY r.started_wall_ns DESC LIMIT 1
                """
            ).fetchone()
        )
        row_counts = {
            table: connection.execute(
                f"SELECT count(*) FROM {table}"
            ).fetchone()[0]
            for table in (
                "shadow_runs",
                "shadow_decisions",
                "shadow_orders",
                "shadow_fills",
                "shadow_equity",
                "shadow_health",
                "notification_outbox",
            )
        }
        logical_hash = _logical_hash(connection)

    metrics = {
        key: (float(value) if isinstance(value, float) else value)
        for key, value in metrics.items()
    }
    closed_net = float(metrics["closed_net_pnl"])
    realized = float(latest["realized_pnl_quote"])
    if not math.isclose(closed_net, realized, rel_tol=1e-10, abs_tol=1e-6):
        raise ValueError(f"{symbol} closed PnL does not reconcile to state")
    gross = float(metrics["gross_pnl"])
    fees = float(metrics["closed_fees"])
    if not math.isclose(gross - fees, closed_net, abs_tol=1e-6):
        raise ValueError(f"{symbol} gross-fee identity failed")
    mtm_pnl = float(latest["last_equity_quote"]) - initial_cash
    fee_rate = float(config["shadow"]["fee_rate"])
    spread_component = metrics["average_round_trip_spread_bps"]
    hurdle_bps = None
    if spread_component is not None:
        hurdle_bps = 2.0 * fee_rate * 10_000.0 + float(spread_component)
    file_hashes = {
        item.name: _sha256(item)
        for item in (
            db_path,
            db_path.with_name(db_path.name + "-wal"),
            db_path.with_name(db_path.name + "-shm"),
            config_path,
        )
        if item.exists()
    }
    return {
        "market": f"KRW-{symbol}",
        "initial_cash_quote": initial_cash,
        "run_id": latest["run_id"],
        "run_status": latest["run_status"],
        "lifecycle_status": latest["lifecycle_status"],
        "halt_reason": latest["halt_reason"],
        "started_at_kst": _iso_kst(latest["started_wall_ns"]),
        "as_of_kst": _iso_kst(latest["updated_wall_ns"]),
        "first_fill_at_kst": _iso_kst(metrics["first_fill_ns"]),
        "last_fill_at_kst": _iso_kst(metrics["last_fill_ns"]),
        "equity_quote": float(latest["last_equity_quote"]),
        "cash_quote": float(latest["cash_quote"]),
        "base_quantity": float(latest["base_quantity"]),
        "realized_pnl_quote": realized,
        "snapshot_mtm_pnl_quote": mtm_pnl,
        "open_mtm_pnl_quote": mtm_pnl - closed_net,
        "cumulative_fees_quote": float(latest["cumulative_fees_quote"]),
        "max_drawdown": float(latest["max_drawdown"]),
        "round_trips": int(metrics["round_trips"]),
        "net_wins": int(metrics["net_wins"]),
        "gross_wins": int(metrics["gross_wins"]),
        "gross_pnl_quote": gross,
        "closed_fees_quote": fees,
        "closed_net_pnl_quote": closed_net,
        "average_net_trade_quote": metrics["avg_net_pnl"],
        "average_gross_bps": metrics["avg_gross_bps"],
        "average_hold_seconds": metrics["avg_hold_s"],
        "average_round_trip_spread_bps": spread_component,
        "estimated_break_even_hurdle_bps": hurdle_bps,
        "integrity_check": integrity,
        "foreign_key_violations": foreign_keys,
        "row_counts": row_counts,
        "logical_dump_sha256": logical_hash,
        "file_sha256": file_hashes,
        "code_version": latest["code_version"],
    }


def _runtime_values(path: Path) -> dict[str, str]:
    output: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if line and not line.lstrip().startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            output[key] = value
    return output


def _d2_instance(instances_root: Path, symbol: str) -> dict[str, Any]:
    instance = f"d2-{symbol.lower()}"
    root = instances_root / instance
    config_path = root / "config/config.toml"
    runtime_path = root / "config/runtime.env"
    db_path = root / "data/shadow.db"
    helper_path = root / "bin/coinpilot-bounded-shadow"
    wrapper_path = root / "bin/coinpilot-service"
    config = tomllib.loads(config_path.read_text(encoding="utf-8"))
    runtime = _runtime_values(runtime_path)
    validation = subprocess.run(
        [
            str(root / "venv/bin/python"),
            str(helper_path),
            "--config",
            str(config_path),
            "--cooldown-seconds",
            runtime["COINPILOT_SHADOW_COOLDOWN_SECONDS"],
            "--max-round-trips-per-day",
            runtime["COINPILOT_SHADOW_MAX_ROUND_TRIPS_PER_DAY"],
            "--reserve-full-order-loss",
            runtime["COINPILOT_SHADOW_RESERVE_FULL_ORDER_LOSS"],
            "--execution-spread-recheck",
            runtime["COINPILOT_SHADOW_EXECUTION_SPREAD_RECHECK"],
            "--audit-dir",
            str(root / "state"),
            "--validate-only",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=15,
    )
    validation_payload = json.loads(validation.stdout)
    port = int(runtime.get("COINPILOT_WEB_PORT", D2_PORTS[symbol]))
    with urllib.request.urlopen(
        f"http://127.0.0.1:{port}/health/ready", timeout=5
    ) as response:
        health = json.load(response)
    with _connect_readonly(db_path) as connection:
        run = dict(
            connection.execute(
                """
                SELECT run_id, status, code_version, halt_reason
                FROM shadow_runs ORDER BY started_wall_ns DESC LIMIT 1
                """
            ).fetchone()
        )
        state = dict(
            connection.execute(
                """
                SELECT lifecycle_status, cash_quote, base_quantity,
                       realized_pnl_quote, cumulative_fees_quote,
                       last_equity_quote, max_drawdown, halt_reason
                FROM shadow_state WHERE run_id = ?
                """,
                (run["run_id"],),
            ).fetchone()
        )
        counts = dict(
            connection.execute(
                """
                SELECT (SELECT count(*) FROM shadow_decisions) AS decisions,
                       (SELECT count(*) FROM shadow_orders) AS orders,
                       (SELECT count(*) FROM shadow_fills) AS fills,
                       (SELECT count(*) FROM shadow_fills WHERE side='sell')
                         AS completed_round_trips
                """
            ).fetchone()
        )
        policy_versions = [
            row[0]
            for row in connection.execute(
                "SELECT DISTINCT policy_version FROM shadow_decisions"
            )
        ]
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
    return {
        "instance": instance,
        "market": config["data"]["market"],
        "dashboard_port": port,
        "ready": bool(health.get("ready")),
        "run_id": run["run_id"],
        "run_status": run["status"],
        "lifecycle_status": state["lifecycle_status"],
        "halt_reason": state["halt_reason"],
        "initial_cash_quote": float(config["shadow"]["initial_cash"]),
        "order_quote": float(config["shadow"]["order_quote"]),
        "max_daily_loss_pct": float(
            config["shadow"]["max_daily_loss_pct"]
        ),
        "max_drawdown_pct": float(config["shadow"]["max_drawdown_pct"]),
        "model_version": config["shadow"]["model_version"],
        "cooldown_seconds": float(
            runtime["COINPILOT_SHADOW_COOLDOWN_SECONDS"]
        ),
        "max_round_trips_per_day": int(
            runtime["COINPILOT_SHADOW_MAX_ROUND_TRIPS_PER_DAY"]
        ),
        "full_order_loss_reserve": (
            runtime["COINPILOT_SHADOW_RESERVE_FULL_ORDER_LOSS"] == "1"
        ),
        "execution_spread_recheck": (
            runtime["COINPILOT_SHADOW_EXECUTION_SPREAD_RECHECK"] == "1"
        ),
        "notifier_enabled": runtime.get("COINPILOT_ENABLE_NOTIFIER") == "1",
        "cash_quote": float(state["cash_quote"]),
        "base_quantity": float(state["base_quantity"]),
        "equity_quote": float(state["last_equity_quote"]),
        "realized_pnl_quote": float(state["realized_pnl_quote"]),
        "cumulative_fees_quote": float(state["cumulative_fees_quote"]),
        "max_drawdown": float(state["max_drawdown"]),
        "counts": counts,
        "policy_versions": policy_versions,
        "integrity_check": integrity,
        "bounded_profile_valid": bool(validation_payload["valid"]),
        "bounded_settings_sha256": validation_payload[
            "bounded_settings_sha256"
        ],
        "wrapper_sha256": _sha256(wrapper_path),
        "helper_sha256": _sha256(helper_path),
        "code_version": run["code_version"],
        "simulated": bool(health.get("simulated")),
        "live_order_routing": bool(health.get("live_order_routing")),
        "orders_sent": int(health.get("orders_sent", -1)),
    }


def _analysis(source_dir: Path, instances_root: Path) -> dict[str, Any]:
    generated = datetime.now(KST).isoformat(timespec="seconds")
    markets = [_historical_market(source_dir, symbol) for symbol in MARKETS]
    d2 = [_d2_instance(instances_root, symbol) for symbol in MARKETS]
    combined = {
        "round_trips": sum(row["round_trips"] for row in markets),
        "net_wins": sum(row["net_wins"] for row in markets),
        "gross_pnl_quote": sum(row["gross_pnl_quote"] for row in markets),
        "closed_fees_quote": sum(
            row["closed_fees_quote"] for row in markets
        ),
        "closed_net_pnl_quote": sum(
            row["closed_net_pnl_quote"] for row in markets
        ),
        "snapshot_mtm_pnl_quote": sum(
            row["snapshot_mtm_pnl_quote"] for row in markets
        ),
    }
    combined["fee_share_of_closed_loss"] = (
        combined["closed_fees_quote"]
        / -combined["closed_net_pnl_quote"]
    )
    combined["open_mtm_delta_quote"] = (
        combined["snapshot_mtm_pnl_quote"]
        - combined["closed_net_pnl_quote"]
    )
    checks = {
        "all_historical_integrity_ok": all(
            row["integrity_check"] == "ok" for row in markets
        ),
        "all_historical_foreign_keys_ok": all(
            row["foreign_key_violations"] == 0 for row in markets
        ),
        "all_673_closed_trades_reconciled": (
            combined["round_trips"] == 673
        ),
        "all_closed_trades_net_losing": (
            combined["net_wins"] == 0
        ),
        "combined_identity_reconciles": math.isclose(
            combined["gross_pnl_quote"]
            - combined["closed_fees_quote"],
            combined["closed_net_pnl_quote"],
            abs_tol=1e-6,
        ),
        "all_d2_ready": all(row["ready"] for row in d2),
        "all_d2_bounded_profiles_valid": all(
            row["bounded_profile_valid"] for row in d2
        ),
        "all_d2_simulated": all(row["simulated"] for row in d2),
        "all_d2_live_routing_false": all(
            not row["live_order_routing"] for row in d2
        ),
        "all_d2_orders_sent_zero": all(
            row["orders_sent"] == 0 for row in d2
        ),
        "all_d2_notifiers_disabled": all(
            not row["notifier_enabled"] for row in d2
        ),
    }
    if not all(checks.values()):
        failures = [name for name, passed in checks.items() if not passed]
        raise ValueError(f"review validation failed: {failures}")
    return {
        "schema_version": 1,
        "generated_at": generated,
        "historical_as_of": max(row["as_of_kst"] for row in markets),
        "timezone": "Asia/Seoul",
        "question": (
            "Why did diagnostic shadow lose about 1%, and how is the new "
            "10% hard boundary contained?"
        ),
        "historical_markets": markets,
        "combined": combined,
        "d2_instances": d2,
        "checks": checks,
        "interpretation": {
            "root_cause": (
                "The plumbing-only imbalance/flow heuristic traded too often "
                "without a cost-calibrated expected edge."
            ),
            "ten_percent_policy": (
                "A maximum simulated-account risk boundary, not a profit "
                "target or evidence of positive expectancy."
            ),
            "promotion_status": "NOT_AUTHORIZED",
            "confidence": "share_with_caveats",
        },
        "orders_sent": 0,
        "live_order_routing": False,
    }


def _notebook(analysis: dict[str, Any]) -> dict[str, Any]:
    cells: list[dict[str, Any]] = []

    def markdown(source: str) -> None:
        cells.append({"cell_type": "markdown", "metadata": {}, "source": source})

    def code(source: str) -> None:
        cells.append(
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": source,
            }
        )

    markdown("# Diagnostic Shadow Loss Review & 10% Bounded Policy\n\n## TL;DR")
    code(
        """from pathlib import Path
import json
import math

analysis = json.loads(Path("analysis.json").read_text(encoding="utf-8"))
combined = analysis["combined"]
print(f"Closed round trips: {combined['round_trips']:,}; net winners: {combined['net_wins']}")
print(f"Closed PnL: {combined['closed_net_pnl_quote']:,.3f} KRW")
print(f"Closed fees: {combined['closed_fees_quote']:,.3f} KRW ({combined['fee_share_of_closed_loss']:.2%} of closed loss)")
print("Conclusion: the 1% guard stopped the old diagnostic; it did not cause the loss.")
print("The new 10% setting is a hard simulated risk boundary, not a return target.")
assert analysis["orders_sent"] == 0 and analysis["live_order_routing"] is False
"""
    )
    markdown("## Context & Methods")
    code(
        """print("Population: frozen BTC/ETH/XRP/SOL diagnostic ledgers")
print("Trade grain: chronological buy/sell round trips within each isolated ledger")
print("Net PnL = sell quote - sell fee - buy quote - buy fee")
print("Timezone: Asia/Seoul; all executions are simulated public-feed fills")
print("D2 status is a point-in-time operational snapshot, separate from the frozen loss sample")
"""
    )
    markdown("## Data")
    code(
        """print("market | as-of KST | trips | integrity | logical SHA256")
for row in analysis["historical_markets"]:
    print(f"{row['market']} | {row['as_of_kst']} | {row['round_trips']} | {row['integrity_check']} | {row['logical_dump_sha256']}")
assert all(analysis["checks"].values())
"""
    )
    markdown("## Results")
    code(
        """print("market | gross KRW | fees KRW | net KRW | avg gross bps | hurdle bps")
for row in analysis["historical_markets"]:
    avg = row["average_gross_bps"]
    hurdle = row["estimated_break_even_hurdle_bps"]
    print(f"{row['market']} | {row['gross_pnl_quote']:,.3f} | {row['closed_fees_quote']:,.3f} | {row['closed_net_pnl_quote']:,.3f} | {avg if avg is not None else 'n/a'} | {hurdle if hurdle is not None else 'n/a'}")
assert math.isclose(
    combined["gross_pnl_quote"] - combined["closed_fees_quote"],
    combined["closed_net_pnl_quote"], abs_tol=1e-6,
)
print("\\nD2 operational snapshot")
for row in analysis["d2_instances"]:
    print(f"{row['instance']}: ready={row['ready']}, DD limit={row['max_drawdown_pct']:.0%}, daily limit={row['max_daily_loss_pct']:.0%}, orders_sent={row['orders_sent']}, notifier={row['notifier_enabled']}")
"""
    )
    markdown("## Takeaways")
    code(
        """print("1. Quarantine the old high-churn diagnostic ledger; do not restart it with a larger loss budget.")
print("2. D2 caps accepted entries/round trips at 24 per KST day, enforces a 1-hour durable backoff, reserves full order loss, and rechecks visible execution depth.")
print("3. At/above 10%, risk exits bypass entry spread blocking, the ledger latches halt, and an owner-only diagnostic is materialized/recovered on restart.")
print("4. Automatic code mutation/promotion remains disabled; a candidate must pass causal replay, time split, and 2x fee stress in a new ledger.")
"""
    )
    return {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {"name": "python", "version": "3.11+"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def _execute_notebook(notebook: dict[str, Any], cwd: Path) -> None:
    namespace: dict[str, Any] = {"__name__": "__main__"}
    prior = Path.cwd()
    execution_count = 0
    try:
        os.chdir(cwd)
        for cell in notebook["cells"]:
            if cell["cell_type"] != "code":
                continue
            execution_count += 1
            stream = io.StringIO()
            with contextlib.redirect_stdout(stream):
                exec(
                    compile(
                        cell["source"],
                        f"analysis.ipynb:cell-{execution_count}",
                        "exec",
                    ),
                    namespace,
                )
            cell["execution_count"] = execution_count
            cell["outputs"] = [
                {
                    "name": "stdout",
                    "output_type": "stream",
                    "text": stream.getvalue(),
                }
            ]
    finally:
        os.chdir(prior)


def _won(value: float) -> str:
    sign = "-" if value < 0 else ""
    return f"{sign}₩{abs(value):,.0f}"


def _artifact(analysis: dict[str, Any]) -> dict[str, Any]:
    combined = analysis["combined"]
    historical = analysis["historical_markets"]
    d2 = analysis["d2_instances"]
    loss_rows = []
    market_rows = []
    hurdle_rows = []
    for row in historical:
        loss_rows.extend(
            [
                {
                    "market": row["market"],
                    "component": "taker 수수료",
                    "loss_quote": row["closed_fees_quote"],
                },
                {
                    "market": row["market"],
                    "component": "가격·스프레드",
                    "loss_quote": max(0.0, -row["gross_pnl_quote"]),
                },
            ]
        )
        market_rows.append(
            {
                "market": row["market"],
                "round_trips": row["round_trips"],
                "net_wins": row["net_wins"],
                "gross_pnl_quote": row["gross_pnl_quote"],
                "closed_fees_quote": row["closed_fees_quote"],
                "closed_net_pnl_quote": row["closed_net_pnl_quote"],
                "snapshot_mtm_pnl_quote": row["snapshot_mtm_pnl_quote"],
                "average_hold_seconds": row["average_hold_seconds"],
                "integrity_check": row["integrity_check"],
                "gross_display": _won(row["gross_pnl_quote"]),
                "fees_display": _won(-row["closed_fees_quote"]),
                "net_display": _won(row["closed_net_pnl_quote"]),
                "mtm_display": _won(row["snapshot_mtm_pnl_quote"]),
                "avg_hold_display": (
                    "—"
                    if row["average_hold_seconds"] is None
                    else f"{row['average_hold_seconds']:.1f}초"
                ),
            }
        )
        if row["average_gross_bps"] is not None:
            hurdle_rows.extend(
                [
                    {
                        "market": row["market"],
                        "metric": "실제 평균 gross",
                        "bps": row["average_gross_bps"],
                    },
                    {
                        "market": row["market"],
                        "metric": "추정 손익분기 비용",
                        "bps": row["estimated_break_even_hurdle_bps"],
                    },
                ]
            )
    d2_rows = [
        {
            "instance": row["instance"],
            "market": row["market"],
            "ready": row["ready"],
            "orders_sent": row["orders_sent"],
            "notifier_enabled": row["notifier_enabled"],
            "max_daily_loss_pct": row["max_daily_loss_pct"],
            "max_drawdown_pct": row["max_drawdown_pct"],
            "cooldown_seconds": row["cooldown_seconds"],
            "max_round_trips_per_day": row["max_round_trips_per_day"],
            "risk_display": "일손실 10% / DD 10%",
            "limits_display": "1시간 / 일 24회",
            "safety_display": "READY · simulated · 주문 0",
            "notifier_display": "OFF" if not row["notifier_enabled"] else "ON",
        }
        for row in d2
    ]
    generated = analysis["generated_at"]
    fee_share = combined["fee_share_of_closed_loss"]
    manifest = {
        "version": 1,
        "surface": "report",
        "title": "진단 Shadow 손실 검수와 10% Bounded 정책",
        "description": (
            "동결 원장 손실 원인과 D2 10% 하드스톱 배포 상태를 "
            "분리해 검증한 시뮬레이션 보고서입니다."
        ),
        "generatedAt": generated,
        "sources": [
            {"id": "frozen_ledgers", "label": "Frozen diagnostic ledgers"},
            {"id": "d2_runtime", "label": "D2 bounded runtime snapshot"},
        ],
        "cards": [
            {
                "id": "closed_loss",
                "description": "673개 완료 왕복의 비용 후 실현손익",
                "dataset": "combined_summary",
                "sourceId": "frozen_ledgers",
                "metrics": [
                    {"label": "완료 거래 손익", "field": "closed_loss_display"},
                    {"label": "평가 포함", "field": "mtm_loss_display"},
                ],
            },
            {
                "id": "fee_share",
                "description": "완료 거래 손실 중 실제 기록 수수료 비중",
                "dataset": "combined_summary",
                "sourceId": "frozen_ledgers",
                "metrics": [
                    {"label": "수수료", "field": "fees_display"},
                    {"label": "손실 중 비중", "field": "fee_share_display"},
                ],
            },
            {
                "id": "d2_guard",
                "description": "BTC·ETH·XRP·SOL 별도 모의계좌",
                "dataset": "combined_summary",
                "sourceId": "d2_runtime",
                "metrics": [
                    {"label": "하드 경계", "field": "risk_limit_display"},
                    {"label": "실주문", "field": "orders_display"},
                ],
            },
        ],
        "charts": [
            {
                "id": "loss_components",
                "title": "손실 대부분은 반복 taker 비용",
                "subtitle": (
                    "완료 왕복 기준; 양수 막대는 손실 기여액입니다."
                ),
                "type": "bar",
                "dataset": "loss_components",
                "sourceId": "frozen_ledgers",
                "question": "시장별 손실은 무엇으로 구성됐는가?",
                "rationale": "수수료와 가격·스프레드 손실을 직접 분해합니다.",
                "encodings": {
                    "x": {"field": "market", "type": "nominal", "label": "시장"},
                    "y": {
                        "field": "loss_quote",
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
                },
                "settings": {
                    "groupMode": "stacked",
                    "orientation": "vertical",
                    "showValues": True,
                    "sort": "none",
                },
                "layout": "full",
            },
            {
                "id": "cost_hurdle",
                "title": "신호의 평균 움직임이 비용 허들에 미달",
                "subtitle": "BTC·ETH·SOL 완료 거래; bps, XRP는 거래 없음.",
                "type": "bar",
                "dataset": "cost_hurdle",
                "sourceId": "frozen_ledgers",
                "question": "평균 가격 움직임이 왕복 비용을 이겼는가?",
                "rationale": "실제 gross bps와 수수료+spread proxy를 비교합니다.",
                "encodings": {
                    "x": {"field": "market", "type": "nominal", "label": "시장"},
                    "y": {
                        "field": "bps",
                        "type": "quantitative",
                        "format": "number",
                        "label": "bps",
                    },
                    "color": {"field": "metric", "type": "nominal", "label": "지표"},
                },
                "settings": {
                    "groupMode": "grouped",
                    "orientation": "vertical",
                    "showValues": True,
                    "sort": "none",
                },
                "layout": "full",
            },
        ],
        "tables": [
            {
                "id": "market_review",
                "title": "시장별 동결 원장 검산",
                "dataset": "market_summary",
                "sourceId": "frozen_ledgers",
                "density": "dense",
                "layout": "full",
                "columns": [
                    {"field": "market", "label": "시장", "type": "text"},
                    {"field": "round_trips", "label": "왕복", "type": "number"},
                    {"field": "net_wins", "label": "순이익", "type": "number"},
                    {"field": "gross_display", "label": "gross", "type": "text"},
                    {"field": "fees_display", "label": "수수료", "type": "text"},
                    {"field": "net_display", "label": "실현 순손익", "type": "text"},
                    {"field": "mtm_display", "label": "평가 포함", "type": "text"},
                    {"field": "avg_hold_display", "label": "평균 보유", "type": "text"},
                    {"field": "integrity_check", "label": "DB", "type": "text"},
                ],
            },
            {
                "id": "d2_review",
                "title": "D2 bounded-v1 운영 상태",
                "dataset": "d2_status",
                "sourceId": "d2_runtime",
                "density": "dense",
                "layout": "full",
                "columns": [
                    {"field": "instance", "label": "인스턴스", "type": "text"},
                    {"field": "market", "label": "시장", "type": "text"},
                    {"field": "risk_display", "label": "위험 경계", "type": "text"},
                    {"field": "limits_display", "label": "재진입 제한", "type": "text"},
                    {"field": "safety_display", "label": "상태", "type": "text"},
                    {"field": "notifier_display", "label": "Slack", "type": "text"},
                ],
            },
        ],
        "blocks": [
            {"id": "title", "type": "markdown", "body": "# 진단 Shadow 손실 검수와 10% Bounded 정책"},
            {
                "id": "executive_summary",
                "type": "markdown",
                "sourceId": "frozen_ledgers",
                "body": (
                    "## Executive Summary\n\n기존 진단 원장의 완료 왕복 **673건은 모두 "
                    f"비용 후 손실**이었고 실현손익은 **{_won(combined['closed_net_pnl_quote'])}**입니다. "
                    f"기록 수수료는 **{_won(-combined['closed_fees_quote'])}**, 완료 손실의 **{fee_share:.2%}**였습니다. "
                    "따라서 1%는 손실 원인이 아니라 기존 고빈도 진단 정책을 멈춘 안전선입니다. "
                    "새 10%는 수익 목표가 아닌 모의계좌 하드 위험 경계이며, D2는 별도 새 원장에서만 실행됩니다."
                ),
            },
            {"id": "metrics", "type": "metric-strip", "cardIds": ["closed_loss", "fee_share", "d2_guard"]},
            {"id": "loss_chart", "type": "chart", "chartId": "loss_components", "layout": "full"},
            {"id": "hurdle_chart", "type": "chart", "chartId": "cost_hurdle", "layout": "full"},
            {"id": "market_table", "type": "table", "tableId": "market_review", "layout": "full"},
            {
                "id": "diagnosis",
                "type": "markdown",
                "sourceId": "frozen_ledgers",
                "body": (
                    "## 손실 원인\n\n기존 신호는 L5 호가 불균형과 최근 1초 체결 흐름을 합친 배관 점검용 휴리스틱입니다. "
                    "예상 수익률로 보정되지 않은 채 양방향 taker 체결을 반복했고, BTC·ETH·SOL의 평균 gross 움직임은 수수료와 spread proxy를 이기지 못했습니다. "
                    "단순히 진입 threshold만 높이는 것은 과거 표본에서 순이익 거래를 만들지 못했으므로 개선 근거로 사용하지 않았습니다."
                ),
            },
            {"id": "d2_table", "type": "table", "tableId": "d2_review", "layout": "full"},
            {
                "id": "controls",
                "type": "markdown",
                "sourceId": "d2_runtime",
                "body": (
                    "## 새 10% 하드 경계\n\nD2는 계좌별 일손실과 peak drawdown을 각각 **10% 이상(>=)**에서 감지합니다. "
                    "열린 포지션은 risk exit로 정리한 뒤 신규 진입을 영구 차단하며, owner-only 진단 JSON을 생성합니다. "
                    "재시작 시 누락 보고서를 원장에서 복구하고 halt를 유지합니다. 또한 1시간 cooldown, KST 일 24회 진입/왕복 cap, 전체 주문손실 reserve, 체결시점 spread·visible-depth 재검사를 강제합니다. "
                    "시장 급변이나 gap 때문에 실제 평가손실이 경계를 초과할 가능성까지 제거할 수는 없지만, 새 진입으로 경계를 알고도 넘는 경로는 차단합니다."
                ),
            },
            {
                "id": "next_steps",
                "type": "markdown",
                "body": (
                    "## 다음 조치\n\n1. D2는 소액·저빈도 진단 자료만 수집하고 현재 diagnostic 신호를 수익 전략으로 승격하지 않습니다.\n"
                    "2. 10% halt가 발생하면 해당 원장을 동결하고 비용 분해, causal replay, 시간분할, 기본/2배 비용 stress를 자동 진단 입력으로 사용합니다.\n"
                    "3. 개선 후보는 새 모델 버전·새 원장에서만 시작하며 자동 코드변경과 자동 승격은 금지합니다.\n"
                    "4. 수익형 검증은 기존 C2 forward-paper를 그대로 유지하고 D2와 성과를 섞지 않습니다."
                ),
            },
            {
                "id": "questions",
                "type": "markdown",
                "body": (
                    "## 추가 판단 질문\n\nD2 표본이 충분히 쌓인 뒤 SOL처럼 비용 허들이 큰 시장을 진단 대상에서 제외할지, "
                    "그리고 Slack을 다시 켤 경우 네 시장을 합친 1시간 단일 digest로만 보낼지 결정해야 합니다."
                ),
            },
            {
                "id": "caveats",
                "type": "markdown",
                "sourceId": "frozen_ledgers",
                "body": (
                    "## 한계와 검증 등급\n\n**Share with caveats.** 동결 원장은 DB integrity/FK/PnL 항등식을 통과했지만 BTC·ETH는 약 5시간, XRP·SOL은 약 50분의 짧은 단일 표본입니다. "
                    "spread 비용은 decision 시점 proxy이며 보유 중 가격 변화와 정확히 분리되지 않습니다. SOL에는 열린 포지션 평가손실이 포함돼 완료 거래 손익과 차이가 있습니다. "
                    "D2 통제는 손실 속도와 상한을 제한할 뿐 양의 기대값을 입증하지 않습니다. 모든 결과는 simulated이고 실제 주문은 0건입니다."
                ),
            },
        ],
    }
    combined_row = {
        **combined,
        "closed_loss_display": _won(combined["closed_net_pnl_quote"]),
        "mtm_loss_display": _won(combined["snapshot_mtm_pnl_quote"]),
        "fees_display": _won(-combined["closed_fees_quote"]),
        "fee_share_display": f"{fee_share:.2%}",
        "risk_limit_display": "계좌별 10%",
        "orders_display": "0건",
    }
    source_rows = [
        {
            "market": row["market"],
            "logical_dump_sha256": row["logical_dump_sha256"],
            "as_of_kst": row["as_of_kst"],
        }
        for row in historical
    ]
    return {
        "surface": "report",
        "manifest": manifest,
        "snapshot": {
            "version": 1,
            "generatedAt": generated,
            "status": "ready",
            "datasets": {
                "combined_summary": [combined_row],
                "loss_components": loss_rows,
                "cost_hurdle": hurdle_rows,
                "market_summary": market_rows,
                "d2_status": d2_rows,
            },
        },
        "sources": [
            {
                "id": "frozen_ledgers",
                "label": "Frozen diagnostic BTC/ETH/XRP/SOL ledgers",
                "query": {
                    "engine": "SQLite",
                    "description": "artifacts/diagnostic-v2/review-2026-07-22/source 아래 동결 원장 4개를 동일 SQL로 집계했습니다.",
                    "executed_at": generated,
                    "sql": ROUND_TRIP_SQL.strip(),
                    "tables_used": ["shadow_runs", "shadow_state", "shadow_decisions", "shadow_orders", "shadow_fills"],
                    "filters": ["시장별 시간순 buy/sell 순번 pairing", "모든 fill은 simulated", "완료 왕복과 snapshot MTM을 분리"],
                    "metric_definitions": ["net PnL = sell quote - sell fee - buy quote - buy fee", "fee share = closed fees / absolute closed net loss", "10% = account risk ceiling, not return target"],
                    "source_hashes": source_rows,
                },
            },
            {
                "id": "d2_runtime",
                "label": "D2 bounded-v1 point-in-time runtime snapshot",
                "query": {
                    "engine": "localhost health + SQLite",
                    "description": "네 D2 localhost readiness, owner-only config/runtime, isolated ledger를 교차 확인했습니다.",
                    "executed_at": generated,
                    "sql": "SELECT r.run_id, r.status, r.code_version, s.lifecycle_status, s.cash_quote, s.base_quantity, s.realized_pnl_quote, s.cumulative_fees_quote, s.last_equity_quote, s.max_drawdown, s.halt_reason FROM shadow_runs AS r JOIN shadow_state AS s USING (run_id) ORDER BY r.started_wall_ns DESC LIMIT 1;",
                    "tables_used": ["shadow_runs", "shadow_state", "shadow_decisions", "shadow_orders", "shadow_fills"],
                    "filters": ["d2-btc", "d2-eth", "d2-xrp", "d2-sol"],
                    "metric_definitions": ["ready = localhost readiness true", "orders_sent must equal 0", "notifier_enabled must be false"],
                },
            },
        ],
    }


def main() -> int:
    args = _args()
    source_dir = args.source_dir.expanduser().resolve(strict=True)
    output_dir = args.output_dir.expanduser().resolve(strict=False)
    output_dir.mkdir(parents=True, exist_ok=True)
    analysis = _analysis(source_dir, args.instances_root.expanduser().resolve())
    _atomic_json(output_dir / "analysis.json", analysis)
    notebook = _notebook(analysis)
    _execute_notebook(notebook, output_dir)
    _atomic_json(output_dir / "analysis.ipynb", notebook)
    artifact = _artifact(analysis)
    _atomic_json(output_dir / "artifact.json", artifact)
    validation = {
        "overall_assessment": "Share with caveats",
        "generated_at": analysis["generated_at"],
        "checks": analysis["checks"],
        "calculation_spot_checks": {
            "gross_minus_fees_equals_closed_net": True,
            "closed_trade_count": analysis["combined"]["round_trips"],
            "closed_net_winners": analysis["combined"]["net_wins"],
            "orders_sent": 0,
        },
        "required_caveats": [
            "Short single-session historical sample",
            "Spread decomposition is a proxy",
            "D2 risk controls do not prove positive expectancy",
            "All executions are simulated",
        ],
    }
    _atomic_json(output_dir / "validation.json", validation)
    print(json.dumps({
        "analysis": "analysis.json",
        "notebook": "analysis.ipynb",
        "artifact": "artifact.json",
        "validation": "validation.json",
        "checks": analysis["checks"],
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
