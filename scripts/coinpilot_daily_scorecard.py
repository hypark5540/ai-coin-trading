#!/usr/bin/env python3
"""Read-only D2/C2 daily scorecards with isolated Slack delivery state.

The eight approved trading ledgers are opened with SQLite ``mode=ro`` and
``query_only``.  Report artifacts and delivery receipts live under a separate
reporting home; this process never writes a trading ledger or its outbox.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import math
import os
import re
import sqlite3
import stat
import subprocess
import sys
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence
from zoneinfo import ZoneInfo


NANOSECONDS = 1_000_000_000
REPORT_SCHEMA_VERSION = 1
REPORT_ID_PREFIX = "daily-scorecard:v1"
SEOUL = ZoneInfo("Asia/Seoul")
UTC = timezone.utc
DEFAULT_SCHEDULE_HOUR = 0
DEFAULT_SCHEDULE_MINUTE = 10
DEFAULT_RETRY_SECONDS = 15 * 60
MAX_RETRY_SECONDS = 6 * 60 * 60
DELIVERY_LEASE_SECONDS = 5 * 60
POSITION_EPSILON = 1e-12

D2_INSTANCES = {
    "d2-btc": "KRW-BTC",
    "d2-eth": "KRW-ETH",
    "d2-xrp": "KRW-XRP",
    "d2-sol": "KRW-SOL",
}
C2_INSTANCES = {
    "c2-btc": "KRW-BTC",
    "c2-eth": "KRW-ETH",
    "c2-xrp": "KRW-XRP",
    "c2-sol": "KRW-SOL",
}
APPROVED_INSTANCES = {**D2_INSTANCES, **C2_INSTANCES}
SHADOW_SUMMARY_TOPICS = {"shadow_hourly_summary", "shadow_daily_summary"}
FORBIDDEN_CONFIG_KEYS = {
    "access_key",
    "access_key_id",
    "api_key",
    "api_secret",
    "private_key",
    "secret_key",
    "webhook_url",
}


class ScorecardError(RuntimeError):
    """Raised when a report or delivery boundary cannot be trusted."""


class SlackDeliveryError(ScorecardError):
    def __init__(
        self,
        message: str,
        *,
        retry_after_seconds: float | None = None,
    ) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


class SlackClient(Protocol):
    def send(self, message: Mapping[str, Any]) -> None: ...


@dataclass(frozen=True, slots=True)
class ReportWindow:
    report_date: date
    start_wall_ns: int
    end_wall_ns: int

    @property
    def start_local(self) -> datetime:
        return datetime.fromtimestamp(self.start_wall_ns / NANOSECONDS, SEOUL)

    @property
    def end_local(self) -> datetime:
        return datetime.fromtimestamp(self.end_wall_ns / NANOSECONDS, SEOUL)

    @property
    def report_id(self) -> str:
        return f"{REPORT_ID_PREFIX}:{self.report_date.isoformat()}"

    @property
    def label(self) -> str:
        return f"{self.report_date.isoformat()} 00:00–24:00 KST"


@dataclass(frozen=True, slots=True)
class FillRecord:
    fill_id: str
    timestamp_ns: int
    side: str
    quantity: float
    notional_quote: float
    fee_quote: float
    slippage_quote: float | None = None
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class CompletedTrade:
    opened_wall_ns: int
    closed_wall_ns: int
    pnl_quote: float
    holding_seconds: float
    exit_reason: str | None


@dataclass(frozen=True, slots=True)
class RealizedEvent:
    timestamp_ns: int
    pnl_quote: float


def _require_finite(value: object, name: str, *, minimum: float | None = None) -> float:
    if isinstance(value, bool):
        raise ScorecardError(f"{name} must be numeric")
    try:
        parsed = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError, OverflowError) as exc:
        raise ScorecardError(f"{name} must be numeric") from exc
    if not math.isfinite(parsed) or (minimum is not None and parsed < minimum):
        raise ScorecardError(f"{name} is outside its valid range")
    return parsed


def _required_text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ScorecardError(f"{name} must be a non-empty string")
    return value.strip()


def _required_mapping(value: object, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ScorecardError(f"{name} must be an object")
    return value


def _optional_bool_matches(
    value: Mapping[str, Any],
    key: str,
    expected: bool,
) -> bool:
    """Accept a missing legacy attestation, but never a contradictory one.

    The currently installed D2 health rows and C2 frozen manifests predate the
    explicit ``simulated``/``own_execution`` fields.  Their immutable installed
    code/config identity is the primary simulation-only proof.  If a newer
    runtime does emit either field, it must agree with that contract.
    """

    return key not in value or value.get(key) is expected


def _wall_ns(value: datetime) -> int:
    if value.tzinfo is None:
        raise ScorecardError("wall-clock timestamps must include a timezone")
    return int(value.timestamp() * NANOSECONDS)


def _iso_to_wall_ns(value: object, name: str) -> int:
    text = _required_text(value, name)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ScorecardError(f"{name} is not an ISO timestamp") from exc
    if parsed.tzinfo is None:
        raise ScorecardError(f"{name} must include a timezone")
    return _wall_ns(parsed)


def _iso_utc(wall_ns: int) -> str:
    return datetime.fromtimestamp(wall_ns / NANOSECONDS, UTC).isoformat()


def window_for_date(report_date: date) -> ReportWindow:
    start = datetime.combine(report_date, datetime_time.min, tzinfo=SEOUL)
    end = start + timedelta(days=1)
    return ReportWindow(report_date, _wall_ns(start), _wall_ns(end))


def latest_due_window(
    now_wall_ns: int,
    *,
    schedule_hour: int = DEFAULT_SCHEDULE_HOUR,
    schedule_minute: int = DEFAULT_SCHEDULE_MINUTE,
) -> ReportWindow:
    if isinstance(now_wall_ns, bool) or not isinstance(now_wall_ns, int):
        raise ScorecardError("now_wall_ns must be an integer")
    if not 0 <= schedule_hour <= 23 or not 0 <= schedule_minute <= 59:
        raise ScorecardError("invalid daily schedule")
    now_local = datetime.fromtimestamp(now_wall_ns / NANOSECONDS, SEOUL)
    due_today = now_local.replace(
        hour=schedule_hour,
        minute=schedule_minute,
        second=0,
        microsecond=0,
    )
    completed_date = now_local.date() - timedelta(
        days=1 if now_local >= due_today else 2
    )
    return window_for_date(completed_date)


def _canonical_json(value: object, *, pretty: bool = False) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        indent=2 if pretty else None,
        separators=None if pretty else (",", ":"),
        allow_nan=False,
    )


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _contains_forbidden_key(value: object) -> bool:
    if isinstance(value, Mapping):
        for key, child in value.items():
            normalized = re.sub(
                r"[^a-z0-9]+",
                "_",
                str(key).strip().lower(),
            ).strip("_")
            if any(
                normalized == forbidden
                or normalized.endswith(f"_{forbidden}")
                for forbidden in FORBIDDEN_CONFIG_KEYS
            ):
                return True
            if _contains_forbidden_key(child):
                return True
    elif isinstance(value, (list, tuple)):
        return any(_contains_forbidden_key(child) for child in value)
    return False


def _reject_symlink_components(path: Path) -> None:
    current = path.absolute()
    while True:
        if current.exists() and current.is_symlink():
            raise ScorecardError(f"symlinked source path is not allowed: {path.name}")
        if current == current.parent:
            return
        current = current.parent


def _within(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def _validate_reporting_paths(
    *,
    root: Path,
    state_dir: Path,
    artifact_dir: Path,
) -> tuple[Path, Path, Path]:
    selected_root = root.expanduser().absolute()
    selected_state = state_dir.expanduser().absolute()
    selected_artifact = artifact_dir.expanduser().absolute()
    for path in (selected_root, selected_state, selected_artifact):
        _reject_symlink_components(path)
    expected_home = selected_root / "reporting"
    if selected_state != expected_home / "state":
        raise ScorecardError("state directory must be exactly Coinpilot/reporting/state")
    if selected_artifact != expected_home / "reports":
        raise ScorecardError("artifact directory must be exactly Coinpilot/reporting/reports")
    instances = selected_root / "instances"
    if _within(selected_state, instances) or _within(selected_artifact, instances):
        raise ScorecardError("reporting output overlaps a trading instance")
    return selected_root, selected_state, selected_artifact


def open_readonly_sqlite(path: Path, *, timeout_seconds: float = 5.0) -> sqlite3.Connection:
    candidate = path.expanduser().absolute()
    _reject_symlink_components(candidate)
    resolved = candidate.resolve()
    if not resolved.is_file():
        raise ScorecardError(f"source database is missing: {resolved.name}")
    connection = sqlite3.connect(
        f"{resolved.as_uri()}?mode=ro",
        uri=True,
        timeout=timeout_seconds,
        isolation_level=None,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only = ON")
    connection.execute(f"PRAGMA busy_timeout = {int(timeout_seconds * 1000)}")
    # All per-ledger queries must share one WAL snapshot. Without an explicit
    # read transaction, an active writer could commit between state and fill
    # reads and create a false reconciliation failure.
    connection.execute("BEGIN")
    return connection


def _database_checks(connection: sqlite3.Connection, *, foreign_keys: bool) -> dict[str, Any]:
    quick = connection.execute("PRAGMA quick_check").fetchone()
    quick_result = None if quick is None else str(quick[0])
    foreign_key_errors: int | None = None
    if foreign_keys:
        foreign_key_errors = len(connection.execute("PRAGMA foreign_key_check").fetchall())
    return {
        "quick_check": quick_result,
        "foreign_key_errors": foreign_key_errors,
        "ok": quick_result == "ok" and foreign_key_errors in (None, 0),
    }


def _safe_error(exc: BaseException) -> str:
    message = " ".join(str(exc).split())
    message = re.sub(r"https://hooks\.slack\.com/\S+", "[redacted]", message)
    return f"{type(exc).__name__}: {message[:240]}"


def _trade_replay(fills: Sequence[FillRecord], *, end_wall_ns: int) -> dict[str, Any]:
    quantity = 0.0
    cost_basis = 0.0
    opened_wall_ns: int | None = None
    open_trade_pnl = 0.0
    trades: list[CompletedTrade] = []
    realized_events: list[RealizedEvent] = []
    errors: list[str] = []
    prior_key: tuple[int, str] | None = None

    for fill in fills:
        if fill.timestamp_ns >= end_wall_ns:
            break
        key = (fill.timestamp_ns, fill.fill_id)
        if prior_key is not None and key <= prior_key:
            errors.append("fill_order_not_strictly_increasing")
        prior_key = key
        try:
            qty = _require_finite(fill.quantity, "fill.quantity", minimum=0.0)
            notional = _require_finite(
                fill.notional_quote,
                "fill.notional_quote",
                minimum=0.0,
            )
            fee = _require_finite(fill.fee_quote, "fill.fee_quote", minimum=0.0)
        except ScorecardError as exc:
            errors.append(str(exc))
            continue
        if qty <= POSITION_EPSILON or notional <= 0:
            errors.append("non_positive_fill")
            continue
        if fill.side == "buy":
            if quantity <= POSITION_EPSILON:
                opened_wall_ns = fill.timestamp_ns
                open_trade_pnl = 0.0
            quantity += qty
            cost_basis += notional + fee
        elif fill.side == "sell":
            if quantity <= POSITION_EPSILON or qty > quantity + POSITION_EPSILON:
                errors.append("sell_without_sufficient_inventory")
                continue
            prior_quantity = quantity
            allocated_cost = cost_basis * min(1.0, qty / prior_quantity)
            realized = (notional - fee) - allocated_cost
            realized_events.append(
                RealizedEvent(timestamp_ns=fill.timestamp_ns, pnl_quote=realized)
            )
            open_trade_pnl += realized
            quantity = max(0.0, prior_quantity - qty)
            cost_basis = max(0.0, cost_basis - allocated_cost)
            if quantity <= POSITION_EPSILON:
                if opened_wall_ns is None:
                    errors.append("closed_trade_without_open_time")
                else:
                    trades.append(
                        CompletedTrade(
                            opened_wall_ns=opened_wall_ns,
                            closed_wall_ns=fill.timestamp_ns,
                            pnl_quote=open_trade_pnl,
                            holding_seconds=max(
                                0.0,
                                (fill.timestamp_ns - opened_wall_ns) / NANOSECONDS,
                            ),
                            exit_reason=fill.reason,
                        )
                    )
                quantity = 0.0
                cost_basis = 0.0
                opened_wall_ns = None
                open_trade_pnl = 0.0
        else:
            errors.append("unsupported_fill_side")

    return {
        "trades": trades,
        "realized_events": realized_events,
        "end_quantity": quantity,
        "end_cost_basis_quote": cost_basis,
        "pairing_reliable": not errors,
        "pairing_errors": sorted(set(errors)),
    }


def _period_fill_metrics(
    fills: Sequence[FillRecord],
    *,
    start_wall_ns: int,
    end_wall_ns: int,
) -> dict[str, Any]:
    replay = _trade_replay(fills, end_wall_ns=end_wall_ns)
    selected_fills = [
        fill
        for fill in fills
        if start_wall_ns <= fill.timestamp_ns < end_wall_ns
    ]
    selected_trades = [
        trade
        for trade in replay["trades"]
        if start_wall_ns <= trade.closed_wall_ns < end_wall_ns
    ]
    selected_realized = [
        event
        for event in replay["realized_events"]
        if start_wall_ns <= event.timestamp_ns < end_wall_ns
    ]
    accounting_pnl = sum(event.pnl_quote for event in selected_realized)
    round_trip_pnl = sum(trade.pnl_quote for trade in selected_trades)
    wins = sum(trade.pnl_quote > 1e-9 for trade in selected_trades)
    losses = sum(trade.pnl_quote < -1e-9 for trade in selected_trades)
    flats = len(selected_trades) - wins - losses
    reliable = bool(replay["pairing_reliable"])
    return {
        "fill_count": len(selected_fills),
        "buy_count": sum(fill.side == "buy" for fill in selected_fills),
        "sell_count": sum(fill.side == "sell" for fill in selected_fills),
        "turnover_quote": sum(fill.notional_quote for fill in selected_fills),
        "fees_quote": sum(fill.fee_quote for fill in selected_fills),
        "slippage_quote": (
            None
            if any(fill.slippage_quote is None for fill in selected_fills)
            else sum(float(fill.slippage_quote or 0.0) for fill in selected_fills)
        ),
        "completed_trades": len(selected_trades) if reliable else None,
        # Accounting PnL is recognized on every sell, including a partial
        # close.  Round-trip PnL and win rate remain attributed to the final
        # sell that returns the position to flat.
        "realized_pnl_quote": accounting_pnl if reliable else None,
        "accounting_realized_pnl_quote": accounting_pnl if reliable else None,
        "completed_round_trip_pnl_quote": round_trip_pnl if reliable else None,
        "wins": wins if reliable else None,
        "losses": losses if reliable else None,
        "flats": flats if reliable else None,
        "win_rate": (
            wins / len(selected_trades)
            if reliable and selected_trades
            else None
        ),
        "average_holding_seconds": (
            sum(trade.holding_seconds for trade in selected_trades)
            / len(selected_trades)
            if reliable and selected_trades
            else None
        ),
        "pairing_reliable": reliable,
        "pairing_errors": replay["pairing_errors"],
    }


def _metric_windows(fills: Sequence[FillRecord], window: ReportWindow) -> dict[str, Any]:
    day_ns = 24 * 60 * 60 * NANOSECONDS
    earliest = min((fill.timestamp_ns for fill in fills), default=None)
    return {
        "daily": _period_fill_metrics(
            fills,
            start_wall_ns=window.start_wall_ns,
            end_wall_ns=window.end_wall_ns,
        ),
        "previous_day": _period_fill_metrics(
            fills,
            start_wall_ns=window.start_wall_ns - day_ns,
            end_wall_ns=window.start_wall_ns,
        ),
        "trailing_7d": _period_fill_metrics(
            fills,
            start_wall_ns=window.end_wall_ns - 7 * day_ns,
            end_wall_ns=window.end_wall_ns,
        ),
        "lineage": _period_fill_metrics(
            fills,
            start_wall_ns=0,
            end_wall_ns=window.end_wall_ns,
        ),
        "first_fill_wall_ns": earliest,
        "first_fill_iso": None if earliest is None else _iso_utc(earliest),
    }


def _read_runtime_env(path: Path) -> dict[str, str]:
    _reject_symlink_components(path)
    result: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        result[key.strip()] = value.strip()
    return result


def _run_json_identity(
    arguments: Sequence[str],
    *,
    environment: Mapping[str, str] | None = None,
    cwd: Path,
) -> dict[str, Any]:
    selected_environment = {
        "PATH": "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin",
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    if environment:
        selected_environment.update(environment)
    try:
        result = subprocess.run(
            list(arguments),
            cwd=cwd,
            env=selected_environment,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
            timeout=30,
        )
        payload = json.loads(result.stdout)
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError) as exc:
        raise ScorecardError("installed runtime identity verification failed") from exc
    if not isinstance(payload, dict):
        raise ScorecardError("installed runtime identity is not a JSON object")
    return payload


def _d2_installed_identity(
    instance_home: Path,
    config_path: Path,
    runtime: Mapping[str, str],
    *,
    bounded_deployment: bool,
) -> dict[str, Any]:
    interpreter = instance_home / "venv" / "bin" / "python"
    service_wrapper = instance_home / "bin" / "coinpilot-service"
    bounded_helper = instance_home / "bin" / "coinpilot-bounded-shadow"
    for path in (service_wrapper, bounded_helper):
        _reject_symlink_components(path)
        if not path.is_file():
            raise ScorecardError("installed D2 runtime helper is missing")
    if not interpreter.is_file() or not os.access(interpreter, os.X_OK):
        raise ScorecardError("installed D2 interpreter is missing")
    deployment_label = ":".join(
        (
            "bounded-shadow-v1",
            _sha256_bytes(service_wrapper.read_bytes()),
            _sha256_bytes(bounded_helper.read_bytes()),
            runtime.get("COINPILOT_SHADOW_COOLDOWN_SECONDS", "3600"),
            runtime.get("COINPILOT_SHADOW_MAX_ROUND_TRIPS_PER_DAY", "24"),
            runtime.get("COINPILOT_SHADOW_RESERVE_FULL_ORDER_LOSS", "1"),
            runtime.get("COINPILOT_SHADOW_EXECUTION_SPREAD_RECHECK", "1"),
        )
    )
    program = """
import json
import sys
from coinpilot.config import load_config
from coinpilot.shadow_service import runtime_shadow_config, shadow_deployment_version

config = load_config(sys.argv[1])
runtime = runtime_shadow_config(config)
print(json.dumps({
    "config": runtime.to_dict(),
    "config_hash": runtime.config_hash,
    "code_version": shadow_deployment_version(config),
}, sort_keys=True))
"""
    environment = (
        {"COINPILOT_CODE_VERSION": deployment_label}
        if bounded_deployment
        else None
    )
    return _run_json_identity(
        (str(interpreter), "-c", program, str(config_path)),
        environment=environment,
        cwd=instance_home,
    )


def _installed_package_root(instance_home: Path) -> Path:
    roots = tuple(
        sorted(
            (instance_home / "venv" / "lib").glob(
                "python*/site-packages/coinpilot"
            )
        )
    )
    if len(roots) != 1 or not roots[0].is_dir():
        raise ScorecardError("installed C2 package root is not unique")
    root = roots[0]
    _reject_symlink_components(root)
    return root


def _c2_installed_identity(
    instance_home: Path,
    config_path: Path,
    manifest_path: Path,
) -> dict[str, Any]:
    interpreter = instance_home / "venv" / "bin" / "python"
    helper = instance_home / "bin" / "coinpilot-c2-config"
    package_root = _installed_package_root(instance_home)
    _reject_symlink_components(helper)
    if not interpreter.is_file() or not os.access(interpreter, os.X_OK):
        raise ScorecardError("installed C2 interpreter is missing")
    if not helper.is_file():
        raise ScorecardError("installed C2 verification helper is missing")
    payload = _run_json_identity(
        (
            str(interpreter),
            str(helper),
            "verify-installed",
            "--config",
            str(config_path),
            "--manifest",
            str(manifest_path),
            "--installed-package-root",
            str(package_root),
        ),
        cwd=instance_home,
    )
    if payload.get("valid") is not True:
        raise ScorecardError("installed C2 runtime verification is not valid")
    return payload


def _table_names(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }


def _latest_d2_state(connection: sqlite3.Connection, market: str) -> sqlite3.Row:
    row = connection.execute(
        """
        SELECT r.run_id, r.schema_version, r.market, r.status AS run_status,
               r.started_wall_ns, r.ended_wall_ns, r.restart_of_run_id,
               r.config_hash, r.code_version, r.halt_reason AS run_halt_reason,
               s.revision, s.lifecycle_status, s.cash_quote, s.base_quantity,
               s.average_cost_quote,
               s.realized_pnl_quote, s.cumulative_fees_quote,
               s.last_equity_quote, s.peak_equity_quote, s.max_drawdown,
               s.warmup_books_seen, s.last_book_wall_ns,
               s.halt_reason, s.updated_wall_ns
        FROM shadow_runs AS r
        JOIN shadow_state AS s ON s.run_id = r.run_id
        WHERE r.market = ?
        ORDER BY r.started_wall_ns DESC, r.rowid DESC
        LIMIT 1
        """,
        (market,),
    ).fetchone()
    if row is None:
        raise ScorecardError("D2 ledger has no state for the expected market")
    return row


def _d2_lineage(
    connection: sqlite3.Connection,
    *,
    market: str,
    latest_run_id: str,
) -> list[sqlite3.Row]:
    rows = connection.execute(
        """
        SELECT run_id, market, started_wall_ns, restart_of_run_id,
               config_hash, config_json, code_version
        FROM shadow_runs
        ORDER BY started_wall_ns, rowid
        """
    ).fetchall()
    by_id = {str(row["run_id"]): row for row in rows}
    market_rows = [row for row in rows if row["market"] == market]
    chain: list[sqlite3.Row] = []
    seen: set[str] = set()
    selected: str | None = latest_run_id
    newer_started: int | None = None
    while selected is not None:
        if selected in seen:
            raise ScorecardError("D2 restart lineage contains a cycle")
        row = by_id.get(selected)
        if row is None or row["market"] != market:
            raise ScorecardError("D2 restart lineage predecessor is missing or cross-market")
        started = int(row["started_wall_ns"])
        if newer_started is not None and started >= newer_started:
            raise ScorecardError("D2 restart lineage time ordering is invalid")
        seen.add(selected)
        chain.append(row)
        newer_started = started
        predecessor = row["restart_of_run_id"]
        selected = None if predecessor is None else str(predecessor)
    if len(chain) != len(market_rows):
        raise ScorecardError("D2 ledger contains a branch or orphan run")
    chain.reverse()
    return chain


def _d2_equity_metrics(
    connection: sqlite3.Connection,
    lineage_run_ids: Sequence[str],
    window: ReportWindow,
) -> dict[str, Any]:
    placeholders = ", ".join("?" for _ in lineage_run_ids)
    anchor = connection.execute(
        f"""
        SELECT e.equity_quote, e.cash_quote, e.base_quantity, e.drawdown,
               e.created_wall_ns
        FROM shadow_equity AS e
        WHERE e.run_id IN ({placeholders}) AND e.created_wall_ns <= ?
        ORDER BY e.created_wall_ns DESC, e.rowid DESC
        LIMIT 1
        """,
        (*lineage_run_ids, window.start_wall_ns),
    ).fetchone()
    rows = connection.execute(
        f"""
        SELECT e.equity_quote, e.cash_quote, e.base_quantity, e.drawdown,
               e.created_wall_ns
        FROM shadow_equity AS e
        WHERE e.run_id IN ({placeholders})
          AND e.created_wall_ns > ? AND e.created_wall_ns < ?
        ORDER BY e.created_wall_ns, e.rowid
        """,
        (*lineage_run_ids, window.start_wall_ns, window.end_wall_ns),
    ).fetchall()
    samples = ([anchor] if anchor is not None else []) + list(rows)
    if not rows:
        return {
            "available": False,
            "full_window": False,
            "valuation_quality": "unavailable_without_in_window_samples",
            "start_equity_quote": None,
            "end_equity_quote": None,
            "change_quote": None,
            "return": None,
            "sampled_daily_drawdown": None,
            "sample_count": 0,
            "start_boundary_lag_seconds": None,
            "end_boundary_lag_seconds": None,
        }
    first = anchor if anchor is not None else rows[0]
    start_equity = _require_finite(
        first["equity_quote"],
        "D2 start equity",
        minimum=0.0,
    )
    end_equity = _require_finite(
        rows[-1]["equity_quote"],
        "D2 end equity",
        minimum=0.0,
    )
    peak = start_equity
    max_drawdown = 0.0
    for row in samples:
        equity = _require_finite(
            row["equity_quote"],
            "D2 equity sample",
            minimum=0.0,
        )
        peak = max(peak, equity)
        if peak > 0:
            max_drawdown = max(max_drawdown, 1.0 - equity / peak)
    return {
        "available": True,
        "full_window": anchor is not None,
        "valuation_quality": (
            "sampled_shadow_equity_with_boundary_anchor"
            if anchor is not None
            else "partial_first_in_window_sample"
        ),
        "start_equity_quote": start_equity,
        "end_equity_quote": end_equity,
        "change_quote": end_equity - start_equity,
        "return": (
            (end_equity - start_equity) / start_equity
            if start_equity > 0
            else None
        ),
        "sampled_daily_drawdown": max_drawdown,
        "sample_count": len(samples),
        "start_sample_wall_ns": int(first["created_wall_ns"]),
        "end_sample_wall_ns": int(rows[-1]["created_wall_ns"]),
        "start_boundary_lag_seconds": (
            window.start_wall_ns - int(first["created_wall_ns"])
            if anchor is not None
            else int(first["created_wall_ns"]) - window.start_wall_ns
        ) / NANOSECONDS,
        "end_boundary_lag_seconds": (
            window.end_wall_ns - int(rows[-1]["created_wall_ns"])
        ) / NANOSECONDS,
    }


def _aggregate_d2(
    *,
    root: Path,
    instance: str,
    expected_market: str,
    window: ReportWindow,
    generated_wall_ns: int,
) -> dict[str, Any]:
    instance_home = root / "instances" / instance
    config_path = instance_home / "config" / "config.toml"
    runtime_path = instance_home / "config" / "runtime.env"
    for source_path in (instance_home, config_path, runtime_path):
        _reject_symlink_components(source_path)
    config = tomllib.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(config, Mapping):
        raise ScorecardError("D2 config must be an object")
    runtime = _read_runtime_env(runtime_path)
    if _contains_forbidden_key(config) or _contains_forbidden_key(runtime):
        raise ScorecardError("D2 config contains a forbidden credential field")
    data = _required_mapping(config.get("data"), "D2 data config")
    operations_config = _required_mapping(
        config.get("operations"), "D2 operations config"
    )
    if data.get("market") != expected_market:
        raise ScorecardError("D2 market does not match the approved allowlist")
    shadow = config.get("shadow", {})
    if not isinstance(shadow, Mapping):
        raise ScorecardError("D2 shadow config is missing")
    database_candidate = Path(
        _required_text(shadow.get("database_path"), "shadow.database_path")
    ).expanduser()
    if not database_candidate.is_absolute():
        raise ScorecardError("D2 database path must be absolute")
    _reject_symlink_components(database_candidate)
    database_path = database_candidate.resolve()
    if not _within(database_path, (instance_home / "data").resolve()):
        raise ScorecardError("D2 database escapes its instance data directory")
    if database_path.name == "shadow.db":
        raise ScorecardError("legacy shadow.db is not an approved D2 ledger")
    bounded_ok = (
        shadow.get("mode") == "diagnostic"
        and str(shadow.get("model_version", "")).startswith("diagnostic-bounded-")
        and float(shadow.get("max_daily_loss_pct", -1)) == 0.10
        and float(shadow.get("max_drawdown_pct", -1)) == 0.10
        and runtime.get("COINPILOT_BOUNDED_SHADOW") == "1"
        and runtime.get("COINPILOT_ENABLE_SHADOW") == "1"
        and runtime.get("COINPILOT_ENABLE_PAPER") == "0"
        and runtime.get("COINPILOT_ENABLE_NOTIFIER") == "0"
    )
    observe_ok = (
        shadow.get("mode") == "observe"
        and shadow.get("model_version") == "observe-public-feed-v1"
        and runtime.get("COINPILOT_BOUNDED_SHADOW") == "0"
        and runtime.get("COINPILOT_ENABLE_SHADOW") == "1"
        and runtime.get("COINPILOT_ENABLE_PAPER") == "0"
        and runtime.get("COINPILOT_ENABLE_NOTIFIER") == "0"
    )
    runtime_profile = (
        "bounded_diagnostic"
        if bounded_ok
        else "public_feed_observe"
        if observe_ok
        else "unapproved"
    )
    installed_identity = _d2_installed_identity(
        instance_home,
        config_path,
        runtime,
        bounded_deployment=(runtime.get("COINPILOT_BOUNDED_SHADOW") == "1"),
    )

    connection = open_readonly_sqlite(database_path)
    try:
        expected_tables = {
            "shadow_runs",
            "shadow_state",
            "shadow_decisions",
            "shadow_orders",
            "shadow_fills",
            "shadow_equity",
            "shadow_health",
            "notification_outbox",
        }
        if not expected_tables.issubset(_table_names(connection)):
            raise ScorecardError("D2 ledger schema is incomplete")
        database_health = _database_checks(connection, foreign_keys=True)
        latest = _latest_d2_state(connection, expected_market)
        for field, minimum in (
            ("cash_quote", 0.0),
            ("base_quantity", 0.0),
            ("average_cost_quote", 0.0),
            ("realized_pnl_quote", None),
            ("cumulative_fees_quote", 0.0),
            ("last_equity_quote", 0.0),
            ("peak_equity_quote", 0.0),
            ("max_drawdown", 0.0),
        ):
            _require_finite(
                latest[field],
                f"D2 state {field}",
                minimum=minimum,
            )
        lineage_rows = _d2_lineage(
            connection,
            market=expected_market,
            latest_run_id=str(latest["run_id"]),
        )
        lineage_run_ids = [str(row["run_id"]) for row in lineage_rows]
        placeholders = ", ".join("?" for _ in lineage_run_ids)
        fills = [
            FillRecord(
                fill_id=str(row["fill_id"]),
                timestamp_ns=int(row["created_wall_ns"]),
                side=str(row["side"]),
                quantity=float(row["filled_base"]),
                notional_quote=float(row["filled_quote"]),
                fee_quote=float(row["fee_quote"]),
            )
            for row in connection.execute(
                f"""
                SELECT f.fill_id, f.created_wall_ns, f.side, f.filled_base,
                       f.filled_quote, f.fee_quote
                FROM shadow_fills AS f
                WHERE f.run_id IN ({placeholders})
                ORDER BY f.created_wall_ns, f.fill_id
                """,
                lineage_run_ids,
            ).fetchall()
        ]
        metrics = _metric_windows(fills, window)
        replay_end = max((fill.timestamp_ns for fill in fills), default=0) + 1
        current_metrics = _period_fill_metrics(
            fills,
            start_wall_ns=0,
            end_wall_ns=replay_end,
        )
        current_replay = _trade_replay(fills, end_wall_ns=replay_end)
        equity = _d2_equity_metrics(connection, lineage_run_ids, window)
        decision_count = int(
            connection.execute(
                f"""
                SELECT COUNT(*) FROM shadow_decisions
                WHERE run_id IN ({placeholders})
                """,
                lineage_run_ids,
            ).fetchone()[0]
        )
        pending_orders = int(
            connection.execute(
                f"""
                SELECT COUNT(*) FROM shadow_orders
                WHERE run_id IN ({placeholders}) AND status = 'pending'
                """,
                lineage_run_ids,
            ).fetchone()[0]
        )
        health_row = connection.execute(
            """
            SELECT status, observed_wall_ns, details_json
            FROM shadow_health
            WHERE run_id = ? AND component = 'shadow_engine'
            ORDER BY observed_wall_ns DESC, rowid DESC LIMIT 1
            """,
            (latest["run_id"],),
        ).fetchone()
        health_details = {} if health_row is None else json.loads(health_row["details_json"])
        if not isinstance(health_details, Mapping):
            raise ScorecardError("D2 health details must be an object")
        operational = connection.execute(
            f"""
            SELECT
              COALESCE(SUM(severity = 'warning'), 0) AS warnings,
              COALESCE(SUM(severity = 'critical'), 0) AS criticals,
              COALESCE(SUM(topic = 'shadow_feed_continuity'), 0) AS continuity,
              COALESCE(SUM(topic = 'shadow_halted'), 0) AS halts,
              COALESCE(SUM(topic = 'shadow_restart_recovery'), 0) AS recoveries
            FROM notification_outbox AS n
            WHERE (n.run_id IN ({placeholders}) OR n.run_id IS NULL)
              AND n.topic NOT IN (?, ?)
              AND n.created_wall_ns >= ? AND n.created_wall_ns < ?
            """,
            (
                *lineage_run_ids,
                *sorted(SHADOW_SUMMARY_TOPICS),
                window.start_wall_ns,
                window.end_wall_ns,
            ),
        ).fetchone()
        pending_outbox = connection.execute(
            f"""
            SELECT
              COUNT(*) AS pending,
              COALESCE(SUM(severity = 'warning'), 0) AS warnings,
              COALESCE(SUM(severity = 'critical'), 0) AS criticals,
              MIN(created_wall_ns) AS oldest_created_wall_ns
            FROM notification_outbox AS n
            WHERE (n.run_id IN ({placeholders}) OR n.run_id IS NULL)
              AND n.status IN ('pending', 'sending')
            """,
            lineage_run_ids,
        ).fetchone()
    finally:
        connection.close()

    config_hashes = {str(row["config_hash"]) for row in lineage_rows}
    code_versions = {str(row["code_version"]) for row in lineage_rows}
    latest_config = json.loads(str(lineage_rows[-1]["config_json"]))
    if not isinstance(latest_config, Mapping):
        raise ScorecardError("D2 stored runtime config must be an object")
    installed_config = _required_mapping(
        installed_identity.get("config"), "installed D2 runtime config"
    )
    config_identity_ok = (
        latest_config == installed_config
        and _sha256_bytes(_canonical_json(latest_config).encode("utf-8"))
        == str(latest["config_hash"])
        == installed_identity.get("config_hash")
    )
    code_identity_ok = str(latest["code_version"]) == installed_identity.get(
        "code_version"
    )
    observe_zero_activity = (
        decision_count == 0 and len(fills) == 0 and pending_orders == 0
    )
    profile_activity_ok = (
        True
        if bounded_ok
        else observe_zero_activity
        if observe_ok
        else False
    )
    simulation_contract_ok = (
        (bounded_ok or observe_ok)
        and profile_activity_ok
        and config_identity_ok
        and code_identity_ok
        and _optional_bool_matches(health_details, "simulated", True)
        and _optional_bool_matches(health_details, "own_execution", False)
    )

    raw_freshness_seconds = (
        None
        if latest["last_book_wall_ns"] is None
        else (generated_wall_ns - int(latest["last_book_wall_ns"])) / NANOSECONDS
    )
    freshness_seconds = (
        None if raw_freshness_seconds is None else max(0.0, raw_freshness_seconds)
    )
    realized_error = (
        None
        if current_metrics["realized_pnl_quote"] is None
        else float(latest["realized_pnl_quote"])
        - float(current_metrics["realized_pnl_quote"])
    )
    fee_error = float(latest["cumulative_fees_quote"]) - float(
        current_metrics["fees_quote"]
    )
    replay_cash, replay_quantity = _replay_cash_quantity(
        fills,
        initial_cash=float(installed_config["initial_cash_quote"]),
        boundary_wall_ns=replay_end,
    )
    cash_error = float(latest["cash_quote"]) - replay_cash
    quantity_error = float(latest["base_quantity"]) - replay_quantity
    replay_average_cost = (
        float(current_replay["end_cost_basis_quote"]) / replay_quantity
        if replay_quantity > POSITION_EPSILON
        else 0.0
    )
    average_cost_error = float(latest["average_cost_quote"]) - replay_average_cost
    reconciliation_ok = (
        realized_error is not None
        and abs(realized_error) <= 0.01
        and abs(fee_error) <= 0.01
        and abs(cash_error) <= 0.01
        and abs(quantity_error) <= 1e-10
        and abs(average_cost_error) <= 0.01
        and bool(current_replay["pairing_reliable"])
    )
    daily_loss = (
        max(0.0, -float(equity["return"]))
        if equity.get("available") and equity.get("return") is not None
        else None
    )
    risk_breached = (
        float(latest["max_drawdown"]) >= float(shadow["max_drawdown_pct"])
        or (
            daily_loss is not None
            and daily_loss >= float(shadow["max_daily_loss_pct"])
        )
    )
    fail_closed = (
        latest["lifecycle_status"] not in {"warmup", "running"}
        and latest["halt_reason"] is not None
        and pending_orders == 0
    )
    risk_boundary_ok = not risk_breached or fail_closed
    safety_ok = (
        simulation_contract_ok
        and database_health["ok"]
        and len(config_hashes) == 1
        and len(code_versions) == 1
        and config_identity_ok
        and code_identity_ok
        and reconciliation_ok
        and risk_boundary_ok
        and health_row is not None
        and health_row["status"] == "ok"
        and health_details.get("live_order_routing") is False
        and health_details.get("orders_sent") == 0
    )
    fresh = (
        raw_freshness_seconds is not None
        and -300.0 <= raw_freshness_seconds
        <= float(operations_config["stale_after_seconds"])
    )
    alive_ok = (
        latest["lifecycle_status"] in {"warmup", "running"}
        and latest["run_status"] in {"warmup", "running"}
        and latest["halt_reason"] is None
        and pending_orders == 0
        and fresh
    )
    ready = alive_ok and latest["lifecycle_status"] == "running"
    oldest_pending_wall_ns = pending_outbox["oldest_created_wall_ns"]
    oldest_pending_age_seconds = (
        None
        if oldest_pending_wall_ns is None
        else max(
            0.0,
            (generated_wall_ns - int(oldest_pending_wall_ns)) / NANOSECONDS,
        )
    )
    strategy_role = (
        "bounded_diagnostic_not_validated_alpha"
        if bounded_ok
        else "public_feed_observation_no_orders"
        if observe_ok
        else "unapproved_shadow_profile"
    )
    simulation_contract_source = (
        "installed_bounded_runtime_identity"
        if bounded_ok
        else "installed_observe_runtime_identity"
        if observe_ok
        else "unapproved_d2_runtime_profile"
    )
    return {
        "instance": instance,
        "family": "D2",
        "market": expected_market,
        "strategy_role": strategy_role,
        "runtime_profile": runtime_profile,
        "metrics_scope": "current_active_ledger_restart_lineage_only",
        "source_database": str(database_path),
        "database_health": database_health,
        "metrics": metrics,
        "equity": equity,
        "lineage": {
            "run_count": len(lineage_rows),
            "first_started_wall_ns": int(lineage_rows[0]["started_wall_ns"]),
            "config_hash_count": len(config_hashes),
            "code_version_count": len(code_versions),
            "branch_free": True,
            "daily_run_starts": sum(
                window.start_wall_ns <= int(row["started_wall_ns"]) < window.end_wall_ns
                for row in lineage_rows
            ),
            "current_run_id": str(latest["run_id"]),
            "config_hash": str(latest["config_hash"]),
            "code_version": str(latest["code_version"]),
        },
        "current": {
            "lifecycle_status": str(latest["lifecycle_status"]),
            "halt_reason": latest["halt_reason"],
            "cash_quote": float(latest["cash_quote"]),
            "base_quantity": float(latest["base_quantity"]),
            "average_cost_quote": float(latest["average_cost_quote"]),
            "realized_pnl_quote": float(latest["realized_pnl_quote"]),
            "cumulative_fees_quote": float(latest["cumulative_fees_quote"]),
            "equity_quote": float(latest["last_equity_quote"]),
            "max_drawdown": float(latest["max_drawdown"]),
            "warmup_books_seen": int(latest["warmup_books_seen"]),
            "pending_orders": pending_orders,
            "updated_wall_ns": int(latest["updated_wall_ns"]),
            "feed_age_seconds": freshness_seconds,
            "alive_ok": alive_ok,
            "ready": ready,
            "status_ok": alive_ok,
        },
        "operations": {
            "warning_events": int(operational["warnings"] or 0),
            "critical_events": int(operational["criticals"] or 0),
            "continuity_events": int(operational["continuity"] or 0),
            "halt_events": int(operational["halts"] or 0),
            "recovery_events": int(operational["recoveries"] or 0),
            "decision_count": decision_count,
            "fill_count": len(fills),
            "pending_outbox": int(pending_outbox["pending"] or 0),
            "pending_warning": int(pending_outbox["warnings"] or 0),
            "pending_critical": int(pending_outbox["criticals"] or 0),
            "oldest_pending_age_seconds": oldest_pending_age_seconds,
        },
        "reconciliation": {
            "state_vs_fill_realized_pnl_error_quote": realized_error,
            "state_vs_fill_fees_error_quote": fee_error,
            "state_vs_fill_cash_error_quote": cash_error,
            "state_vs_fill_quantity_error": quantity_error,
            "state_vs_fill_average_cost_error_quote": average_cost_error,
            "ok": reconciliation_ok,
        },
        "safety": {
            "simulated": (
                True if simulation_contract_ok else health_details.get("simulated")
            ),
            "own_execution": (
                False
                if simulation_contract_ok
                else health_details.get("own_execution")
            ),
            "live_order_routing": health_details.get("live_order_routing"),
            "orders_sent": health_details.get("orders_sent"),
            "bounded_profile": bounded_ok,
            "observe_profile": observe_ok,
            "approved_profile": bounded_ok or observe_ok,
            "observe_zero_activity": (
                observe_zero_activity if observe_ok else None
            ),
            "simulation_contract": simulation_contract_ok,
            "simulation_contract_source": simulation_contract_source,
            "current_config_fingerprint": config_identity_ok,
            "installed_code_fingerprint": code_identity_ok,
            "risk_boundary_fail_closed": risk_boundary_ok,
            "risk_boundary_breached": risk_breached,
            "verified": safety_ok,
        },
        "available": True,
    }


def _installed_package_manifest(instance_home: Path) -> dict[str, Any]:
    root = _installed_package_root(instance_home)
    files = tuple(sorted(root.rglob("*.py")))
    if not files:
        raise ScorecardError("installed C2 package has no Python files")
    aggregate = hashlib.sha256()
    for path in files:
        if path.is_symlink() or not path.is_file():
            raise ScorecardError("installed C2 package contains an unsafe path")
        aggregate.update(path.relative_to(root).as_posix().encode("utf-8"))
        aggregate.update(b"\0")
        aggregate.update(path.read_bytes())
        aggregate.update(b"\0")
    return {
        "aggregate_sha256": aggregate.hexdigest(),
        "file_count": len(files),
        "scope": "sorted installed coinpilot/**/*.py",
    }


def _c2_fills(
    connection: sqlite3.Connection,
    account_key: str,
) -> list[FillRecord]:
    result: list[FillRecord] = []
    for row in connection.execute(
        """
        SELECT event_id, timestamp, payload_json
        FROM paper_events
        WHERE account_key = ? AND event_type = 'fill'
        ORDER BY timestamp, event_id
        """,
        (account_key,),
    ).fetchall():
        timestamp_ns = _iso_to_wall_ns(row["timestamp"], "paper fill timestamp")
        payload = json.loads(row["payload_json"])
        if not isinstance(payload, Mapping):
            raise ScorecardError("paper fill payload must be an object")
        result.append(
            FillRecord(
                fill_id=str(row["event_id"]),
                timestamp_ns=timestamp_ns,
                side=_required_text(payload.get("side"), "paper fill side"),
                quantity=_require_finite(payload.get("quantity"), "paper fill quantity", minimum=0.0),
                notional_quote=_require_finite(payload.get("notional"), "paper fill notional", minimum=0.0),
                fee_quote=_require_finite(payload.get("fee"), "paper fill fee", minimum=0.0),
                slippage_quote=_require_finite(
                    payload.get("slippage_cost", 0.0),
                    "paper fill slippage",
                    minimum=0.0,
                ),
                reason=str(payload.get("reason")) if payload.get("reason") is not None else None,
            )
        )
    return sorted(result, key=lambda fill: (fill.timestamp_ns, fill.fill_id))


def _replay_cash_quantity(
    fills: Sequence[FillRecord],
    *,
    initial_cash: float,
    boundary_wall_ns: int,
) -> tuple[float, float]:
    cash = float(initial_cash)
    quantity = 0.0
    for fill in fills:
        if fill.timestamp_ns >= boundary_wall_ns:
            break
        if fill.side == "buy":
            cash -= fill.notional_quote + fill.fee_quote
            quantity += fill.quantity
        elif fill.side == "sell":
            cash += fill.notional_quote - fill.fee_quote
            quantity -= fill.quantity
        else:
            raise ScorecardError("unsupported paper fill side")
        if cash < -1e-5 or quantity < -1e-10:
            raise ScorecardError("paper fill replay produced an invalid portfolio")
        cash = max(0.0, cash)
        quantity = max(0.0, quantity)
    return cash, quantity


def _candle_before(
    connection: sqlite3.Connection,
    *,
    market: str,
    interval_minutes: int,
    boundary_wall_ns: int,
) -> tuple[int, float] | None:
    selected: tuple[int, float] | None = None
    rows = connection.execute(
        """
        SELECT timestamp, close FROM candles
        WHERE market = ? AND interval_minutes = ?
        ORDER BY timestamp
        """,
        (market, interval_minutes),
    ).fetchall()
    for row in rows:
        timestamp_ns = _iso_to_wall_ns(row["timestamp"], "candle timestamp")
        if timestamp_ns >= boundary_wall_ns:
            break
        selected = (timestamp_ns, float(row["close"]))
    if selected is not None:
        selected_wall_ns, selected_close = selected
        selected_close = _require_finite(
            selected_close,
            "closed candle mark",
            minimum=0.0,
        )
        if selected_close <= 0:
            raise ScorecardError("closed candle mark must be positive")
        selected = (selected_wall_ns, selected_close)
    return selected


def _c2_boundary_equity(
    connection: sqlite3.Connection,
    fills: Sequence[FillRecord],
    *,
    market: str,
    interval_minutes: int,
    initial_cash: float,
    fee_rate: float,
    slippage_bps: float,
    boundary_wall_ns: int,
) -> dict[str, Any]:
    cash, quantity = _replay_cash_quantity(
        fills,
        initial_cash=initial_cash,
        boundary_wall_ns=boundary_wall_ns,
    )
    if quantity <= POSITION_EPSILON:
        return {
            "available": True,
            "equity_quote": cash,
            "cash_quote": cash,
            "base_quantity": 0.0,
            "valuation_quality": "exact_flat_cash",
            "mark_wall_ns": None,
            "mark_close": None,
            "mark_age_seconds": None,
        }
    candle = _candle_before(
        connection,
        market=market,
        interval_minutes=interval_minutes,
        boundary_wall_ns=boundary_wall_ns,
    )
    if candle is None:
        return {
            "available": False,
            "equity_quote": None,
            "cash_quote": cash,
            "base_quantity": quantity,
            "valuation_quality": "unavailable_non_flat_without_closed_candle",
            "mark_wall_ns": None,
            "mark_close": None,
            "mark_age_seconds": None,
        }
    candle_wall_ns, close = candle
    mark_age_seconds = (boundary_wall_ns - candle_wall_ns) / NANOSECONDS
    if not 0 < mark_age_seconds <= interval_minutes * 2 * 60:
        return {
            "available": False,
            "equity_quote": None,
            "cash_quote": cash,
            "base_quantity": quantity,
            "valuation_quality": "unavailable_non_flat_stale_closed_candle",
            "mark_wall_ns": candle_wall_ns,
            "mark_close": close,
            "mark_age_seconds": mark_age_seconds,
        }
    liquidation_price = close * (1.0 - slippage_bps / 10_000.0)
    equity = cash + quantity * liquidation_price * (1.0 - fee_rate)
    return {
        "available": True,
        "equity_quote": equity,
        "cash_quote": cash,
        "base_quantity": quantity,
        "valuation_quality": "estimated_closed_candle_liquidation",
        "mark_wall_ns": candle_wall_ns,
        "mark_close": close,
        "mark_age_seconds": mark_age_seconds,
    }


def _aggregate_c2(
    *,
    root: Path,
    instance: str,
    expected_market: str,
    window: ReportWindow,
    generated_wall_ns: int,
) -> dict[str, Any]:
    instance_home = root / "instances" / instance
    config_path = instance_home / "config" / "paper.toml"
    manifest_path = instance_home / "config" / "paper.manifest.json"
    runtime_path = instance_home / "config" / "runtime.env"
    for source_path in (config_path, manifest_path, runtime_path):
        _reject_symlink_components(source_path)
    config = tomllib.loads(config_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(config, Mapping) or not isinstance(manifest, Mapping):
        raise ScorecardError("C2 config and manifest must be objects")
    runtime = _read_runtime_env(runtime_path)
    if (
        _contains_forbidden_key(config)
        or _contains_forbidden_key(manifest)
        or _contains_forbidden_key(runtime)
    ):
        raise ScorecardError("C2 config contains a forbidden credential field")
    data = _required_mapping(config.get("data"), "C2 data config")
    paper = _required_mapping(config.get("paper"), "C2 paper config")
    risk = _required_mapping(config.get("risk"), "C2 risk config")
    market_manifest = _required_mapping(
        manifest.get("market_config"), "C2 market manifest"
    )
    if data.get("market") != expected_market:
        raise ScorecardError("C2 market does not match the approved allowlist")
    if data.get("api_base_url") != "https://api.upbit.com":
        raise ScorecardError("C2 source is not the approved public Upbit API")
    interval_minutes = int(data.get("interval_minutes", 0))
    if interval_minutes != 60:
        raise ScorecardError("C2 interval is not the frozen 60-minute policy")
    database_candidate = Path(
        _required_text(data.get("database_path"), "data.database_path")
    ).expanduser()
    if not database_candidate.is_absolute():
        raise ScorecardError("C2 database path must be absolute")
    _reject_symlink_components(database_candidate)
    database_path = database_candidate.resolve()
    expected_database = (instance_home / "data" / "coinpilot-c2.db").resolve()
    if database_path != expected_database:
        raise ScorecardError("C2 database is not the approved paper ledger")
    account_key = _required_text(
        market_manifest.get("paper_account_key"),
        "manifest paper account key",
    )
    expected_account = f"paper-v8:{paper.get('account_name')}:{expected_market}:60m"
    if account_key != expected_account:
        raise ScorecardError("C2 account key does not match the frozen policy")
    expected_config_hash = _required_text(
        market_manifest.get("config_sha256"),
        "manifest config hash",
    )
    config_hash_ok = _sha256_bytes(config_path.read_bytes()) == expected_config_hash
    installed_manifest = _installed_package_manifest(instance_home)
    installed_package_ok = installed_manifest == manifest.get("installed_package")
    installed_identity = _c2_installed_identity(
        instance_home,
        config_path,
        manifest_path,
    )
    manifest_safety = _required_mapping(manifest.get("safety"), "C2 safety manifest")
    installed_runtime_ok = (
        installed_identity.get("market") == expected_market
        and installed_identity.get("paper_account_key") == account_key
        and installed_identity.get("public_only") is True
        and installed_identity.get("live_order_routing") is False
        and installed_identity.get("orders_sent") == 0
        and installed_identity.get("valid") is True
    )
    simulation_contract_ok = (
        installed_package_ok
        and installed_runtime_ok
        and manifest_safety.get("public_only") is True
        and _optional_bool_matches(manifest_safety, "simulated", True)
        and _optional_bool_matches(manifest_safety, "own_execution", False)
        and _optional_bool_matches(installed_identity, "simulated", True)
        and _optional_bool_matches(installed_identity, "own_execution", False)
    )
    runtime_profile_ok = (
        runtime.get("COINPILOT_ENABLE_PAPER") == "1"
        and runtime.get("COINPILOT_ENABLE_SHADOW") == "0"
        and runtime.get("COINPILOT_ENABLE_NOTIFIER") == "0"
        and runtime.get("COINPILOT_ENABLE_WEB") == "0"
        and runtime.get("COINPILOT_ENABLE_WATCHDOG") == "0"
    )

    connection = open_readonly_sqlite(database_path)
    try:
        expected_tables = {"candles", "paper_state", "paper_events"}
        if not expected_tables.issubset(_table_names(connection)):
            raise ScorecardError("C2 ledger schema is incomplete")
        database_health = _database_checks(connection, foreign_keys=False)
        state_row = connection.execute(
            "SELECT state_json, updated_at FROM paper_state WHERE account_key = ?",
            (account_key,),
        ).fetchone()
        if state_row is None:
            raise ScorecardError("C2 paper state is missing")
        state = json.loads(state_row["state_json"])
        if not isinstance(state, Mapping):
            raise ScorecardError("C2 paper state must be an object")
        if state.get("market") != expected_market or state.get("schema_version") != 8:
            raise ScorecardError("C2 paper state schema or market is invalid")
        if state.get("updated_at") != state_row["updated_at"]:
            raise ScorecardError("C2 state timestamp columns do not match")
        selected_state_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM paper_state WHERE account_key = ?",
                (account_key,),
            ).fetchone()[0]
        )
        archived_state_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM paper_state WHERE account_key <> ?",
                (account_key,),
            ).fetchone()[0]
        )
        out_of_scope_events = int(
            connection.execute(
                "SELECT COUNT(*) FROM paper_events WHERE account_key <> ?",
                (account_key,),
            ).fetchone()[0]
        )
        database_health["account_scope_ok"] = selected_state_count == 1
        database_health["selected_paper_state_rows"] = selected_state_count
        database_health["archived_paper_state_rows_excluded"] = archived_state_count
        database_health["archived_events_excluded"] = out_of_scope_events
        database_health["ok"] = bool(
            database_health["ok"] and database_health["account_scope_ok"]
        )
        manifest_fingerprint = market_manifest.get("paper_config_fingerprint")
        current_fingerprint = installed_identity.get("paper_config_fingerprint")
        fingerprint_ok = (
            state.get("config_fingerprint")
            == manifest_fingerprint
            == current_fingerprint
        )
        # Keep fills through the live snapshot for state reconciliation.  Each
        # reporting window below still applies its own cutoff, so a fill after
        # midnight cannot leak into the completed day's metrics.
        fills = _c2_fills(connection, account_key)
        metrics = _metric_windows(fills, window)
        initial_cash = _require_finite(risk.get("initial_cash"), "risk.initial_cash", minimum=0.0)
        fee_rate = _require_finite(risk.get("fee_rate"), "risk.fee_rate", minimum=0.0)
        slippage_bps = _require_finite(risk.get("slippage_bps"), "risk.slippage_bps", minimum=0.0)
        start_equity = _c2_boundary_equity(
            connection,
            fills,
            market=expected_market,
            interval_minutes=interval_minutes,
            initial_cash=initial_cash,
            fee_rate=fee_rate,
            slippage_bps=slippage_bps,
            boundary_wall_ns=window.start_wall_ns,
        )
        end_equity = _c2_boundary_equity(
            connection,
            fills,
            market=expected_market,
            interval_minutes=interval_minutes,
            initial_cash=initial_cash,
            fee_rate=fee_rate,
            slippage_bps=slippage_bps,
            boundary_wall_ns=window.end_wall_ns,
        )
        current_boundary_ns = max(
            generated_wall_ns + 1,
            max((fill.timestamp_ns for fill in fills), default=0) + 1,
        )
        current_equity = _c2_boundary_equity(
            connection,
            fills,
            market=expected_market,
            interval_minutes=interval_minutes,
            initial_cash=initial_cash,
            fee_rate=fee_rate,
            slippage_bps=slippage_bps,
            boundary_wall_ns=current_boundary_ns,
        )
        event_rows = connection.execute(
            """
            SELECT timestamp, event_type FROM paper_events
            WHERE account_key = ? ORDER BY timestamp, event_id
            """,
            (account_key,),
        ).fetchall()
    finally:
        connection.close()

    event_counts: dict[str, int] = {}
    first_event_ns: int | None = None
    for event in event_rows:
        event_ns = _iso_to_wall_ns(event["timestamp"], "paper event timestamp")
        first_event_ns = event_ns if first_event_ns is None else min(first_event_ns, event_ns)
        if window.start_wall_ns <= event_ns < window.end_wall_ns:
            name = str(event["event_type"])
            event_counts[name] = event_counts.get(name, 0) + 1
    updated_wall_ns = _iso_to_wall_ns(state.get("updated_at"), "paper state updated_at")
    raw_state_age_seconds = (generated_wall_ns - updated_wall_ns) / NANOSECONDS
    state_age_seconds = max(0.0, raw_state_age_seconds)
    current_quantity = _require_finite(
        state.get("quantity", 0.0),
        "C2 state quantity",
        minimum=0.0,
    )
    current_cash = _require_finite(
        state.get("cash", 0.0),
        "C2 state cash",
        minimum=0.0,
    )
    current_realized = _require_finite(
        state.get("realized_pnl", 0.0),
        "C2 state realized PnL",
    )
    current_peak_equity = _require_finite(
        state.get("peak_equity", initial_cash),
        "C2 state peak equity",
        minimum=0.0,
    )
    replay_end = max((fill.timestamp_ns for fill in fills), default=0) + 1
    current_fill_metrics = _period_fill_metrics(
        fills,
        start_wall_ns=0,
        end_wall_ns=replay_end,
    )
    lineage_realized = current_fill_metrics["realized_pnl_quote"]
    reconciliation_error = (
        None
        if lineage_realized is None
        else current_realized - float(lineage_realized)
    )
    replay_cash, replay_quantity = _replay_cash_quantity(
        fills,
        initial_cash=initial_cash,
        boundary_wall_ns=replay_end,
    )
    cash_error = current_cash - replay_cash
    quantity_error = current_quantity - replay_quantity
    reconciliation_ok = (
        reconciliation_error is not None
        and abs(reconciliation_error) <= 0.01
        and abs(cash_error) <= 0.01
        and abs(quantity_error) <= 1e-10
    )
    equity_change = None
    equity_return = None
    if start_equity["available"] and end_equity["available"]:
        start_value = float(start_equity["equity_quote"])
        end_value = float(end_equity["equity_quote"])
        equity_change = end_value - start_value
        equity_return = equity_change / start_value if start_value > 0 else None
    safety_ok = (
        database_health["ok"]
        and config_hash_ok
        and installed_package_ok
        and installed_runtime_ok
        and simulation_contract_ok
        and fingerprint_ok
        and runtime_profile_ok
        and manifest_safety.get("public_only") is True
        and manifest_safety.get("live_order_routing") is False
        and manifest_safety.get("orders_sent") == 0
        and reconciliation_ok
    )
    status_ok = (
        state.get("halt_state") == "ACTIVE"
        and int(state.get("revision", 0)) >= 1
        and -300.0 <= raw_state_age_seconds
        <= max(int(paper.get("poll_seconds", 30)) * 5, 300)
    )
    return {
        "instance": instance,
        "family": "C2",
        "market": expected_market,
        "strategy_role": "forward_paper_candidate",
        "source_database": str(database_path),
        "database_health": database_health,
        "metrics": metrics,
        "equity": {
            "available": start_equity["available"] and end_equity["available"],
            "start": start_equity,
            "end": end_equity,
            "change_quote": equity_change,
            "return": equity_return,
            "period_max_drawdown": None,
            "period_max_drawdown_reason": "C2 does not persist equity history",
        },
        "lineage": {
            "account_key": account_key,
            "config_fingerprint": state.get("config_fingerprint"),
            "first_event_wall_ns": first_event_ns,
            "first_event_iso": None if first_event_ns is None else _iso_utc(first_event_ns),
        },
        "current": {
            "halt_state": state.get("halt_state"),
            "cash_quote": current_cash,
            "base_quantity": current_quantity,
            "realized_pnl_quote": current_realized,
            "peak_equity_quote": current_peak_equity,
            "equity_quote": current_equity.get("equity_quote"),
            "equity_valuation_quality": current_equity.get("valuation_quality"),
            "revision": int(state.get("revision", 0)),
            "updated_at": state.get("updated_at"),
            "age_seconds": state_age_seconds,
            "status_ok": status_ok,
        },
        "operations": {
            "daily_event_counts": event_counts,
            "known_repository_source_drift_excluded": True,
        },
        "reconciliation": {
            "state_vs_fill_realized_pnl_error_quote": reconciliation_error,
            "state_vs_fill_cash_error_quote": cash_error,
            "state_vs_fill_quantity_error": quantity_error,
            "ok": reconciliation_ok,
        },
        "safety": {
            "simulated": (
                True if simulation_contract_ok else manifest_safety.get("simulated")
            ),
            "own_execution": (
                False
                if simulation_contract_ok
                else manifest_safety.get("own_execution")
            ),
            "public_only": manifest_safety.get("public_only"),
            "live_order_routing": manifest_safety.get("live_order_routing"),
            "orders_sent": manifest_safety.get("orders_sent"),
            "runtime_profile": runtime_profile_ok,
            "config_hash": config_hash_ok,
            "config_fingerprint": fingerprint_ok,
            "installed_package_manifest": installed_package_ok,
            "installed_runtime_verification": installed_runtime_ok,
            "simulation_contract": simulation_contract_ok,
            "simulation_contract_source": "installed_frozen_public_paper_identity",
            "verified": safety_ok,
        },
        "available": True,
    }


def _failed_instance(instance: str, family: str, market: str, exc: BaseException) -> dict[str, Any]:
    return {
        "instance": instance,
        "family": family,
        "market": market,
        "available": False,
        "error": _safe_error(exc),
        "safety": {
            "simulated": None,
            "own_execution": None,
            "live_order_routing": None,
            "orders_sent": None,
            "verified": False,
        },
    }


def _sum_metric(instances: Sequence[Mapping[str, Any]], period: str, field: str) -> float | int | None:
    values: list[float | int] = []
    for instance in instances:
        if not instance.get("available"):
            return None
        value = instance["metrics"][period].get(field)
        if value is None:
            return None
        values.append(value)
    return sum(values)


def _family_summary(instances: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    daily_trades = _sum_metric(instances, "daily", "completed_trades")
    daily_wins = _sum_metric(instances, "daily", "wins")
    daily_realized = _sum_metric(instances, "daily", "realized_pnl_quote")
    previous_realized = _sum_metric(instances, "previous_day", "realized_pnl_quote")
    trailing_realized = _sum_metric(instances, "trailing_7d", "realized_pnl_quote")
    lineage_realized = _sum_metric(instances, "lineage", "realized_pnl_quote")
    return {
        "available_instances": sum(bool(row.get("available")) for row in instances),
        "expected_instances": len(instances),
        "daily": {
            "realized_pnl_quote": daily_realized,
            "accounting_realized_pnl_quote": daily_realized,
            "completed_round_trip_pnl_quote": _sum_metric(
                instances, "daily", "completed_round_trip_pnl_quote"
            ),
            "completed_trades": daily_trades,
            "fees_quote": _sum_metric(instances, "daily", "fees_quote"),
            "turnover_quote": _sum_metric(instances, "daily", "turnover_quote"),
            "wins": daily_wins,
            "win_rate": (
                float(daily_wins) / float(daily_trades)
                if daily_wins is not None and daily_trades not in (None, 0)
                else None
            ),
        },
        "previous_day_realized_pnl_quote": previous_realized,
        "trailing_7d_realized_pnl_quote": trailing_realized,
        "lineage_realized_pnl_quote": lineage_realized,
        "current_status_ok": all(
            bool(row.get("available")) and bool(row.get("current", {}).get("status_ok"))
            for row in instances
        ),
        "safety_verified": all(
            bool(row.get("available")) and bool(row.get("safety", {}).get("verified"))
            for row in instances
        ),
    }


def _observation_days(c2: Sequence[Mapping[str, Any]], window: ReportWindow) -> int:
    starts = [
        int(row["lineage"]["first_event_wall_ns"])
        for row in c2
        if row.get("available") and row.get("lineage", {}).get("first_event_wall_ns")
    ]
    if not starts:
        return 0
    latest_start = max(starts)
    return max(0, int((window.end_wall_ns - latest_start) // (24 * 60 * 60 * NANOSECONDS)))


def _improvement_assessment(
    d2: Sequence[Mapping[str, Any]],
    c2: Sequence[Mapping[str, Any]],
    *,
    window: ReportWindow,
) -> dict[str, Any]:
    all_rows = [*d2, *c2]
    d2_observe_only = bool(d2) and all(
        row.get("available")
        and row.get("strategy_role") == "public_feed_observation_no_orders"
        for row in d2
    )
    safety_ok = all(
        row.get("available") and row.get("safety", {}).get("verified")
        for row in all_rows
    )
    status_ok = all(
        row.get("available") and row.get("current", {}).get("status_ok")
        for row in all_rows
    )
    days = _observation_days(c2, window)
    c2_trades = _sum_metric(c2, "lineage", "completed_trades")
    observations: list[str] = []
    if not safety_ok:
        observations.append("안전 또는 원장 검증 실패가 있어 운영자 진단이 우선입니다.")
    if not status_ok:
        observations.append("하나 이상의 활성 계정 상태가 정상 기준을 벗어났습니다.")
    if c2_trades in (None, 0):
        observations.append("C2 완료 거래 표본이 없어 성과 방향을 판단할 수 없습니다.")
    elif int(c2_trades) < 30:
        observations.append(f"C2 누적 완료 거래가 {int(c2_trades)}건으로 매우 적습니다.")
    for family_name, rows in (("D2", d2), ("C2", c2)):
        if family_name == "D2" and d2_observe_only:
            decisions = sum(
                int(row.get("operations", {}).get("decision_count", 0))
                for row in rows
            )
            fills = sum(
                int(row.get("operations", {}).get("fill_count", 0))
                for row in rows
            )
            unresolved = sum(
                int(row.get("operations", {}).get("pending_outbox", 0))
                for row in rows
            )
            observations.append(
                "D2 observe 활성 원장은 주문 없는 공개피드 관찰 구간이며 "
                f"결정 {decisions}건, 체결 {fills}건, audit unresolved "
                f"{unresolved}건입니다."
            )
            continue
        daily = _sum_metric(rows, "daily", "realized_pnl_quote")
        previous = _sum_metric(rows, "previous_day", "realized_pnl_quote")
        trailing = _sum_metric(rows, "trailing_7d", "realized_pnl_quote")
        fees = _sum_metric(rows, "daily", "fees_quote")
        turnover = _sum_metric(rows, "daily", "turnover_quote")
        trades = _sum_metric(rows, "daily", "completed_trades")
        wins = _sum_metric(rows, "daily", "wins")
        if daily is not None and previous is not None:
            direction = "개선" if float(daily) > float(previous) else "악화" if float(daily) < float(previous) else "동일"
            observations.append(
                f"{family_name} 회계 실현손익은 전일 대비 {direction} "
                f"({float(previous):.2f}원 → {float(daily):.2f}원)입니다."
            )
        if trailing is not None:
            observations.append(
                f"{family_name} 최근 7일 회계 실현손익은 {float(trailing):.2f}원입니다."
            )
        if fees is not None and turnover not in (None, 0):
            observations.append(
                f"{family_name} 일 수수료/회전대금은 {float(fees) / float(turnover):.4%}입니다."
            )
        if trades not in (None, 0) and wins is not None:
            observations.append(
                f"{family_name} 일 완료 RT 승률은 {float(wins) / float(trades):.1%}입니다."
            )
    open_positions = sum(
        abs(float(row.get("current", {}).get("base_quantity", 0.0))) > POSITION_EPSILON
        for row in all_rows
        if row.get("available")
    )
    observations.append(f"현재 non-flat 모의 포지션은 {open_positions}/8개입니다.")
    d2_drawdowns = [
        float(row["equity"]["sampled_daily_drawdown"])
        for row in d2
        if row.get("available")
        and row.get("equity", {}).get("sampled_daily_drawdown") is not None
    ]
    if d2_drawdowns and not d2_observe_only:
        observations.append(f"D2 최대 일중 sampled DD는 {max(d2_drawdowns):.3%}입니다.")
    d2_fees = _sum_metric(d2, "lineage", "fees_quote")
    d2_pnl = _sum_metric(d2, "lineage", "realized_pnl_quote")
    if d2_fees is not None and d2_pnl is not None and not d2_observe_only:
        observations.append(
            f"D2 누적 수수료 {float(d2_fees):.2f}원, 순실현손익 {float(d2_pnl):.2f}원은 배관 진단 관측치입니다."
        )
    if not observations:
        observations.append("새로 검증할 이상 징후가 관측되지 않았습니다.")
    if not safety_ok or not status_ok:
        gate = "OPERATOR_REVIEW_REQUIRED"
        next_step = "원장·fingerprint·halt·서비스 상태를 진단하고 자동 변경은 금지합니다."
    elif days < 30:
        gate = "COLLECTING_T0_EVIDENCE"
        next_step = f"C2 공통 관측 {days}/30일: 동일 계정으로 데이터를 계속 수집합니다."
    elif c2_trades is None or int(c2_trades) < 30:
        gate = "INSUFFICIENT_TRADES"
        next_step = "C2 완료 RT 30건 전에는 성과 후보를 만들지 않고 동일 계정 관측을 계속합니다."
    else:
        gate = "OFFLINE_REVIEW_CANDIDATE"
        next_step = (
            "후보만 오프라인 replay/walk-forward와 2배 비용 stress로 검증합니다. "
            "활성 전략은 자동 변경하지 않습니다."
        )
    return {
        "gate": gate,
        "common_c2_observation_days": days,
        "minimum_observation_days": 30,
        "minimum_completed_round_trips": 30,
        "profit_target": None,
        "observations": observations,
        "next_step": next_step,
        "automatic_strategy_change": False,
        "automatic_rearm": False,
        "promotion_requires_new_model_account_or_ledger": True,
        "required_validation": [
            "causal replay",
            "purged walk-forward",
            "2x cost stress",
            "operator review",
        ],
    }


def build_scorecard(
    *,
    root: Path,
    window: ReportWindow,
    generated_wall_ns: int | None = None,
) -> dict[str, Any]:
    generated = time.time_ns() if generated_wall_ns is None else generated_wall_ns
    root = root.expanduser().absolute()
    _reject_symlink_components(root)
    root = root.resolve()
    if root.name != "Coinpilot" or not (root / "instances").is_dir():
        raise ScorecardError("Coinpilot root is not the expected application boundary")
    d2: list[dict[str, Any]] = []
    c2: list[dict[str, Any]] = []
    for instance, market in D2_INSTANCES.items():
        try:
            row = _aggregate_d2(
                root=root,
                instance=instance,
                expected_market=market,
                window=window,
                generated_wall_ns=generated,
            )
        except (OSError, ValueError, KeyError, TypeError, AttributeError, json.JSONDecodeError, sqlite3.Error, ScorecardError) as exc:
            row = _failed_instance(instance, "D2", market, exc)
        d2.append(row)
    for instance, market in C2_INSTANCES.items():
        try:
            row = _aggregate_c2(
                root=root,
                instance=instance,
                expected_market=market,
                window=window,
                generated_wall_ns=generated,
            )
        except (OSError, ValueError, KeyError, TypeError, AttributeError, json.JSONDecodeError, sqlite3.Error, ScorecardError) as exc:
            row = _failed_instance(instance, "C2", market, exc)
        c2.append(row)
    d2_summary = _family_summary(d2)
    c2_summary = _family_summary(c2)
    improvement = _improvement_assessment(d2, c2, window=window)
    all_rows = [*d2, *c2]
    source_starts: list[int] = []
    for row in d2:
        value = row.get("lineage", {}).get("first_started_wall_ns")
        if row.get("available") and value is not None:
            source_starts.append(int(value))
    for row in c2:
        value = row.get("lineage", {}).get("first_event_wall_ns")
        if row.get("available") and value is not None:
            source_starts.append(int(value))
    coverage_start_date = (
        datetime.fromtimestamp(min(source_starts) / NANOSECONDS, SEOUL)
        .date()
        .isoformat()
        if source_starts
        else window.report_date.isoformat()
    )
    safety_verified = all(
        row.get("available") and row.get("safety", {}).get("verified")
        for row in all_rows
    )
    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "report_id": window.report_id,
        "report_date": window.report_date.isoformat(),
        "window": {
            "timezone": "Asia/Seoul",
            "semantics": "half_open_[start,end)",
            "start_wall_ns": window.start_wall_ns,
            "end_wall_ns": window.end_wall_ns,
            "start_iso": window.start_local.isoformat(),
            "end_iso": window.end_local.isoformat(),
            "label": window.label,
        },
        "generated_wall_ns": generated,
        "generated_at": _iso_utc(generated),
        "coverage": {
            "start_date": coverage_start_date,
            "end_date": window.report_date.isoformat(),
            "catch_up_policy": "latest_first_then_one_missing_day_per_run",
        },
        "source_policy": {
            "approved_instances": list(APPROVED_INSTANCES),
            "legacy_instances_excluded": ["btc", "eth", "xrp", "sol"],
            "source_database_access": "sqlite_mode_ro_query_only_single_read_transaction",
            "trading_outbox_mutation": False,
            "d2_and_c2_returns_combined": False,
            "d2_active_ledger_only": True,
            "d2_historical_ledger_auto_aggregation": False,
        },
        "D2": {"summary": d2_summary, "instances": d2},
        "C2": {"summary": c2_summary, "instances": c2},
        "safety": {
            "verified": safety_verified,
            "simulated": True if safety_verified else None,
            "own_execution": False if safety_verified else None,
            "live_order_routing": False if safety_verified else None,
            "orders_sent": 0 if safety_verified else None,
            "target": {
                "simulated": True,
                "own_execution": False,
                "live_order_routing": False,
                "orders_sent": 0,
            },
        },
        "improvement": improvement,
        "caveats": [
            (
                "D2 is a bounded diagnostic, not validated alpha."
                if all(
                    row.get("strategy_role")
                    == "bounded_diagnostic_not_validated_alpha"
                    for row in d2
                    if row.get("available")
                )
                else "D2 observe is public-feed observation with no decisions, orders, or fills."
            ),
            "D2 metrics cover only the current active ledger restart lineage; prior diagnostic terminal results remain in the transition receipt and frozen ledger and are never auto-aggregated.",
            "C2 historical intraday max drawdown is unavailable because the paper ledger has no equity history.",
            "A non-flat C2 boundary valuation is a labeled closed-candle liquidation estimate.",
            "Repository-source C2 manifest drift is excluded; the installed frozen package manifest controls runtime verification.",
            "Automatic strategy changes, halt rearm, and ledger reuse are prohibited.",
        ],
    }


def _krw(value: object, *, signed: bool = False) -> str:
    if value is None:
        return "N/A"
    numeric = float(value)
    rounded = int(round(abs(numeric)))
    if numeric < 0:
        return f"-₩{rounded:,}"
    if signed and numeric > 0:
        return f"+₩{rounded:,}"
    return f"₩{rounded:,}"


def _percent(value: object, *, signed: bool = False) -> str:
    if value is None:
        return "N/A"
    numeric = float(value) * 100.0
    prefix = "+" if signed and numeric > 0 else ""
    return f"{prefix}{numeric:.3f}%"


def _d2_equity_coverage(row: Mapping[str, Any]) -> str:
    equity = row.get("equity", {})
    if not isinstance(equity, Mapping) or not equity.get("available"):
        return "unavailable"
    if equity.get("full_window"):
        return "full"
    lag = equity.get("start_boundary_lag_seconds")
    if lag is None:
        return "partial"
    return f"partial(start +{float(lag) / 3600.0:.1f}h)"


def _c2_equity_coverage(row: Mapping[str, Any]) -> str:
    equity = row.get("equity", {})
    if not isinstance(equity, Mapping) or not equity.get("available"):
        return "unavailable"
    start = equity.get("start", {})
    end = equity.get("end", {})
    qualities = {
        item.get("valuation_quality")
        for item in (start, end)
        if isinstance(item, Mapping)
    }
    if qualities == {"exact_flat_cash"}:
        return "exact-flat"
    return "estimated"


def _d2_section_title(instances: Sequence[Mapping[str, Any]]) -> str:
    roles = {
        str(row.get("strategy_role"))
        for row in instances
        if row.get("available")
    }
    if roles == {"bounded_diagnostic_not_validated_alpha"}:
        return "D2 bounded diagnostic"
    if roles == {"public_feed_observation_no_orders"}:
        return "D2 public-feed observe"
    return "D2 approved shadow profiles"


def _d2_audit_backlog_text(row: Mapping[str, Any]) -> str:
    operations = row.get("operations", {})
    if "pending_outbox" not in operations:
        return ""
    oldest = operations.get("oldest_pending_age_seconds")
    oldest_text = "N/A" if oldest is None else f"{float(oldest):.0f}s"
    return (
        f"audit unresolved/warn/crit {operations.get('pending_outbox', 0)}/"
        f"{operations.get('pending_warning', 0)}/"
        f"{operations.get('pending_critical', 0)}, oldest {oldest_text}"
    )


def _compact_instance_line(row: Mapping[str, Any]) -> str:
    market = str(row.get("market", "?"))
    if not row.get("available"):
        return f"• {market}: SOURCE ERROR"
    daily = row["metrics"]["daily"]
    previous = row["metrics"]["previous_day"]
    trailing = row["metrics"]["trailing_7d"]
    status = row.get("current", {}).get("lifecycle_status") or row.get("current", {}).get("halt_state")
    trades = daily.get("completed_trades")
    trade_text = "N/A" if trades is None else str(int(trades))
    position = float(row.get("current", {}).get("base_quantity", 0.0))
    position_text = "flat" if abs(position) <= POSITION_EPSILON else f"{position:.8g}"
    freshness = row.get("current", {}).get("feed_age_seconds")
    if freshness is None:
        freshness = row.get("current", {}).get("age_seconds")
    freshness_text = "N/A" if freshness is None else f"{float(freshness):.0f}s"
    reconciliation = "✅" if row.get("reconciliation", {}).get("ok") else "❌"
    win_rate = _percent(daily.get("win_rate"))
    turnover = _krw(daily.get("turnover_quote"))
    if row.get("family") == "D2":
        drawdown = _percent(row.get("equity", {}).get("sampled_daily_drawdown"))
        period_return = _percent(row.get("equity", {}).get("return"), signed=True)
        equity = _krw(row.get("current", {}).get("equity_quote"))
        coverage = _d2_equity_coverage(row)
        operations = row.get("operations", {})
        ops_text = (
            f"cont/halt/recover {operations.get('continuity_events', 0)}/"
            f"{operations.get('halt_events', 0)}/{operations.get('recovery_events', 0)}"
        )
        audit_backlog = _d2_audit_backlog_text(row)
        if audit_backlog:
            ops_text += f" · {audit_backlog}"
    else:
        drawdown = "N/A"
        period_return = _percent(row.get("equity", {}).get("return"), signed=True)
        equity = _krw(row.get("current", {}).get("equity_quote"))
        coverage = _c2_equity_coverage(row)
        event_total = sum(row.get("operations", {}).get("daily_event_counts", {}).values())
        ops_text = f"events {event_total}"
    return (
        f"• *{market}* 회계P&L 일 {_krw(daily.get('realized_pnl_quote'), signed=True)} / "
        f"전일 {_krw(previous.get('realized_pnl_quote'), signed=True)} / "
        f"7일 {_krw(trailing.get('realized_pnl_quote'), signed=True)} · "
        f"RT {trade_text} ({win_rate}, {_krw(daily.get('completed_round_trip_pnl_quote'), signed=True)}) · "
        f"fee {_krw(daily.get('fees_quote'))} / turn {turnover}\n"
        f"  dayRet {period_return} / DD {drawdown} · {coverage} · pos {position_text} · equity {equity} · fresh {freshness_text} · "
        f"{ops_text} · recon {reconciliation} · {status}"
    )


def scorecard_markdown(report: Mapping[str, Any]) -> str:
    safety = report["safety"]
    d2 = report["D2"]
    c2 = report["C2"]
    d2_title = _d2_section_title(d2["instances"])
    has_d2_profile_contract = any(
        row.get("available") and "runtime_profile" in row
        for row in d2["instances"]
    )
    lines = [
        f"# CoinPilot 일일 모의거래 성적표 — {report['report_date']}",
        "",
        f"- 구간: {report['window']['label']}",
        f"- report ID: `{report['report_id']}`",
        f"- 생성: {report['generated_at']}",
        (
            "- 안전: "
            + (
                "PASS (`simulated=true`, `own_execution=false`, `live_order_routing=false`, `orders_sent=0`)"
                if safety["verified"]
                else "FAIL/UNKNOWN — 운영자 확인 필요"
            )
        ),
        "",
        f"## {d2_title}",
        "",
        (
            f"일 회계 실현손익 {_krw(d2['summary']['daily']['realized_pnl_quote'], signed=True)}, "
            f"완료 거래 {d2['summary']['daily']['completed_trades']}, "
            f"수수료 {_krw(d2['summary']['daily']['fees_quote'])}."
        ),
        "",
        "| 시장 | 일 회계 P&L | 전일 | 7일 | 일 RT P&L / 건 / 승률 | 수수료 / 회전 | 일 return / DD / coverage | 현재 position / equity | fresh | ops | recon | 상태 |",
        "| --- | ---: | ---: | ---: | --- | --- | ---: | --- | ---: | --- | --- | --- |",
    ]
    for row in d2["instances"]:
        if not row.get("available"):
            lines.append(f"| {row['market']} | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A | SOURCE ERROR |")
            continue
        metrics = row["metrics"]
        lines.append(
            "| {market} | {daily} | {previous} | {rolling} | {rt_pnl} / {trades} / {win_rate} | {fees} / {turnover} | {period_return} / {dd} / {coverage} | {position} / {equity} | {fresh} | {ops} | {recon} | {status} |".format(
                market=row["market"],
                daily=_krw(metrics["daily"]["realized_pnl_quote"], signed=True),
                previous=_krw(metrics["previous_day"]["realized_pnl_quote"], signed=True),
                rolling=_krw(metrics["trailing_7d"]["realized_pnl_quote"], signed=True),
                rt_pnl=_krw(metrics["daily"]["completed_round_trip_pnl_quote"], signed=True),
                trades=metrics["daily"]["completed_trades"],
                win_rate=_percent(metrics["daily"]["win_rate"]),
                fees=_krw(metrics["daily"]["fees_quote"]),
                turnover=_krw(metrics["daily"]["turnover_quote"]),
                dd=_percent(row["equity"]["sampled_daily_drawdown"]),
                period_return=_percent(row["equity"]["return"], signed=True),
                coverage=_d2_equity_coverage(row),
                position=("flat" if abs(float(row["current"]["base_quantity"])) <= POSITION_EPSILON else f"{float(row['current']['base_quantity']):.8g}"),
                equity=_krw(row["current"]["equity_quote"]),
                fresh=f"{float(row['current']['feed_age_seconds']):.0f}s" if row["current"]["feed_age_seconds"] is not None else "N/A",
                ops=(
                    f"cont/halt/recover {row['operations']['continuity_events']}/"
                    f"{row['operations']['halt_events']}/"
                    f"{row['operations']['recovery_events']}"
                    + (
                        f"; {_d2_audit_backlog_text(row)}"
                        if _d2_audit_backlog_text(row)
                        else ""
                    )
                ),
                recon="PASS" if row["reconciliation"]["ok"] else "FAIL",
                status=row["current"]["lifecycle_status"],
            )
        )
    lines.extend(
        [
            "",
            (
                "D2 수치는 알파 성과가 아니라 체결·비용·회계 배관 진단 관측치입니다."
                if d2_title == "D2 bounded diagnostic"
                else "D2 observe는 공개피드 관찰 전용이며 결정·주문·체결을 생성하지 않습니다."
                if d2_title == "D2 public-feed observe"
                else "D2 행별 strategy_role에 따라 bounded diagnostic과 주문 없는 공개피드 observe를 구분합니다."
            ),
        ]
    )
    if has_d2_profile_contract:
        lines.extend(
            [
                "D2 지표는 current active ledger only입니다. 이전 diagnostic terminal 성과는 transition receipt와 동결 원장에 보존하며 자동 합산하지 않습니다.",
                "D2 per-instance notifier OFF는 승인 profile의 정상 조건입니다. audit pending backlog는 관찰 품질이며 notifier 장애 판정이 아닙니다.",
            ]
        )
    lines.extend(
        [
            "",
            "## C2 forward paper",
            "",
            (
                f"일 회계 실현손익 {_krw(c2['summary']['daily']['realized_pnl_quote'], signed=True)}, "
                f"완료 거래 {c2['summary']['daily']['completed_trades']}, "
                f"수수료 {_krw(c2['summary']['daily']['fees_quote'])}."
            ),
            "",
            "| 시장 | 일 회계 P&L | 전일 | 7일 | 일 RT P&L / 건 / 승률 | 수수료 / 회전 | 일 return / DD / coverage | 현재 position / equity | fresh | ops | recon | 상태 |",
            "| --- | ---: | ---: | ---: | --- | --- | ---: | --- | ---: | --- | --- | --- |",
        ]
    )
    for row in c2["instances"]:
        if not row.get("available"):
            lines.append(f"| {row['market']} | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A | SOURCE ERROR |")
            continue
        metrics = row["metrics"]
        lines.append(
            "| {market} | {daily} | {previous} | {rolling} | {rt_pnl} / {trades} / {win_rate} | {fees} / {turnover} | {period_return} / N/A / {coverage} | {position} / {equity} | {fresh} | {ops} | {recon} | {status} |".format(
                market=row["market"],
                daily=_krw(metrics["daily"]["realized_pnl_quote"], signed=True),
                previous=_krw(metrics["previous_day"]["realized_pnl_quote"], signed=True),
                rolling=_krw(metrics["trailing_7d"]["realized_pnl_quote"], signed=True),
                rt_pnl=_krw(metrics["daily"]["completed_round_trip_pnl_quote"], signed=True),
                trades=metrics["daily"]["completed_trades"],
                win_rate=_percent(metrics["daily"]["win_rate"]),
                fees=_krw(metrics["daily"]["fees_quote"]),
                turnover=_krw(metrics["daily"]["turnover_quote"]),
                period_return=_percent(row["equity"]["return"], signed=True),
                coverage=_c2_equity_coverage(row),
                position=("flat" if abs(float(row["current"]["base_quantity"])) <= POSITION_EPSILON else f"{float(row['current']['base_quantity']):.8g}"),
                equity=_krw(row["current"]["equity_quote"]),
                fresh=f"{float(row['current']['age_seconds']):.0f}s",
                ops=f"events {sum(row['operations']['daily_event_counts'].values())}",
                recon="PASS" if row["reconciliation"]["ok"] else "FAIL",
                status=row["current"]["halt_state"],
            )
        )
    improvement = report["improvement"]
    lines.extend(
        [
            "",
            "## 개선 gate",
            "",
            f"- 상태: `{improvement['gate']}`",
            f"- C2 공통 관측: {improvement['common_c2_observation_days']}/30일",
            f"- 다음 단계: {improvement['next_step']}",
        ]
    )
    lines.extend(f"- 관측: {item}" for item in improvement["observations"])
    lines.extend(
        [
            "",
            "## 지표 계약과 한계",
            "",
            "- 회계 실현손익은 각 sell 시점의 배분원가와 수수료로 KST 일자에 귀속합니다. RT P&L·승률은 position이 flat으로 돌아온 최종 sell 일자 기준입니다.",
            "- D2 equity는 일중 저장 sample로 계산하며 경계 lag를 JSON에 보존합니다.",
            "- C2 non-flat equity는 최근 완료 60분봉 close 기반 liquidation 추정치이며 실제 boundary NAV가 아닙니다.",
            "- D2와 C2 수익률은 전략 역할이 달라 하나의 전략 수익률로 합산하지 않습니다.",
            "- 자동 전략 변경·halt rearm·원장 재사용은 수행하지 않습니다.",
            "",
        ]
    )
    return "\n".join(lines)


def slack_message(report: Mapping[str, Any]) -> dict[str, Any]:
    safety_ok = bool(report["safety"]["verified"])
    d2 = report["D2"]
    c2 = report["C2"]
    d2_title = _d2_section_title(d2["instances"])
    has_d2_profile_contract = any(
        row.get("available") and "runtime_profile" in row
        for row in d2["instances"]
    )
    d2_summary_title = d2_title if has_d2_profile_contract else "D2 진단"
    icon = "📊" if safety_ok else "⚠️"
    safety_text = (
        "✅ PASS · `simulated=true` · `own_execution=false` · `live_order_routing=false` · `orders_sent=0`"
        if safety_ok
        else "🚨 FAIL/UNKNOWN · 안전·원장 상태를 즉시 확인하세요"
    )
    d2_lines = "\n".join(_compact_instance_line(row) for row in d2["instances"])
    c2_lines = "\n".join(_compact_instance_line(row) for row in c2["instances"])
    improvement = report["improvement"]
    fallback = (
        f"[CoinPilot Daily][{report['report_date']}] "
        f"D2 {_krw(d2['summary']['daily']['realized_pnl_quote'], signed=True)}, "
        f"C2 {_krw(c2['summary']['daily']['realized_pnl_quote'], signed=True)}, "
        f"safety={'PASS' if safety_ok else 'FAIL'}, {report['report_id']}"
    )
    blocks: list[dict[str, Any]] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{icon} CoinPilot 일일 모의거래 성적표",
                "emoji": True,
            },
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"🗓️ {report['window']['label']} · `{report['report_id']}`",
                }
            ],
        },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": (
                        f"*{d2_summary_title} · 일 회계 실현*\n"
                        f"{_krw(d2['summary']['daily']['realized_pnl_quote'], signed=True)} · "
                        f"RT {d2['summary']['daily']['completed_trades']} · "
                        f"fee {_krw(d2['summary']['daily']['fees_quote'])}"
                    ),
                },
                {
                    "type": "mrkdwn",
                    "text": (
                        "*C2 paper · 일 회계 실현*\n"
                        f"{_krw(c2['summary']['daily']['realized_pnl_quote'], signed=True)} · "
                        f"RT {c2['summary']['daily']['completed_trades']} · "
                        f"fee {_krw(c2['summary']['daily']['fees_quote'])}"
                    ),
                },
            ],
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*{d2_title}*\n{d2_lines}"},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*C2 forward paper*\n{c2_lines}"},
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*자가개선 gate* · `{improvement['gate']}`\n"
                    f"• C2 공통 관측 {improvement['common_c2_observation_days']}/30일\n"
                    f"• {improvement['next_step']}"
                ),
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*분석-only 관측*\n" + "\n".join(
                    f"• {item}" for item in improvement["observations"]
                ),
            },
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*안전 불변조건*\n{safety_text}"},
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": (
                        "D2는 alpha가 아닌 진단/공개피드 관찰 · C2 non-flat MTM은 별도 추정치 · "
                        + (
                            "D2 notifier OFF는 정상, audit unresolved는 관찰 품질 · "
                            "D2 current active ledger only, 과거 원장 자동 합산 없음 · "
                            if has_d2_profile_contract
                            else ""
                        )
                        + "자동 전략 변경/rearm 없음 · 상세 JSON/Markdown은 로컬 감사 보존"
                    ),
                }
            ],
        },
    ]
    for block in blocks:
        block_type = block.get("type")
        text_value = block.get("text")
        if isinstance(text_value, Mapping):
            limit = 150 if block_type == "header" else 3000
            if len(str(text_value.get("text", ""))) > limit:
                raise ScorecardError("Slack block text exceeds its limit")
        fields = block.get("fields", [])
        if not isinstance(fields, list) or len(fields) > 10:
            raise ScorecardError("Slack section has invalid fields")
        for field in fields:
            if not isinstance(field, Mapping) or len(str(field.get("text", ""))) > 2000:
                raise ScorecardError("Slack field exceeds 2000 characters")
        for element in block.get("elements", []):
            if not isinstance(element, Mapping) or len(str(element.get("text", ""))) > 3000:
                raise ScorecardError("Slack context element exceeds its limit")
    if len(blocks) > 50:
        raise ScorecardError("Slack message exceeds the Block Kit block limit")
    return {"text": fallback[:3000], "blocks": blocks}


def source_quality_alert(report: Mapping[str, Any]) -> dict[str, Any]:
    failures = [
        row
        for family in ("D2", "C2")
        for row in report[family]["instances"]
        if not row.get("available") or not row.get("safety", {}).get("verified")
    ]
    details = "\n".join(
        f"• {row['instance']} ({row['market']}): "
        f"{row.get('error', 'safety/fingerprint/reconciliation mismatch')}"
        for row in failures
    )
    if not details:
        details = "• aggregate safety validation failed"
    text = (
        f"[CoinPilot Daily][QUALITY][{report['report_date']}] "
        "daily scorecard not finalized; hourly retry remains active"
    )
    return {
        "text": text,
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "⚠️ CoinPilot 일일 성적표 품질 경고",
                    "emoji": True,
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"`{report['report_id']}`는 확정·delivered 처리하지 않았습니다. "
                        "source가 정상화되면 다시 계산해 전송합니다."
                    ),
                },
            },
            {"type": "section", "text": {"type": "mrkdwn", "text": details[:3000]}},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        "*안전 목표* · `simulated=true` · `own_execution=false` · "
                        "`live_order_routing=false` · `orders_sent=0`\n"
                        "자동 전략 변경·rearm·원장 재사용은 수행하지 않습니다."
                    ),
                },
            },
        ],
    }


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def validate_slack_webhook_url(url: str) -> None:
    try:
        parsed = urllib.parse.urlsplit(url)
    except ValueError as exc:
        raise ScorecardError("Slack webhook URL is malformed") from exc
    parts = [part for part in parsed.path.split("/") if part]
    if (
        parsed.scheme != "https"
        or parsed.hostname != "hooks.slack.com"
        or parsed.port is not None
        or parsed.username is not None
        or parsed.password is not None
        or len(parts) != 4
        or parts[0] != "services"
        or parsed.query
        or parsed.fragment
    ):
        raise ScorecardError("Slack webhook must be an exact hooks.slack.com/services URL")


def read_slack_webhook_from_keychain(*, service: str, account: str) -> str:
    if not service.strip() or not account.strip():
        raise ScorecardError("Keychain service and account are required")
    try:
        result = subprocess.run(
            [
                "/usr/bin/security",
                "find-generic-password",
                "-a",
                account,
                "-s",
                service,
                "-w",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ScorecardError("Slack webhook is unavailable in macOS Keychain") from exc
    secret = result.stdout.strip()
    validate_slack_webhook_url(secret)
    return secret


class SlackWebhookClient:
    def __init__(self, webhook_url: str, *, timeout_seconds: float = 10.0) -> None:
        validate_slack_webhook_url(webhook_url)
        self._url = webhook_url
        self._timeout = timeout_seconds
        self._opener = urllib.request.build_opener(_NoRedirect())

    def send(self, message: Mapping[str, Any]) -> None:
        body = _canonical_json(dict(message)).encode("utf-8")
        request = urllib.request.Request(
            self._url,
            data=body,
            headers={
                "Content-Type": "application/json; charset=utf-8",
                "User-Agent": "coinpilot-daily-scorecard/1",
            },
            method="POST",
        )
        try:
            with self._opener.open(request, timeout=self._timeout) as response:
                if int(response.status) != 200:
                    raise SlackDeliveryError(f"Slack returned HTTP {int(response.status)}")
                response.read(32)
        except urllib.error.HTTPError as exc:
            retry_after: float | None = None
            if exc.code == 429:
                try:
                    retry_after = max(1.0, float(exc.headers.get("Retry-After", "1")))
                except (TypeError, ValueError):
                    retry_after = 1.0
            raise SlackDeliveryError(
                f"Slack returned HTTP {exc.code}",
                retry_after_seconds=retry_after,
            ) from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise SlackDeliveryError("Slack delivery failed") from exc


def _atomic_write(path: Path, content: bytes, *, replace: bool = True) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    _reject_symlink_components(path.parent)
    if path.exists() and path.is_symlink():
        raise ScorecardError(f"refusing symlinked output: {path.name}")
    if not replace and path.exists():
        raise ScorecardError(f"refusing to overwrite report artifact: {path.name}")
    os.chmod(path.parent, 0o700)
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp"
    )
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    descriptor: int | None = None
    try:
        descriptor = os.open(temporary, flags, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = None
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    finally:
        if descriptor is not None:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def _artifact_paths(artifact_dir: Path, report_date: str) -> dict[str, Path]:
    stem = f"coinpilot-daily-scorecard-v{REPORT_SCHEMA_VERSION}-{report_date}"
    return {
        "json": artifact_dir / f"{stem}.json",
        "markdown": artifact_dir / f"{stem}.md",
        "checksum": artifact_dir / f"{stem}.sha256",
    }


def write_report_artifacts(report: Mapping[str, Any], artifact_dir: Path) -> dict[str, str]:
    paths = _artifact_paths(artifact_dir, str(report["report_date"]))
    for path in paths.values():
        _reject_symlink_components(path)
    if paths["json"].exists():
        json_stat = paths["json"].stat()
        if stat.S_IMODE(json_stat.st_mode) != 0o600 or json_stat.st_nlink != 1:
            raise ScorecardError("existing report JSON has unsafe ownership mode")
        json_bytes = paths["json"].read_bytes()
        digest = _sha256_bytes(json_bytes)
        frozen = json.loads(json_bytes)
        frozen_safety = (
            frozen.get("safety") if isinstance(frozen, Mapping) else None
        )
        if (
            not isinstance(frozen, Mapping)
            or frozen.get("report_id") != report.get("report_id")
            or frozen.get("report_date") != report.get("report_date")
            or not isinstance(frozen_safety, Mapping)
            or not frozen_safety.get("verified")
        ):
            raise ScorecardError("existing report artifact bundle failed verification")
        expected_markdown = scorecard_markdown(frozen).encode("utf-8")
        expected_checksum = f"{digest}  {paths['json'].name}\n".encode("ascii")
        if paths["markdown"].exists():
            markdown_stat = paths["markdown"].stat()
            if stat.S_IMODE(markdown_stat.st_mode) != 0o600 or markdown_stat.st_nlink != 1:
                raise ScorecardError("existing report markdown has unsafe mode")
            if paths["markdown"].read_bytes() != expected_markdown:
                raise ScorecardError("existing report markdown failed verification")
        else:
            _atomic_write(paths["markdown"], expected_markdown, replace=False)
        if paths["checksum"].exists():
            checksum_stat = paths["checksum"].stat()
            if stat.S_IMODE(checksum_stat.st_mode) != 0o600 or checksum_stat.st_nlink != 1:
                raise ScorecardError("existing report checksum has unsafe mode")
            if paths["checksum"].read_bytes() != expected_checksum:
                raise ScorecardError("existing report checksum failed verification")
        else:
            _atomic_write(paths["checksum"], expected_checksum, replace=False)
        return {
            "json": str(paths["json"]),
            "markdown": str(paths["markdown"]),
            "checksum": str(paths["checksum"]),
            "sha256": digest,
        }
    if paths["markdown"].exists() or paths["checksum"].exists():
        raise ScorecardError("report bundle is missing its JSON source of truth")
    json_bytes = (_canonical_json(report, pretty=True) + "\n").encode("utf-8")
    markdown_bytes = scorecard_markdown(report).encode("utf-8")
    digest = _sha256_bytes(json_bytes)
    checksum = f"{digest}  {paths['json'].name}\n".encode("ascii")
    _atomic_write(paths["json"], json_bytes, replace=False)
    _atomic_write(paths["markdown"], markdown_bytes, replace=False)
    _atomic_write(paths["checksum"], checksum, replace=False)
    return {
        "json": str(paths["json"]),
        "markdown": str(paths["markdown"]),
        "checksum": str(paths["checksum"]),
        "sha256": digest,
    }


def _delivery_path(state_dir: Path, report_date: str) -> Path:
    return state_dir / "deliveries" / f"{report_date}.json"


def _load_json(path: Path) -> dict[str, Any] | None:
    _reject_symlink_components(path)
    if not path.is_file():
        return None
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ScorecardError(f"invalid state document: {path.name}")
    return value


def _load_frozen_report(artifacts: Mapping[str, Any]) -> dict[str, Any]:
    path = Path(_required_text(artifacts.get("json"), "artifact json path"))
    _reject_symlink_components(path)
    expected = _required_text(artifacts.get("sha256"), "artifact sha256")
    content = path.read_bytes()
    if _sha256_bytes(content) != expected:
        raise ScorecardError("frozen report artifact checksum mismatch")
    report = json.loads(content)
    if not isinstance(report, dict):
        raise ScorecardError("frozen report artifact is invalid")
    return report


def _validate_delivery_state(
    *,
    path: Path,
    state: Mapping[str, Any],
    state_dir: Path,
) -> date:
    _reject_symlink_components(path)
    path_stat = path.stat()
    if stat.S_IMODE(path_stat.st_mode) != 0o600 or path_stat.st_nlink != 1:
        raise ScorecardError("delivery receipt must be a private regular file")
    try:
        state_date = date.fromisoformat(path.stem)
        report_date = date.fromisoformat(str(state.get("report_date")))
        coverage_start = date.fromisoformat(str(state.get("coverage_start_date")))
    except ValueError as exc:
        raise ScorecardError(f"invalid delivery receipt date: {path.name}") from exc
    if (
        state.get("schema_version") != 1
        or report_date != state_date
        or state.get("report_id") != window_for_date(state_date).report_id
        or state.get("status") not in {"pending", "sending", "delivered"}
        or coverage_start > state_date
        or (state_date - coverage_start).days > 3650
    ):
        raise ScorecardError(f"invalid delivery receipt: {path.name}")
    for field in ("attempt_count", "available_wall_ns", "created_wall_ns"):
        value = state.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ScorecardError(f"invalid delivery receipt field: {field}")
    if state.get("status") == "delivered" and not isinstance(
        state.get("delivered_wall_ns"), int
    ):
        raise ScorecardError("delivered receipt has no delivery timestamp")
    artifacts = _required_mapping(state.get("artifacts"), "delivery artifacts")
    expected_paths = _artifact_paths(state_dir.parent / "reports", state_date.isoformat())
    for name in ("json", "markdown", "checksum"):
        actual = Path(_required_text(artifacts.get(name), f"artifact {name} path"))
        if actual != expected_paths[name] or not actual.is_file():
            raise ScorecardError(f"delivery artifact path mismatch: {name}")
        _reject_symlink_components(actual)
        artifact_stat = actual.stat()
        if stat.S_IMODE(artifact_stat.st_mode) != 0o600 or artifact_stat.st_nlink != 1:
            raise ScorecardError(f"delivery artifact mode is unsafe: {name}")
    report = _load_frozen_report(artifacts)
    report_safety = _required_mapping(report.get("safety"), "report safety")
    if (
        report.get("report_id") != window_for_date(state_date).report_id
        or report.get("report_date") != state_date.isoformat()
        or not report_safety.get("verified")
        or expected_paths["markdown"].read_bytes()
        != scorecard_markdown(report).encode("utf-8")
    ):
        raise ScorecardError("delivery artifact content mismatch")
    digest = _required_text(artifacts.get("sha256"), "artifact sha256")
    checksum = f"{digest}  {expected_paths['json'].name}\n".encode("ascii")
    if expected_paths["checksum"].read_bytes() != checksum:
        raise ScorecardError("delivery artifact checksum receipt mismatch")
    return state_date


def _next_retry_wall_ns(
    attempt_count: int,
    now_wall_ns: int,
    retry_after_seconds: float | None,
) -> int:
    exponential = min(
        MAX_RETRY_SECONDS,
        DEFAULT_RETRY_SECONDS * (2 ** min(max(0, attempt_count - 1), 8)),
    )
    delay = max(exponential, int(retry_after_seconds or 0))
    return now_wall_ns + delay * NANOSECONDS


def _notify_source_incomplete(
    *,
    report: Mapping[str, Any],
    state_dir: Path,
    keychain_service: str,
    keychain_account: str,
    now_wall_ns: int,
    client: SlackClient | None,
) -> None:
    incident_dir = state_dir / "incidents"
    incident_path = incident_dir / f"{report['report_date']}.json"
    _reject_symlink_components(incident_path)
    failure_material = [
        {
            "instance": row.get("instance"),
            "available": row.get("available"),
            "error": row.get("error"),
            "safety": row.get("safety"),
        }
        for family in ("D2", "C2")
        for row in report[family]["instances"]
        if not row.get("available") or not row.get("safety", {}).get("verified")
    ]
    signature = _sha256_bytes(_canonical_json(failure_material).encode("utf-8"))
    previous = _load_json(incident_path)
    if previous is not None:
        if (
            previous.get("signature") == signature
            and isinstance(previous.get("sent_wall_ns"), int)
            and now_wall_ns - int(previous["sent_wall_ns"])
            < 6 * 60 * 60 * NANOSECONDS
        ):
            return
    selected_client = client
    if selected_client is None:
        webhook = read_slack_webhook_from_keychain(
            service=keychain_service,
            account=keychain_account,
        )
        selected_client = SlackWebhookClient(webhook)
    selected_client.send(source_quality_alert(report))
    receipt = {
        "schema_version": 1,
        "report_id": report["report_id"],
        "report_date": report["report_date"],
        "signature": signature,
        "sent_wall_ns": now_wall_ns,
        "final_scorecard_delivered": False,
        "simulated": True,
        "own_execution": False,
        "live_order_routing": False,
        "orders_sent": 0,
    }
    _atomic_write(
        incident_path,
        (_canonical_json(receipt, pretty=True) + "\n").encode("utf-8"),
    )


def run_delivery(
    *,
    root: Path,
    state_dir: Path,
    artifact_dir: Path,
    window: ReportWindow,
    keychain_service: str,
    keychain_account: str,
    now_wall_ns: int | None = None,
    client: SlackClient | None = None,
    schedule_hour: int = DEFAULT_SCHEDULE_HOUR,
    schedule_minute: int = DEFAULT_SCHEDULE_MINUTE,
) -> dict[str, Any]:
    now_ns = time.time_ns() if now_wall_ns is None else now_wall_ns
    root, state_dir, artifact_dir = _validate_reporting_paths(
        root=root,
        state_dir=state_dir,
        artifact_dir=artifact_dir,
    )
    latest_due = latest_due_window(
        now_ns,
        schedule_hour=schedule_hour,
        schedule_minute=schedule_minute,
    )
    if window.report_date > latest_due.report_date:
        raise ScorecardError("refusing to freeze or deliver an incomplete report date")
    _reject_symlink_components(state_dir)
    _reject_symlink_components(artifact_dir)
    state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    artifact_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    _reject_symlink_components(state_dir)
    _reject_symlink_components(artifact_dir)
    os.chmod(state_dir, 0o700)
    os.chmod(artifact_dir, 0o700)
    lock_path = state_dir / "daily-scorecard.lock"
    lock_flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(lock_path, lock_flags, 0o600)
    try:
        lock_stat = os.fstat(descriptor)
        if not stat.S_ISREG(lock_stat.st_mode) or lock_stat.st_nlink != 1:
            raise ScorecardError("reporting lock is not a private regular file")
        os.fchmod(descriptor, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {"status": "busy", "report_id": window.report_id}
        state_path = _delivery_path(state_dir, window.report_date.isoformat())
        _reject_symlink_components(state_path)
        state = _load_json(state_path)
        if state is not None:
            _validate_delivery_state(
                path=state_path,
                state=state,
                state_dir=state_dir,
            )
        if state is not None and state.get("report_id") != window.report_id:
            raise ScorecardError("delivery state report ID mismatch")
        if state is not None and state.get("status") == "delivered":
            return {
                "status": "already_delivered",
                "report_id": window.report_id,
                "delivered_wall_ns": state.get("delivered_wall_ns"),
            }
        if state is not None:
            available = int(state.get("available_wall_ns", 0))
            lease = int(state.get("lease_expires_wall_ns", 0) or 0)
            if state.get("status") == "sending" and lease > now_ns:
                return {"status": "leased", "report_id": window.report_id}
            if available > now_ns:
                return {
                    "status": "backoff",
                    "report_id": window.report_id,
                    "available_wall_ns": available,
                }
        if state is None:
            report = build_scorecard(
                root=root,
                window=window,
                generated_wall_ns=now_ns,
            )
            if not report.get("safety", {}).get("verified"):
                _notify_source_incomplete(
                    report=report,
                    state_dir=state_dir,
                    keychain_service=keychain_service,
                    keychain_account=keychain_account,
                    now_wall_ns=now_ns,
                    client=client,
                )
                raise ScorecardError(
                    "daily source completeness or safety validation failed; retry required"
                )
            artifacts = write_report_artifacts(report, artifact_dir)
            report = _load_frozen_report(artifacts)
            state = {
                "schema_version": 1,
                "report_id": window.report_id,
                "report_date": window.report_date.isoformat(),
                "status": "pending",
                "attempt_count": 0,
                "available_wall_ns": now_ns,
                "lease_expires_wall_ns": None,
                "created_wall_ns": now_ns,
                "delivered_wall_ns": None,
                "last_error": None,
                "coverage_start_date": report["coverage"]["start_date"],
                "artifacts": artifacts,
            }
            _atomic_write(
                state_path,
                (_canonical_json(state, pretty=True) + "\n").encode("utf-8"),
            )
        else:
            report = _load_frozen_report(state["artifacts"])
        attempt_count = int(state.get("attempt_count", 0)) + 1
        state.update(
            {
                "status": "sending",
                "attempt_count": attempt_count,
                "available_wall_ns": now_ns,
                "lease_expires_wall_ns": now_ns + DELIVERY_LEASE_SECONDS * NANOSECONDS,
                "last_error": None,
            }
        )
        _atomic_write(
            state_path,
            (_canonical_json(state, pretty=True) + "\n").encode("utf-8"),
        )
        try:
            selected_client = client
            if selected_client is None:
                webhook = read_slack_webhook_from_keychain(
                    service=keychain_service,
                    account=keychain_account,
                )
                selected_client = SlackWebhookClient(webhook)
            selected_client.send(slack_message(report))
        except (SlackDeliveryError, ScorecardError) as exc:
            retry_after = (
                exc.retry_after_seconds
                if isinstance(exc, SlackDeliveryError)
                else None
            )
            state.update(
                {
                    "status": "pending",
                    "available_wall_ns": _next_retry_wall_ns(
                        attempt_count,
                        now_ns,
                        retry_after,
                    ),
                    "lease_expires_wall_ns": None,
                    "last_error": type(exc).__name__,
                }
            )
            _atomic_write(
                state_path,
                (_canonical_json(state, pretty=True) + "\n").encode("utf-8"),
            )
            return {
                "status": "retry_scheduled",
                "report_id": window.report_id,
                "attempt_count": attempt_count,
                "available_wall_ns": state["available_wall_ns"],
            }
        state.update(
            {
                "status": "delivered",
                "available_wall_ns": now_ns,
                "lease_expires_wall_ns": None,
                "delivered_wall_ns": now_ns,
                "last_error": None,
            }
        )
        _atomic_write(
            state_path,
            (_canonical_json(state, pretty=True) + "\n").encode("utf-8"),
        )
        return {
            "status": "delivered",
            "report_id": window.report_id,
            "attempt_count": attempt_count,
            "artifacts": state["artifacts"],
        }
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


def delivery_status(
    state_dir: Path,
    *,
    now_wall_ns: int | None = None,
) -> dict[str, Any]:
    selected_state = state_dir.expanduser().absolute()
    _reject_symlink_components(selected_state)
    delivery_dir = selected_state / "deliveries"
    paths = tuple(sorted(delivery_dir.glob("*.json"))) if delivery_dir.is_dir() else ()
    states: dict[date, dict[str, Any]] = {}
    for path in paths:
        state = _load_json(path)
        if state is None:
            raise ScorecardError(f"empty delivery receipt: {path.name}")
        state_date = _validate_delivery_state(
            path=path,
            state=state,
            state_dir=selected_state,
        )
        states[state_date] = state
    now_ns = time.time_ns() if now_wall_ns is None else now_wall_ns
    latest_due = latest_due_window(now_ns).report_date
    latest_due_state = states.get(latest_due)
    coverage = min(
        (
            date.fromisoformat(str(state["coverage_start_date"]))
            for state in states.values()
        ),
        default=latest_due,
    )
    missing = 0
    candidate = coverage
    while candidate <= latest_due:
        if candidate not in states:
            missing += 1
        candidate += timedelta(days=1)
    unresolved = sum(state.get("status") != "delivered" for state in states.values())
    latest = states[max(states)] if states else None
    return {
        "delivery_count": len(paths),
        "latest_due_report_date": latest_due.isoformat(),
        "latest_due_delivered": bool(
            latest_due_state and latest_due_state.get("status") == "delivered"
        ),
        "missing_backlog_days": missing,
        "unresolved_receipts": unresolved,
        "healthy": bool(
            latest_due_state
            and latest_due_state.get("status") == "delivered"
            and missing == 0
            and unresolved == 0
        ),
        "latest": latest,
    }


def select_scheduled_window(
    *,
    state_dir: Path,
    now_wall_ns: int,
    schedule_hour: int = DEFAULT_SCHEDULE_HOUR,
    schedule_minute: int = DEFAULT_SCHEDULE_MINUTE,
) -> ReportWindow:
    """Select one due day without permanently skipping an outage backlog.

    A newly observed latest day is always delivered first. Subsequent hourly
    invocations drain one older missing or retryable receipt at a time, so a
    long outage neither loses reports nor emits an unbounded message burst.
    """

    latest = latest_due_window(
        now_wall_ns,
        schedule_hour=schedule_hour,
        schedule_minute=schedule_minute,
    )
    delivery_dir = state_dir.expanduser().absolute() / "deliveries"
    _reject_symlink_components(delivery_dir)
    paths = tuple(sorted(delivery_dir.glob("*.json"))) if delivery_dir.is_dir() else ()
    states: dict[date, dict[str, Any]] = {}
    for path in paths:
        state = _load_json(path)
        if state is None:
            raise ScorecardError(f"empty delivery receipt: {path.name}")
        state_date = _validate_delivery_state(
            path=path,
            state=state,
            state_dir=state_dir.expanduser().absolute(),
        )
        if state_date > latest.report_date:
            raise ScorecardError(f"future delivery receipt is not allowed: {path.name}")
        states[state_date] = state

    if latest.report_date not in states:
        return latest

    ready_retries: list[date] = []
    for state_date, state in states.items():
        if state_date > latest.report_date or state.get("status") == "delivered":
            continue
        available = int(state.get("available_wall_ns", 0))
        lease = int(state.get("lease_expires_wall_ns", 0) or 0)
        if available <= now_wall_ns and lease <= now_wall_ns:
            ready_retries.append(state_date)
    if ready_retries:
        return window_for_date(min(ready_retries))

    coverage_candidates = [
        date.fromisoformat(str(state["coverage_start_date"]))
        for state in states.values()
        if state.get("coverage_start_date")
    ]
    coverage_start = min(coverage_candidates, default=latest.report_date)
    candidate = coverage_start
    while candidate <= latest.report_date:
        if candidate not in states:
            return window_for_date(candidate)
        candidate += timedelta(days=1)
    return latest


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build and deliver one read-only D2+C2 KST daily scorecard."
    )
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--state-dir", required=True, type=Path)
    parser.add_argument("--artifact-dir", required=True, type=Path)
    parser.add_argument("--keychain-service", default="coinpilot-slack-webhook")
    parser.add_argument("--keychain-account", default="coinpilot-shadow")
    parser.add_argument("--schedule-hour", type=int, default=DEFAULT_SCHEDULE_HOUR)
    parser.add_argument("--schedule-minute", type=int, default=DEFAULT_SCHEDULE_MINUTE)
    parser.add_argument("--date", type=date.fromisoformat)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--status", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    root, state_dir, artifact_dir = _validate_reporting_paths(
        root=args.root,
        state_dir=args.state_dir,
        artifact_dir=args.artifact_dir,
    )
    if args.status:
        print(_canonical_json(delivery_status(state_dir), pretty=True))
        return 0
    now_wall_ns = time.time_ns()
    window = (
        window_for_date(args.date)
        if args.date is not None
        else select_scheduled_window(
            state_dir=state_dir,
            now_wall_ns=now_wall_ns,
            schedule_hour=args.schedule_hour,
            schedule_minute=args.schedule_minute,
        )
    )
    if window.report_date > latest_due_window(
        now_wall_ns,
        schedule_hour=args.schedule_hour,
        schedule_minute=args.schedule_minute,
    ).report_date:
        raise ScorecardError("refusing an incomplete or future report date")
    if args.dry_run:
        report = build_scorecard(
            root=root,
            window=window,
            generated_wall_ns=now_wall_ns,
        )
        print(scorecard_markdown(report))
        return 0 if report["safety"]["verified"] else 2
    result = run_delivery(
        root=root,
        state_dir=state_dir,
        artifact_dir=artifact_dir,
        window=window,
        keychain_service=args.keychain_service,
        keychain_account=args.keychain_account,
        now_wall_ns=now_wall_ns,
        schedule_hour=args.schedule_hour,
        schedule_minute=args.schedule_minute,
    )
    print(_canonical_json({**result, "orders_sent": 0}))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, KeyError, json.JSONDecodeError, sqlite3.Error, ScorecardError) as exc:
        print(f"error: {_safe_error(exc)}", file=sys.stderr)
        raise SystemExit(2) from exc
