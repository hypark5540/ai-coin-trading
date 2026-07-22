#!/usr/bin/env python3
"""Low-noise hourly Slack summaries for CoinPilot shadow instances.

The shadow runtime keeps writing its normal event outbox as an audit trail.  This
process terminally suppresses those per-event deliveries and sends only one
summary for the latest completed fixed wall-clock window. Summary creation is
deduplicated by window, while delivery is leased and retried through the
existing SQLite outbox.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import signal
import socket
import sqlite3
import sys
import time
import tomllib
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import FrameType
from typing import Any, Callable, Mapping, Protocol, Sequence
from zoneinfo import ZoneInfo

from coinpilot.shadow_ops import (
    SlackDeliveryError,
    SlackWebhookClient,
    read_slack_webhook_from_keychain,
)


NANOSECONDS = 1_000_000_000
SUMMARY_TOPIC = "shadow_hourly_summary"
SUMMARY_VERSION = 1
SEOUL = ZoneInfo("Asia/Seoul")
SUPPRESSION_REASON = "suppressed_by_hourly_summary_policy"
SUPERSEDED_REASON = "superseded_by_latest_hourly_summary"


class SlackClient(Protocol):
    def send(self, message: Mapping[str, Any]) -> None: ...


@dataclass(frozen=True, slots=True)
class NotifierSettings:
    config_path: Path
    database_path: Path
    market: str
    keychain_service: str
    keychain_account: str


@dataclass(frozen=True, slots=True)
class SummaryWindow:
    start_wall_ns: int
    end_wall_ns: int

    @property
    def start_local(self) -> datetime:
        return datetime.fromtimestamp(self.start_wall_ns / NANOSECONDS, SEOUL)

    @property
    def end_local(self) -> datetime:
        return datetime.fromtimestamp(self.end_wall_ns / NANOSECONDS, SEOUL)

    @property
    def label(self) -> str:
        start = self.start_local
        end = self.end_local
        if start.date() == end.date():
            return f"{start:%Y-%m-%d %H:%M}–{end:%H:%M} KST"
        return f"{start:%Y-%m-%d %H:%M}–{end:%Y-%m-%d %H:%M} KST"


def _required_text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value.strip()


def _positive_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return value


def load_settings(config_path: str | Path) -> NotifierSettings:
    path = Path(config_path).expanduser().resolve()
    try:
        with path.open("rb") as handle:
            raw = tomllib.load(handle)
    except OSError as exc:
        raise ValueError(f"cannot read notifier config: {path}") from exc

    data = raw.get("data")
    shadow = raw.get("shadow")
    operations = raw.get("operations")
    if not all(isinstance(item, dict) for item in (data, shadow, operations)):
        raise ValueError("config must contain data, shadow, and operations tables")

    market = _required_text(data.get("market"), "data.market").upper()
    if "-" not in market:
        raise ValueError("data.market must look like KRW-BTC")
    database = Path(
        _required_text(shadow.get("database_path"), "shadow.database_path")
    ).expanduser()
    if not database.is_absolute():
        database = path.parent / database

    return NotifierSettings(
        config_path=path,
        database_path=database.resolve(),
        market=market,
        keychain_service=_required_text(
            operations.get("keychain_service"),
            "operations.keychain_service",
        ),
        keychain_account=_required_text(
            operations.get("keychain_account"),
            "operations.keychain_account",
        ),
    )


def latest_completed_window(
    now_wall_ns: int,
    *,
    interval_seconds: int = 3600,
    grace_seconds: int = 15,
) -> SummaryWindow:
    _positive_int(interval_seconds, "interval_seconds")
    if isinstance(grace_seconds, bool) or not isinstance(grace_seconds, int):
        raise ValueError("grace_seconds must be a non-negative integer")
    if grace_seconds < 0 or grace_seconds >= interval_seconds:
        raise ValueError("grace_seconds must be smaller than interval_seconds")
    if isinstance(now_wall_ns, bool) or not isinstance(now_wall_ns, int):
        raise ValueError("now_wall_ns must be an integer")
    if now_wall_ns <= (interval_seconds + grace_seconds) * NANOSECONDS:
        raise ValueError("now_wall_ns is too early to form a completed window")

    interval_ns = interval_seconds * NANOSECONDS
    effective_now = now_wall_ns - grace_seconds * NANOSECONDS
    end_wall_ns = (effective_now // interval_ns) * interval_ns
    return SummaryWindow(end_wall_ns - interval_ns, end_wall_ns)


def _connect(database_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(database_path, timeout=10.0, isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 10000")
    return connection


def ensure_reporting_indexes(database_path: Path) -> None:
    """Create bounded-window indexes without changing application tables."""

    connection = _connect(database_path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_shadow_fills_wall
            ON shadow_fills(created_wall_ns, fill_id)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_shadow_equity_wall
            ON shadow_equity(created_wall_ns DESC, equity_id DESC)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_notification_outbox_topic_wall
            ON notification_outbox(topic, created_wall_ns)
            """
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def _one(
    connection: sqlite3.Connection,
    sql: str,
    parameters: Sequence[object] = (),
) -> dict[str, Any] | None:
    row = connection.execute(sql, parameters).fetchone()
    return None if row is None else dict(row)


def _number(value: object, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return default
    try:
        parsed = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default
    return parsed if math.isfinite(parsed) else default


def _integer(value: object, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError, OverflowError):
        return default


def _equity_before(
    connection: sqlite3.Connection,
    *,
    market: str,
    boundary_wall_ns: int,
) -> dict[str, Any] | None:
    return _one(
        connection,
        """
        SELECT e.equity_quote, e.cash_quote, e.base_quantity,
               e.liquidation_bid, e.drawdown, e.created_wall_ns
        FROM shadow_equity AS e
        JOIN shadow_runs AS r ON r.run_id = e.run_id
        WHERE r.market = ? AND e.created_wall_ns < ?
        ORDER BY e.created_wall_ns DESC, e.equity_id DESC
        LIMIT 1
        """,
        (market, boundary_wall_ns),
    )


def _latest_state(
    connection: sqlite3.Connection,
    *,
    market: str,
) -> dict[str, Any] | None:
    return _one(
        connection,
        """
        SELECT r.run_id, r.status AS run_status, r.halt_reason AS run_halt_reason,
               s.lifecycle_status, s.halt_reason, s.last_equity_quote,
               s.cash_quote, s.base_quantity, s.updated_wall_ns
        FROM shadow_runs AS r
        JOIN shadow_state AS s ON s.run_id = r.run_id
        WHERE r.market = ?
        ORDER BY r.started_wall_ns DESC
        LIMIT 1
        """,
        (market,),
    )


def aggregate_window(
    connection: sqlite3.Connection,
    *,
    market: str,
    window: SummaryWindow,
) -> dict[str, Any]:
    start_ns = window.start_wall_ns
    end_ns = window.end_wall_ns
    fills = _one(
        connection,
        """
        SELECT COUNT(*) AS fill_count,
               COALESCE(SUM(f.side = 'buy'), 0) AS buy_count,
               COALESCE(SUM(f.side = 'sell'), 0) AS sell_count,
               COALESCE(SUM(f.filled_quote), 0.0) AS turnover_quote,
               COALESCE(SUM(f.fee_quote), 0.0) AS fees_quote
        FROM shadow_fills AS f
        JOIN shadow_runs AS r ON r.run_id = f.run_id
        WHERE r.market = ?
          AND f.created_wall_ns >= ?
          AND f.created_wall_ns < ?
        """,
        (market, start_ns, end_ns),
    ) or {}

    completed = _one(
        connection,
        """
        WITH prior_fill AS (
            SELECT f.side, f.filled_quote, f.fee_quote, f.created_wall_ns,
                   f.fill_id
            FROM shadow_fills AS f
            JOIN shadow_runs AS r ON r.run_id = f.run_id
            WHERE r.market = ? AND f.created_wall_ns < ?
            ORDER BY f.created_wall_ns DESC, f.fill_id DESC
            LIMIT 1
        ),
        window_fills AS (
            SELECT f.side, f.filled_quote, f.fee_quote, f.created_wall_ns,
                   f.fill_id
            FROM shadow_fills AS f
            JOIN shadow_runs AS r ON r.run_id = f.run_id
            WHERE r.market = ?
              AND f.created_wall_ns >= ?
              AND f.created_wall_ns < ?
        ),
        candidates AS (
            SELECT * FROM prior_fill
            UNION ALL
            SELECT * FROM window_fills
        ),
        ordered AS (
            SELECT *,
                   LAG(side) OVER (
                       ORDER BY created_wall_ns, fill_id
                   ) AS previous_side,
                   LAG(filled_quote) OVER (
                       ORDER BY created_wall_ns, fill_id
                   ) AS buy_quote,
                   LAG(fee_quote) OVER (
                       ORDER BY created_wall_ns, fill_id
                   ) AS buy_fee,
                   LAG(created_wall_ns) OVER (
                       ORDER BY created_wall_ns, fill_id
                   ) AS buy_wall_ns
            FROM candidates
        ),
        pairs AS (
            SELECT created_wall_ns AS closed_wall_ns,
                   (created_wall_ns - buy_wall_ns) / 1000000000.0
                       AS holding_seconds,
                   (filled_quote - fee_quote)
                       - (buy_quote + buy_fee) AS net_pnl_quote
            FROM ordered
            WHERE side = 'sell' AND previous_side = 'buy'
        )
        SELECT COUNT(*) AS completed_trades,
               COALESCE(SUM(net_pnl_quote), 0.0) AS completed_pnl_quote,
               COALESCE(SUM(net_pnl_quote > 0), 0) AS wins,
               COALESCE(SUM(net_pnl_quote < 0), 0) AS losses,
               COALESCE(SUM(net_pnl_quote = 0), 0) AS flats,
               AVG(holding_seconds) AS avg_holding_seconds,
               (SELECT COALESCE(SUM(previous_side = side), 0)
                FROM ordered) AS same_side_transitions,
               (SELECT COALESCE(SUM(side = 'buy'), 0)
                FROM ordered) AS candidate_buys,
               (SELECT COALESCE(SUM(side = 'sell'), 0)
                FROM ordered) AS candidate_sells
        FROM pairs
        WHERE closed_wall_ns >= ? AND closed_wall_ns < ?
        """,
        (market, start_ns, market, start_ns, end_ns, start_ns, end_ns),
    ) or {}
    pairing_reliable = (
        _integer(completed.get("same_side_transitions")) == 0
        and abs(
            _integer(completed.get("candidate_buys"))
            - _integer(completed.get("candidate_sells"))
        )
        <= 1
    )

    events = _one(
        connection,
        """
        SELECT COUNT(*) AS source_events,
               COALESCE(SUM(n.severity = 'warning'), 0) AS warning_events,
               COALESCE(SUM(n.severity = 'critical'), 0) AS critical_events,
               COALESCE(SUM(n.topic = 'shadow_feed_continuity'), 0)
                   AS continuity_events,
               COALESCE(SUM(n.topic = 'shadow_halted'), 0) AS halt_events,
               COALESCE(SUM(n.topic = 'shadow_restart_recovery'), 0)
                   AS restart_events
        FROM notification_outbox AS n
        JOIN shadow_runs AS r ON r.run_id = n.run_id
        WHERE r.market = ?
          AND n.topic <> ?
          AND n.created_wall_ns >= ?
          AND n.created_wall_ns < ?
        """,
        (market, SUMMARY_TOPIC, start_ns, end_ns),
    ) or {}
    run_stats = _one(
        connection,
        """
        SELECT COALESCE(SUM(started_wall_ns >= ? AND started_wall_ns < ?), 0)
                   AS runs_started,
               COALESCE(SUM(ended_wall_ns >= ? AND ended_wall_ns < ?
                            AND halt_reason = 'graceful:runtime_error'), 0)
                   AS runtime_errors
        FROM shadow_runs
        WHERE market = ?
        """,
        (start_ns, end_ns, start_ns, end_ns, market),
    ) or {}

    start_equity = _equity_before(
        connection,
        market=market,
        boundary_wall_ns=start_ns,
    )
    end_equity = _equity_before(
        connection,
        market=market,
        boundary_wall_ns=end_ns,
    )
    current_state = _latest_state(connection, market=market) or {}
    start_value = (
        None if start_equity is None else _number(start_equity["equity_quote"])
    )
    end_value = None if end_equity is None else _number(end_equity["equity_quote"])
    equity_change = (
        None
        if start_value is None or end_value is None
        else end_value - start_value
    )
    equity_return = (
        None
        if equity_change is None or start_value is None or start_value <= 0
        else equity_change / start_value
    )

    summary: dict[str, Any] = {
        "schema_version": SUMMARY_VERSION,
        "market": market,
        "window_start_wall_ns": start_ns,
        "window_end_wall_ns": end_ns,
        "window_start_iso": window.start_local.isoformat(),
        "window_end_iso": window.end_local.isoformat(),
        "window_label": window.label,
        "fill_count": _integer(fills.get("fill_count")),
        "buy_count": _integer(fills.get("buy_count")),
        "sell_count": _integer(fills.get("sell_count")),
        "turnover_quote": _number(fills.get("turnover_quote")),
        "fees_quote": _number(fills.get("fees_quote")),
        "pairing_reliable": pairing_reliable,
        "completed_trades": _integer(completed.get("completed_trades")),
        "completed_pnl_quote": _number(completed.get("completed_pnl_quote")),
        "wins": _integer(completed.get("wins")),
        "losses": _integer(completed.get("losses")),
        "flats": _integer(completed.get("flats")),
        "avg_holding_seconds": (
            None
            if completed.get("avg_holding_seconds") is None
            else _number(completed.get("avg_holding_seconds"))
        ),
        "start_equity_quote": start_value,
        "end_equity_quote": end_value,
        "equity_change_quote": equity_change,
        "equity_return": equity_return,
        "end_cash_quote": (
            None if end_equity is None else _number(end_equity["cash_quote"])
        ),
        "end_base_quantity": (
            None if end_equity is None else _number(end_equity["base_quantity"])
        ),
        "max_drawdown": (
            None if end_equity is None else _number(end_equity["drawdown"])
        ),
        "source_events": _integer(events.get("source_events")),
        "warning_events": _integer(events.get("warning_events")),
        "critical_events": _integer(events.get("critical_events")),
        "continuity_events": _integer(events.get("continuity_events")),
        "halt_events": _integer(events.get("halt_events")),
        "restart_events": _integer(events.get("restart_events")),
        "runs_started": _integer(run_stats.get("runs_started")),
        "runtime_errors": _integer(run_stats.get("runtime_errors")),
        "lifecycle_status": str(current_state.get("lifecycle_status") or "unknown"),
        "halt_reason": current_state.get("halt_reason"),
        "simulated": True,
        "live_order_routing": False,
        "orders_sent": 0,
    }
    if not pairing_reliable:
        for key in (
            "completed_trades",
            "completed_pnl_quote",
            "wins",
            "losses",
            "flats",
            "avg_holding_seconds",
        ):
            summary[key] = None
    return summary


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(
        dict(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _alert_key(market: str, window: SummaryWindow) -> str:
    return (
        f"hourly-summary:v{SUMMARY_VERSION}:{market}:"
        f"{window.start_wall_ns}:{window.end_wall_ns}"
    )


def _notification_id(alert_key: str) -> str:
    digest = hashlib.sha256(alert_key.encode("utf-8")).hexdigest()
    return f"hourly_notice_{digest}"


def _suppress_individual_notifications(
    connection: sqlite3.Connection,
    *,
    now_wall_ns: int,
) -> int:
    cursor = connection.execute(
        """
        UPDATE notification_outbox
        SET status = 'dead_letter',
            available_wall_ns = ?,
            lease_owner = NULL,
            lease_expires_wall_ns = NULL,
            last_error = ?
        WHERE topic <> ? AND status IN ('pending', 'sending')
        """,
        (now_wall_ns, SUPPRESSION_REASON, SUMMARY_TOPIC),
    )
    return int(cursor.rowcount)


def _supersede_stale_summaries(
    connection: sqlite3.Connection,
    *,
    latest_end_wall_ns: int,
) -> int:
    cursor = connection.execute(
        """
        UPDATE notification_outbox
        SET status = 'dead_letter',
            lease_owner = NULL,
            lease_expires_wall_ns = NULL,
            last_error = ?
        WHERE topic = ?
          AND status IN ('pending', 'sending')
          AND created_wall_ns < ?
        """,
        (SUPERSEDED_REASON, SUMMARY_TOPIC, latest_end_wall_ns),
    )
    return int(cursor.rowcount)


def ensure_summary_notification(
    database_path: Path,
    *,
    market: str,
    window: SummaryWindow,
    now_wall_ns: int,
) -> dict[str, int]:
    connection = _connect(database_path)
    try:
        alert_key = _alert_key(market, window)
        existing = connection.execute(
            "SELECT 1 FROM notification_outbox WHERE alert_key = ?",
            (alert_key,),
        ).fetchone()
        has_individual = connection.execute(
            """
            SELECT 1 FROM notification_outbox
            WHERE topic <> ? AND status IN ('pending', 'sending')
            LIMIT 1
            """,
            (SUMMARY_TOPIC,),
        ).fetchone()
        has_stale_summary = connection.execute(
            """
            SELECT 1 FROM notification_outbox
            WHERE topic = ? AND status IN ('pending', 'sending')
              AND created_wall_ns < ?
            LIMIT 1
            """,
            (SUMMARY_TOPIC, window.end_wall_ns),
        ).fetchone()
        payload = (
            None
            if existing is not None
            else aggregate_window(
                connection,
                market=market,
                window=window,
            )
        )
        if (
            existing is not None
            and has_individual is None
            and has_stale_summary is None
        ):
            return {"inserted": 0, "suppressed": 0, "superseded": 0}

        connection.execute("BEGIN IMMEDIATE")
        suppressed = _suppress_individual_notifications(
            connection,
            now_wall_ns=now_wall_ns,
        )
        superseded = _supersede_stale_summaries(
            connection,
            latest_end_wall_ns=window.end_wall_ns,
        )
        existing = connection.execute(
            "SELECT 1 FROM notification_outbox WHERE alert_key = ?",
            (alert_key,),
        ).fetchone()
        inserted = 0
        if existing is None:
            if payload is None:
                raise RuntimeError("summary payload was not prepared")
            notification_id = _notification_id(alert_key)
            connection.execute(
                """
                INSERT INTO notification_outbox (
                    notification_id, run_id, alert_key, topic, severity,
                    payload_json, status, attempt_count, available_wall_ns,
                    lease_owner, lease_expires_wall_ns, created_wall_ns,
                    delivered_wall_ns, last_error
                ) VALUES (
                    ?, NULL, ?, ?, 'info', ?, 'pending', 0, ?,
                    NULL, NULL, ?, NULL, NULL
                )
                """,
                (
                    notification_id,
                    alert_key,
                    SUMMARY_TOPIC,
                    _canonical_json(payload),
                    window.end_wall_ns,
                    window.end_wall_ns,
                ),
            )
            inserted = 1
        connection.commit()
        return {
            "inserted": inserted,
            "suppressed": suppressed,
            "superseded": superseded,
        }
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def claim_summary_notification(
    database_path: Path,
    *,
    worker_id: str,
    now_wall_ns: int,
    lease_seconds: int = 60,
) -> dict[str, Any] | None:
    connection = _connect(database_path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute(
            """
            SELECT notification_id
            FROM notification_outbox
            WHERE topic = ?
              AND available_wall_ns <= ?
              AND (
                  status = 'pending'
                  OR (status = 'sending' AND lease_expires_wall_ns <= ?)
              )
            ORDER BY created_wall_ns DESC
            LIMIT 1
            """,
            (SUMMARY_TOPIC, now_wall_ns, now_wall_ns),
        ).fetchone()
        if row is None:
            connection.commit()
            return None
        notification_id = str(row["notification_id"])
        connection.execute(
            """
            UPDATE notification_outbox
            SET status = 'sending', lease_owner = ?, lease_expires_wall_ns = ?
            WHERE notification_id = ?
            """,
            (
                worker_id,
                now_wall_ns + _positive_int(lease_seconds, "lease_seconds")
                * NANOSECONDS,
                notification_id,
            ),
        )
        claimed = connection.execute(
            "SELECT * FROM notification_outbox WHERE notification_id = ?",
            (notification_id,),
        ).fetchone()
        connection.commit()
        if claimed is None:
            return None
        result = dict(claimed)
        result["payload"] = json.loads(result.pop("payload_json"))
        return result
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def mark_summary_delivered(
    database_path: Path,
    *,
    notification_id: str,
    worker_id: str,
    delivered_wall_ns: int,
) -> None:
    connection = _connect(database_path)
    try:
        cursor = connection.execute(
            """
            UPDATE notification_outbox
            SET status = 'delivered', delivered_wall_ns = ?,
                lease_owner = NULL, lease_expires_wall_ns = NULL,
                last_error = NULL
            WHERE notification_id = ? AND status = 'sending'
              AND lease_owner = ?
            """,
            (delivered_wall_ns, notification_id, worker_id),
        )
        if cursor.rowcount != 1:
            raise RuntimeError("summary notification lease was lost")
    finally:
        connection.close()


def record_summary_failure(
    database_path: Path,
    *,
    notification_id: str,
    worker_id: str,
    now_wall_ns: int,
    retry_at_wall_ns: int,
    error: str,
    max_attempts: int = 10,
) -> None:
    connection = _connect(database_path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute(
            """
            SELECT attempt_count FROM notification_outbox
            WHERE notification_id = ? AND status = 'sending'
              AND lease_owner = ?
            """,
            (notification_id, worker_id),
        ).fetchone()
        if row is None:
            raise RuntimeError("summary notification lease was lost")
        attempts = int(row["attempt_count"]) + 1
        next_status = "dead_letter" if attempts >= max_attempts else "pending"
        connection.execute(
            """
            UPDATE notification_outbox
            SET status = ?, attempt_count = ?, available_wall_ns = ?,
                lease_owner = NULL, lease_expires_wall_ns = NULL,
                last_error = ?
            WHERE notification_id = ?
            """,
            (
                next_status,
                attempts,
                max(now_wall_ns, retry_at_wall_ns),
                str(error)[:160],
                notification_id,
            ),
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def _krw(value: object, *, signed: bool = False) -> str:
    if value is None:
        return "집계 불가"
    numeric = _number(value)
    rounded = int(round(abs(numeric)))
    if numeric < 0:
        return f"-₩{rounded:,}"
    if signed and numeric > 0:
        return f"+₩{rounded:,}"
    return f"₩{rounded:,}"


def _percent(value: object, *, signed: bool = False) -> str:
    if value is None:
        return "집계 불가"
    numeric = _number(value) * 100.0
    prefix = "+" if signed and numeric > 0 else ""
    return f"{prefix}{numeric:.3f}%"


def _status_text(status: object) -> str:
    normalized = str(status or "unknown")
    return {
        "running": "실행 중",
        "warmup": "준비 중",
        "halted_recovery": "거래 중단 · 복구 필요",
        "stopped": "정지",
        "interrupted": "비정상 중단",
    }.get(normalized, normalized)


def _safe_reason(value: object) -> str:
    if value is None:
        return "없음"
    text = " ".join(str(value).split())
    return text[:180] if text else "없음"


def summary_slack_message(summary: Mapping[str, Any]) -> dict[str, Any]:
    market = _required_text(summary.get("market"), "summary.market")
    base_symbol = market.split("-", 1)[-1]
    fill_count = _integer(summary.get("fill_count"))
    completed = summary.get("completed_trades")
    wins = summary.get("wins")
    losses = summary.get("losses")
    flats = summary.get("flats")
    status = _status_text(summary.get("lifecycle_status"))
    equity_change = summary.get("equity_change_quote")
    equity_return = summary.get("equity_return")
    window_label = _required_text(summary.get("window_label"), "window_label")
    critical = _integer(summary.get("critical_events"))
    warnings = _integer(summary.get("warning_events"))
    runtime_errors = _integer(summary.get("runtime_errors"))
    continuity = _integer(summary.get("continuity_events"))
    halts = _integer(summary.get("halt_events"))
    restarts = _integer(summary.get("restart_events"))
    has_operational_issue = any(
        (critical, warnings, runtime_errors, continuity, halts, restarts)
    ) or str(summary.get("lifecycle_status")) != "running"
    movement_icon = (
        "⚪"
        if equity_change is None or abs(_number(equity_change)) < 0.5
        else ("🟢" if _number(equity_change) > 0 else "🔴")
    )
    header_icon = "⚠️" if has_operational_issue else "🧪"
    position = summary.get("end_base_quantity")
    position_text = (
        "없음"
        if position is not None and abs(_number(position)) <= 1e-12
        else (
            "집계 불가"
            if position is None
            else f"{_number(position):.8f} {base_symbol}"
        )
    )
    completed_text = (
        "pairing 검증 실패"
        if completed is None
        else (
            f"{_integer(completed)}회 · 승 {_integer(wins)} / "
            f"패 {_integer(losses)} / 보합 {_integer(flats)}"
        )
    )
    holding = summary.get("avg_holding_seconds")
    holding_text = (
        "-" if holding is None else f"{_number(holding):.1f}초"
    )
    trade_line = (
        "체결 없음"
        if fill_count == 0
        else (
            f"{fill_count:,}건 · 매수 {_integer(summary.get('buy_count')):,} / "
            f"매도 {_integer(summary.get('sell_count')):,}"
        )
    )
    halt_reason = _safe_reason(summary.get("halt_reason"))
    operational_text = (
        f"{header_icon} *운영 상태*\n"
        f"• 현재: *{status}* · 사유 `{halt_reason}`\n"
        f"• critical {critical:,} · warning {warnings:,} · "
        f"runtime error {runtime_errors:,}\n"
        f"• 재시작 {restarts:,} · halt {halts:,} · continuity {continuity:,}"
        if has_operational_issue
        else "✅ *운영 상태*\n• 실행 중 · 경고/중단 이벤트 없음"
    )
    fallback = (
        f"[CoinPilot Shadow][{market}] {window_label} | "
        f"계좌변동 {_krw(equity_change, signed=True)} "
        f"({_percent(equity_return, signed=True)}) | {trade_line} | "
        f"수수료 {_krw(summary.get('fees_quote'))} | "
        f"상태 {status} | simulated=true orders_sent=0"
    )
    blocks: list[dict[str, Any]] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{header_icon} {market} · 1시간 Shadow 요약",
                "emoji": True,
            },
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"🕐 {window_label}  ·  모의체결 전용  ·  실주문 0건",
                }
            ],
        },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": (
                        f"{movement_icon} *계좌 변동*\n"
                        f"{_krw(equity_change, signed=True)} "
                        f"({_percent(equity_return, signed=True)})"
                    ),
                },
                {
                    "type": "mrkdwn",
                    "text": (
                        "*마감 평가자산*\n"
                        f"{_krw(summary.get('end_equity_quote'))}"
                    ),
                },
                {
                    "type": "mrkdwn",
                    "text": (
                        "*완료 거래 손익*\n"
                        f"{_krw(summary.get('completed_pnl_quote'), signed=True)}"
                    ),
                },
                {
                    "type": "mrkdwn",
                    "text": f"*마감 포지션*\n{position_text}",
                },
            ],
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    "*거래 요약*\n"
                    f"• 체결: {trade_line}\n"
                    f"• 완료 거래: {completed_text}\n"
                    f"• 평균 보유: {holding_text}\n"
                    f"• 거래대금: {_krw(summary.get('turnover_quote'))}\n"
                    f"• 수수료: {_krw(summary.get('fees_quote'))}"
                ),
            },
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": operational_text},
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": (
                        "🛡️ `simulated=true` · `live_order_routing=false` · "
                        "`orders_sent=0` · 고정 구간 `[start, end)`"
                    ),
                }
            ],
        },
    ]
    return {"text": fallback[:3000], "blocks": blocks}


def run_cycle(
    settings: NotifierSettings,
    client: SlackClient,
    *,
    worker_id: str,
    now_wall_ns: int,
    interval_seconds: int = 3600,
    grace_seconds: int = 15,
) -> dict[str, int]:
    window = latest_completed_window(
        now_wall_ns,
        interval_seconds=interval_seconds,
        grace_seconds=grace_seconds,
    )
    ensured = ensure_summary_notification(
        settings.database_path,
        market=settings.market,
        window=window,
        now_wall_ns=now_wall_ns,
    )
    row = claim_summary_notification(
        settings.database_path,
        worker_id=worker_id,
        now_wall_ns=now_wall_ns,
    )
    delivered = 0
    failed = 0
    if row is not None:
        notification_id = str(row["notification_id"])
        try:
            client.send(summary_slack_message(row["payload"]))
        except SlackDeliveryError as exc:
            failed = 1
            attempts = int(row.get("attempt_count", 0)) + 1
            retry_seconds = (
                exc.retry_after_seconds
                if exc.retry_after_seconds is not None
                else min(300.0, 2.0 ** min(attempts, 8))
            )
            record_summary_failure(
                settings.database_path,
                notification_id=notification_id,
                worker_id=worker_id,
                now_wall_ns=now_wall_ns,
                retry_at_wall_ns=now_wall_ns + int(retry_seconds * NANOSECONDS),
                error=type(exc).__name__,
            )
        else:
            delivered = 1
            mark_summary_delivered(
                settings.database_path,
                notification_id=notification_id,
                worker_id=worker_id,
                delivered_wall_ns=now_wall_ns,
            )
    return {
        "delivered": delivered,
        "failed": failed,
        **ensured,
    }


def _log(event: str, **details: object) -> None:
    print(
        json.dumps(
            {"event": event, **details, "orders_sent": 0},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ),
        flush=True,
    )


def run_notifier(
    settings: NotifierSettings,
    client: SlackClient,
    *,
    interval_seconds: int = 3600,
    grace_seconds: int = 15,
    poll_seconds: float = 2.0,
    once: bool = False,
    stop_requested: Callable[[], bool] | None = None,
    wall_time_ns: Callable[[], int] = time.time_ns,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, int]:
    _positive_int(interval_seconds, "interval_seconds")
    if not math.isfinite(poll_seconds) or poll_seconds <= 0:
        raise ValueError("poll_seconds must be positive")
    worker_id = f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:8]}"
    totals = {
        "delivered": 0,
        "failed": 0,
        "inserted": 0,
        "suppressed": 0,
        "superseded": 0,
    }
    ensure_reporting_indexes(settings.database_path)
    while True:
        if stop_requested is not None and stop_requested():
            break
        result = run_cycle(
            settings,
            client,
            worker_id=worker_id,
            now_wall_ns=wall_time_ns(),
            interval_seconds=interval_seconds,
            grace_seconds=grace_seconds,
        )
        for key in totals:
            totals[key] += result[key]
        if result["delivered"] or result["failed"] or result["suppressed"]:
            _log("hourly_notifier_cycle", market=settings.market, **result)
        if once:
            break
        sleep(poll_seconds)
    return totals


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Send one low-noise CoinPilot shadow summary per completed hour."
    )
    parser.add_argument("--config", required=True)
    parser.add_argument("--summary-seconds", type=int, default=3600)
    parser.add_argument("--grace-seconds", type=int, default=15)
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    parser.add_argument("--once", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    settings = load_settings(args.config)
    if not settings.database_path.is_file():
        raise SystemExit(f"shadow database is missing: {settings.database_path}")
    webhook = read_slack_webhook_from_keychain(
        service=settings.keychain_service,
        account=settings.keychain_account,
    )
    client = SlackWebhookClient(webhook)
    stopped = False

    def request_stop(_signum: int, _frame: FrameType | None) -> None:
        nonlocal stopped
        stopped = True

    previous = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, request_stop)
    try:
        result = run_notifier(
            settings,
            client,
            interval_seconds=args.summary_seconds,
            grace_seconds=args.grace_seconds,
            poll_seconds=args.poll_seconds,
            once=args.once,
            stop_requested=lambda: stopped,
        )
    finally:
        signal.signal(signal.SIGTERM, previous)
    _log("hourly_notifier_stopped", market=settings.market, **result)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, sqlite3.Error) as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
