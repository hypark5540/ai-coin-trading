"""Durable SQLite ledger for public-data-only HFT shadow trading.

The store deliberately has no exchange client, credential fields, or live-order
table.  It persists simulated decisions and visible-depth fills together with a
transactional notification outbox so an alerting outage cannot corrupt the
shadow account.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import sqlite3
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any


SHADOW_SCHEMA_VERSION = 1
ACTIVE_RUN_STATUSES = ("warmup", "running", "halted_recovery")
RECOVERABLE_RUN_STATUSES = (*ACTIVE_RUN_STATUSES, "stopped")
LATCHED_RECOVERY_REASONS = ("restart_fingerprint_changed",)


_LATEST_HEALTH_SQL = """
    WITH latest_time AS (
        SELECT component, MAX(observed_wall_ns) AS observed_wall_ns
        FROM shadow_health
        WHERE run_id = ?
        GROUP BY component
    ),
    latest_row AS (
        SELECT MAX(h.rowid) AS rowid
        FROM latest_time AS latest
        CROSS JOIN shadow_health AS h
        WHERE h.run_id = ?
          AND h.component = latest.component
          AND h.observed_wall_ns = latest.observed_wall_ns
        GROUP BY latest.component
    )
    SELECT h.component, h.status, h.observed_wall_ns, h.details_json
    FROM latest_row
    JOIN shadow_health AS h ON h.rowid = latest_row.rowid
    ORDER BY h.component
"""


class ShadowStoreError(RuntimeError):
    """Base class for durable shadow-ledger failures."""


class ShadowInvariantError(ShadowStoreError):
    """Raised when deterministic data or account invariants are violated."""


def canonical_json(value: Any) -> str:
    """Return stable, strict JSON suitable for hashes and collision checks."""

    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ShadowInvariantError("shadow payload must be strict JSON") from exc


def deterministic_shadow_id(kind: str, *parts: Any) -> str:
    """Build a stable, namespaced ID from immutable causal inputs."""

    if not isinstance(kind, str) or not kind.strip():
        raise ShadowInvariantError("deterministic ID kind must be non-empty")
    digest = hashlib.sha256(
        canonical_json([kind, *parts]).encode("utf-8")
    ).hexdigest()
    safe_kind = "".join(
        character for character in kind.lower() if character.isalnum()
    )[:12]
    return f"{safe_kind}_{digest[:32]}"


def _finite_nonnegative(value: Any, field: str) -> float:
    if isinstance(value, bool):
        raise ShadowInvariantError(f"{field} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ShadowInvariantError(f"{field} must be numeric") from exc
    if not math.isfinite(result) or result < 0:
        raise ShadowInvariantError(f"{field} must be finite and non-negative")
    return result


def _positive_int(value: Any, field: str, *, allow_zero: bool = False) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ShadowInvariantError(f"{field} must be an integer")
    minimum = 0 if allow_zero else 1
    if value < minimum:
        raise ShadowInvariantError(f"{field} must be >= {minimum}")
    return value


def _required_text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ShadowInvariantError(f"{field} must be non-empty")
    return value.strip()


def _redact_error(value: str) -> str:
    """Bound and scrub common webhook/token forms before durable storage."""

    selected = value[:2_000]
    selected = re.sub(
        r"https://hooks\.slack\.com/services/[A-Za-z0-9/_-]+",
        "[REDACTED_SLACK_WEBHOOK]",
        selected,
    )
    selected = re.sub(
        r"\b(?:xox[a-z]-|Bearer\s+)[A-Za-z0-9._-]+",
        "[REDACTED_TOKEN]",
        selected,
        flags=re.IGNORECASE,
    )
    return selected


@dataclass(frozen=True, slots=True)
class ShadowRunStart:
    run_id: str
    lifecycle_status: str
    restarted_from_run_id: str | None
    expired_pending_orders: int
    already_started: bool


class ShadowStore:
    """Single-writer-friendly WAL ledger with read-only status APIs."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.is_symlink():
            raise ShadowInvariantError(
                "shadow SQLite database cannot be a symbolic link"
            )
        try:
            descriptor = os.open(
                self.path,
                os.O_CREAT | os.O_EXCL | os.O_RDWR,
                0o600,
            )
        except FileExistsError:
            pass
        else:
            os.close(descriptor)
        self._secure_files()
        self._initialize()
        self._secure_files()

    def _secure_files(self) -> None:
        for candidate in (
            self.path,
            Path(f"{self.path}-wal"),
            Path(f"{self.path}-shm"),
        ):
            if candidate.exists():
                os.chmod(candidate, 0o600)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA synchronous = FULL")
        connection.execute("PRAGMA busy_timeout = 30000")
        return connection

    @contextmanager
    def write_transaction(self) -> Iterator[sqlite3.Connection]:
        """Open an immediate transaction for one complete shadow event."""

        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()
            self._secure_files()

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS shadow_runs (
                    run_id TEXT PRIMARY KEY,
                    schema_version INTEGER NOT NULL,
                    mode TEXT NOT NULL CHECK (mode = 'shadow'),
                    market TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (
                        status IN (
                            'warmup', 'running', 'halted_recovery',
                            'stopped', 'interrupted'
                        )
                    ),
                    started_wall_ns INTEGER NOT NULL,
                    ended_wall_ns INTEGER,
                    restart_of_run_id TEXT REFERENCES shadow_runs(run_id),
                    config_hash TEXT NOT NULL,
                    config_json TEXT NOT NULL,
                    code_version TEXT NOT NULL,
                    halt_reason TEXT
                );

                CREATE TABLE IF NOT EXISTS shadow_state (
                    run_id TEXT PRIMARY KEY REFERENCES shadow_runs(run_id),
                    revision INTEGER NOT NULL,
                    lifecycle_status TEXT NOT NULL CHECK (
                        lifecycle_status IN (
                            'warmup', 'running', 'halted_recovery', 'stopped'
                        )
                    ),
                    cash_quote REAL NOT NULL CHECK (cash_quote >= 0),
                    base_quantity REAL NOT NULL CHECK (base_quantity >= 0),
                    average_cost_quote REAL NOT NULL CHECK (
                        average_cost_quote >= 0
                    ),
                    realized_pnl_quote REAL NOT NULL,
                    cumulative_fees_quote REAL NOT NULL CHECK (
                        cumulative_fees_quote >= 0
                    ),
                    last_equity_quote REAL NOT NULL CHECK (
                        last_equity_quote >= 0
                    ),
                    peak_equity_quote REAL NOT NULL CHECK (
                        peak_equity_quote >= 0
                    ),
                    max_drawdown REAL NOT NULL CHECK (
                        max_drawdown >= 0 AND max_drawdown <= 1
                    ),
                    warmup_books_seen INTEGER NOT NULL CHECK (
                        warmup_books_seen >= 0
                    ),
                    last_capture_id TEXT,
                    last_connection_id TEXT,
                    last_book_ordinal INTEGER,
                    last_book_monotonic_ns INTEGER,
                    last_book_wall_ns INTEGER,
                    halt_reason TEXT,
                    updated_wall_ns INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS shadow_decisions (
                    decision_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL REFERENCES shadow_runs(run_id),
                    capture_id TEXT NOT NULL,
                    connection_id TEXT NOT NULL,
                    book_ordinal INTEGER NOT NULL,
                    book_monotonic_ns INTEGER NOT NULL,
                    book_wall_ns INTEGER,
                    action TEXT NOT NULL CHECK (
                        action IN ('buy', 'sell', 'hold')
                    ),
                    signal REAL,
                    reason TEXT NOT NULL,
                    policy_version TEXT NOT NULL,
                    features_json TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (
                        status IN ('accepted', 'hold', 'rejected')
                    ),
                    rejection_reason TEXT,
                    created_wall_ns INTEGER NOT NULL,
                    UNIQUE (
                        run_id, capture_id, connection_id, book_ordinal
                    )
                );

                CREATE TABLE IF NOT EXISTS shadow_orders (
                    order_id TEXT PRIMARY KEY,
                    decision_id TEXT NOT NULL UNIQUE
                        REFERENCES shadow_decisions(decision_id),
                    run_id TEXT NOT NULL REFERENCES shadow_runs(run_id),
                    side TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
                    request_kind TEXT NOT NULL CHECK (
                        request_kind IN ('quote_notional', 'base_quantity')
                    ),
                    requested_base REAL,
                    requested_quote REAL,
                    remaining_base REAL NOT NULL CHECK (remaining_base >= 0),
                    remaining_quote REAL NOT NULL CHECK (remaining_quote >= 0),
                    due_monotonic_ns INTEGER NOT NULL,
                    status TEXT NOT NULL CHECK (
                        status IN (
                            'pending', 'filled', 'partially_filled', 'expired'
                        )
                    ),
                    terminal_reason TEXT,
                    created_wall_ns INTEGER NOT NULL,
                    completed_wall_ns INTEGER,
                    CHECK (
                        (request_kind = 'base_quantity'
                            AND requested_base IS NOT NULL
                            AND requested_quote IS NULL)
                        OR
                        (request_kind = 'quote_notional'
                            AND requested_quote IS NOT NULL
                            AND requested_base IS NULL)
                    )
                );

                CREATE UNIQUE INDEX IF NOT EXISTS
                    idx_shadow_one_pending_order_per_run
                    ON shadow_orders(run_id)
                    WHERE status = 'pending';

                CREATE TABLE IF NOT EXISTS shadow_fills (
                    fill_id TEXT PRIMARY KEY,
                    order_id TEXT NOT NULL UNIQUE
                        REFERENCES shadow_orders(order_id),
                    run_id TEXT NOT NULL REFERENCES shadow_runs(run_id),
                    capture_id TEXT NOT NULL,
                    connection_id TEXT NOT NULL,
                    book_ordinal INTEGER NOT NULL,
                    book_monotonic_ns INTEGER NOT NULL,
                    book_wall_ns INTEGER,
                    side TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
                    filled_base REAL NOT NULL CHECK (filled_base > 0),
                    filled_quote REAL NOT NULL CHECK (filled_quote > 0),
                    fee_quote REAL NOT NULL CHECK (fee_quote >= 0),
                    vwap_price REAL NOT NULL CHECK (vwap_price > 0),
                    levels_consumed INTEGER NOT NULL CHECK (
                        levels_consumed > 0
                    ),
                    execution_status TEXT NOT NULL CHECK (
                        execution_status IN ('filled', 'partial')
                    ),
                    assumptions_json TEXT NOT NULL,
                    created_wall_ns INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS shadow_equity (
                    equity_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL REFERENCES shadow_runs(run_id),
                    capture_id TEXT NOT NULL,
                    connection_id TEXT NOT NULL,
                    book_ordinal INTEGER NOT NULL,
                    book_monotonic_ns INTEGER NOT NULL,
                    book_wall_ns INTEGER,
                    cash_quote REAL NOT NULL CHECK (cash_quote >= 0),
                    base_quantity REAL NOT NULL CHECK (base_quantity >= 0),
                    liquidation_bid REAL,
                    liquidation_covered_base REAL NOT NULL CHECK (
                        liquidation_covered_base >= 0
                    ),
                    equity_quote REAL NOT NULL CHECK (equity_quote >= 0),
                    peak_equity_quote REAL NOT NULL CHECK (
                        peak_equity_quote >= 0
                    ),
                    drawdown REAL NOT NULL CHECK (
                        drawdown >= 0 AND drawdown <= 1
                    ),
                    reason TEXT NOT NULL,
                    created_wall_ns INTEGER NOT NULL,
                    UNIQUE (
                        run_id, capture_id, connection_id, book_ordinal, reason
                    )
                );

                CREATE TABLE IF NOT EXISTS notification_outbox (
                    notification_id TEXT PRIMARY KEY,
                    run_id TEXT REFERENCES shadow_runs(run_id),
                    alert_key TEXT NOT NULL UNIQUE,
                    topic TEXT NOT NULL,
                    severity TEXT NOT NULL CHECK (
                        severity IN ('info', 'warning', 'critical')
                    ),
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (
                        status IN (
                            'pending', 'sending', 'delivered', 'dead_letter'
                        )
                    ),
                    attempt_count INTEGER NOT NULL CHECK (attempt_count >= 0),
                    available_wall_ns INTEGER NOT NULL,
                    lease_owner TEXT,
                    lease_expires_wall_ns INTEGER,
                    created_wall_ns INTEGER NOT NULL,
                    delivered_wall_ns INTEGER,
                    last_error TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_notification_outbox_ready
                    ON notification_outbox(
                        status, available_wall_ns, created_wall_ns
                    );
                CREATE INDEX IF NOT EXISTS idx_notification_outbox_run_status
                    ON notification_outbox(run_id, status);

                CREATE TABLE IF NOT EXISTS shadow_health (
                    health_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL REFERENCES shadow_runs(run_id),
                    component TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (
                        status IN ('ok', 'degraded', 'critical')
                    ),
                    observed_wall_ns INTEGER NOT NULL,
                    details_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_shadow_health_run_component
                    ON shadow_health(
                        run_id, component, observed_wall_ns DESC
                    );
                CREATE INDEX IF NOT EXISTS idx_shadow_fills_run_time
                    ON shadow_fills(run_id, created_wall_ns DESC);
                CREATE INDEX IF NOT EXISTS idx_shadow_equity_run_book
                    ON shadow_equity(run_id, book_monotonic_ns);
                """
            )

    @staticmethod
    def insert_exact(
        connection: sqlite3.Connection,
        table: str,
        id_field: str,
        values: Mapping[str, Any],
    ) -> bool:
        """Idempotently insert, rejecting a deterministic-ID data collision."""

        permitted = {
            "shadow_decisions": "decision_id",
            "shadow_orders": "order_id",
            "shadow_fills": "fill_id",
            "shadow_equity": "equity_id",
            "notification_outbox": "notification_id",
            "shadow_health": "health_id",
        }
        if permitted.get(table) != id_field:
            raise ShadowInvariantError("unsupported deterministic insert target")
        columns = tuple(values)
        placeholders = ", ".join("?" for _ in columns)
        cursor = connection.execute(
            f"INSERT OR IGNORE INTO {table} "
            f"({', '.join(columns)}) VALUES ({placeholders})",
            tuple(values[column] for column in columns),
        )
        if cursor.rowcount == 1:
            return True
        row = connection.execute(
            f"SELECT {', '.join(columns)} FROM {table} "
            f"WHERE {id_field} = ?",
            (values[id_field],),
        ).fetchone()
        if row is None:
            raise ShadowInvariantError(
                f"{table} insert was ignored without an existing ID"
            )
        actual = tuple(row[column] for column in columns)
        expected = tuple(values[column] for column in columns)
        if actual != expected:
            raise ShadowInvariantError(
                f"deterministic {id_field} collided with different data"
            )
        return False

    def enqueue_notification(
        self,
        connection: sqlite3.Connection,
        *,
        run_id: str | None,
        alert_key: str,
        topic: str,
        severity: str,
        payload: Mapping[str, Any],
        created_wall_ns: int,
    ) -> str:
        notification_id = deterministic_shadow_id("notice", alert_key)
        immutable = {
            "notification_id": notification_id,
            "run_id": run_id,
            "alert_key": _required_text(alert_key, "alert_key"),
            "topic": _required_text(topic, "topic"),
            "severity": severity,
            "payload_json": canonical_json(dict(payload)),
            "created_wall_ns": created_wall_ns,
        }
        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO notification_outbox (
                notification_id, run_id, alert_key, topic, severity,
                payload_json, status, attempt_count, available_wall_ns,
                lease_owner, lease_expires_wall_ns, created_wall_ns,
                delivered_wall_ns, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, 'pending', 0, ?, NULL, NULL, ?, NULL, NULL)
            """,
            (
                notification_id,
                run_id,
                immutable["alert_key"],
                immutable["topic"],
                immutable["severity"],
                immutable["payload_json"],
                created_wall_ns,
                created_wall_ns,
            ),
        )
        if cursor.rowcount == 0:
            existing = connection.execute(
                """
                SELECT notification_id, run_id, alert_key, topic, severity,
                       payload_json, created_wall_ns
                FROM notification_outbox WHERE notification_id = ?
                """,
                (notification_id,),
            ).fetchone()
            if existing is None or any(
                existing[key] != value for key, value in immutable.items()
            ):
                raise ShadowInvariantError(
                    "deterministic notification ID collided with different data"
                )
        return notification_id

    def start_run(
        self,
        *,
        run_id: str,
        market: str,
        initial_cash_quote: float,
        config: Mapping[str, Any],
        config_hash: str,
        code_version: str,
        started_wall_ns: int,
        position_epsilon: float = 1e-12,
    ) -> ShadowRunStart:
        """Start a run and atomically quarantine state left by a crash.

        Pending orders from an active predecessor are expired.  Cash and
        position are carried forward for accounting continuity, but a nonzero
        recovered position starts in ``halted_recovery``.  A flat restart starts
        in warmup and cannot immediately create a new order.
        """

        run_id = _required_text(run_id, "run_id")
        market = _required_text(market, "market").upper()
        initial_cash = _finite_nonnegative(
            initial_cash_quote, "initial_cash_quote"
        )
        if initial_cash <= 0:
            raise ShadowInvariantError("initial_cash_quote must be positive")
        _positive_int(started_wall_ns, "started_wall_ns")
        config_json = canonical_json(dict(config))
        config_hash = _required_text(config_hash, "config_hash")
        code_version = _required_text(code_version, "code_version")

        with self.write_transaction() as connection:
            existing = connection.execute(
                """
                SELECT run_id, market, config_hash, config_json, code_version,
                       status
                FROM shadow_runs WHERE run_id = ?
                """,
                (run_id,),
            ).fetchone()
            if existing is not None:
                if (
                    existing["market"] != market
                    or existing["config_hash"] != config_hash
                    or existing["config_json"] != config_json
                    or existing["code_version"] != code_version
                ):
                    raise ShadowInvariantError(
                        "run_id already exists with different immutable data"
                    )
                state = connection.execute(
                    """
                    SELECT lifecycle_status FROM shadow_state WHERE run_id = ?
                    """,
                    (run_id,),
                ).fetchone()
                if state is None:
                    raise ShadowInvariantError("existing run has no state")
                return ShadowRunStart(
                    run_id=run_id,
                    lifecycle_status=str(state["lifecycle_status"]),
                    restarted_from_run_id=None,
                    expired_pending_orders=0,
                    already_started=True,
                )

            predecessor = connection.execute(
                f"""
                SELECT r.run_id, r.status AS run_status,
                       r.config_hash AS predecessor_config_hash,
                       r.code_version AS predecessor_code_version,
                       s.*
                FROM shadow_runs AS r
                JOIN shadow_state AS s ON s.run_id = r.run_id
                WHERE r.market = ?
                  AND r.status IN ({
                    ', '.join('?' for _ in RECOVERABLE_RUN_STATUSES)
                  })
                ORDER BY r.started_wall_ns DESC
                LIMIT 1
                """,
                (market, *RECOVERABLE_RUN_STATUSES),
            ).fetchone()

            restart_of: str | None = None
            expired_count = 0
            if predecessor is None:
                cash_quote = initial_cash
                base_quantity = 0.0
                average_cost_quote = 0.0
                realized_pnl_quote = 0.0
                cumulative_fees_quote = 0.0
                last_equity_quote = initial_cash
                peak_equity_quote = initial_cash
                max_drawdown = 0.0
                lifecycle = "warmup"
                halt_reason = None
            else:
                restart_of = str(predecessor["run_id"])
                pending = connection.execute(
                    """
                    SELECT order_id FROM shadow_orders
                    WHERE run_id = ? AND status = 'pending'
                    """,
                    (restart_of,),
                ).fetchall()
                expired_count = len(pending)
                connection.execute(
                    """
                    UPDATE shadow_orders
                    SET status = 'expired',
                        terminal_reason = 'process_restart',
                        completed_wall_ns = ?
                    WHERE run_id = ? AND status = 'pending'
                    """,
                    (started_wall_ns, restart_of),
                )
                if predecessor["run_status"] in ACTIVE_RUN_STATUSES:
                    connection.execute(
                        """
                        UPDATE shadow_runs
                        SET status = 'interrupted',
                            ended_wall_ns = ?,
                            halt_reason = 'process_restart'
                        WHERE run_id = ?
                        """,
                        (started_wall_ns, restart_of),
                    )
                    connection.execute(
                        """
                        UPDATE shadow_state
                        SET lifecycle_status = 'stopped',
                            revision = revision + 1,
                            halt_reason = 'process_restart',
                            updated_wall_ns = ?
                        WHERE run_id = ?
                        """,
                        (started_wall_ns, restart_of),
                    )
                cash_quote = float(predecessor["cash_quote"])
                base_quantity = float(predecessor["base_quantity"])
                average_cost_quote = float(
                    predecessor["average_cost_quote"]
                )
                realized_pnl_quote = float(
                    predecessor["realized_pnl_quote"]
                )
                cumulative_fees_quote = float(
                    predecessor["cumulative_fees_quote"]
                )
                last_equity_quote = float(
                    predecessor["last_equity_quote"]
                )
                peak_equity_quote = float(
                    predecessor["peak_equity_quote"]
                )
                max_drawdown = float(predecessor["max_drawdown"])
                predecessor_halt = predecessor["halt_reason"]
                if (
                    isinstance(predecessor_halt, str)
                    and (
                        predecessor_halt.startswith("external_halt:")
                        or predecessor_halt in LATCHED_RECOVERY_REASONS
                    )
                ):
                    lifecycle = "halted_recovery"
                    halt_reason = predecessor_halt
                elif (
                    predecessor["predecessor_config_hash"] != config_hash
                    or predecessor["predecessor_code_version"] != code_version
                ):
                    lifecycle = "halted_recovery"
                    halt_reason = "restart_fingerprint_changed"
                elif base_quantity > position_epsilon:
                    lifecycle = "halted_recovery"
                    halt_reason = "restart_with_open_position"
                else:
                    lifecycle = "warmup"
                    halt_reason = None

            connection.execute(
                """
                INSERT INTO shadow_runs (
                    run_id, schema_version, mode, market, status,
                    started_wall_ns, ended_wall_ns, restart_of_run_id,
                    config_hash, config_json, code_version, halt_reason
                ) VALUES (?, ?, 'shadow', ?, ?, ?, NULL, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    SHADOW_SCHEMA_VERSION,
                    market,
                    lifecycle,
                    started_wall_ns,
                    restart_of,
                    config_hash,
                    config_json,
                    code_version,
                    halt_reason,
                ),
            )
            connection.execute(
                """
                INSERT INTO shadow_state (
                    run_id, revision, lifecycle_status, cash_quote,
                    base_quantity, average_cost_quote, realized_pnl_quote,
                    cumulative_fees_quote, last_equity_quote,
                    peak_equity_quote, max_drawdown, warmup_books_seen,
                    last_capture_id, last_connection_id, last_book_ordinal,
                    last_book_monotonic_ns, last_book_wall_ns, halt_reason,
                    updated_wall_ns
                ) VALUES (
                    ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0,
                    NULL, NULL, NULL, NULL, NULL, ?, ?
                )
                """,
                (
                    run_id,
                    lifecycle,
                    cash_quote,
                    base_quantity,
                    average_cost_quote,
                    realized_pnl_quote,
                    cumulative_fees_quote,
                    last_equity_quote,
                    peak_equity_quote,
                    max_drawdown,
                    halt_reason,
                    started_wall_ns,
                ),
            )
            self.enqueue_notification(
                connection,
                run_id=run_id,
                alert_key=f"run-started:{run_id}",
                topic="shadow_run_started",
                severity=(
                    "critical" if lifecycle == "halted_recovery" else "info"
                ),
                payload={
                    "run_id": run_id,
                    "market": market,
                    "lifecycle_status": lifecycle,
                    "restart_of_run_id": restart_of,
                    "expired_pending_orders": expired_count,
                    "live_order_routing": False,
                },
                created_wall_ns=started_wall_ns,
            )
            if restart_of is not None:
                self.enqueue_notification(
                    connection,
                    run_id=run_id,
                    alert_key=f"restart-recovery:{run_id}:{restart_of}",
                    topic="shadow_restart_recovery",
                    severity=(
                        "critical"
                        if lifecycle == "halted_recovery"
                        else "warning"
                    ),
                    payload={
                        "run_id": run_id,
                        "previous_run_id": restart_of,
                        "expired_pending_orders": expired_count,
                        "recovered_base_quantity": base_quantity,
                        "lifecycle_status": lifecycle,
                    },
                    created_wall_ns=started_wall_ns,
                )
            return ShadowRunStart(
                run_id=run_id,
                lifecycle_status=lifecycle,
                restarted_from_run_id=restart_of,
                expired_pending_orders=expired_count,
                already_started=False,
            )

    def get_state(
        self,
        run_id: str,
        *,
        connection: sqlite3.Connection | None = None,
    ) -> dict[str, Any]:
        owns_connection = connection is None
        selected = self._connect() if connection is None else connection
        try:
            row = selected.execute(
                "SELECT * FROM shadow_state WHERE run_id = ?",
                (run_id,),
            ).fetchone()
        finally:
            if owns_connection:
                selected.close()
        if row is None:
            raise ShadowInvariantError(f"unknown shadow run {run_id!r}")
        return dict(row)

    def latest_run_id(self, market: str | None = None) -> str | None:
        """Discover the newest run without sharing an in-memory/run-id file."""

        predicate = "" if market is None else "WHERE market = ?"
        parameters: Sequence[Any] = (
            () if market is None else (_required_text(market, "market").upper(),)
        )
        with self._connect() as connection:
            row = connection.execute(
                f"""
                SELECT run_id FROM shadow_runs
                {predicate}
                ORDER BY started_wall_ns DESC, rowid DESC
                LIMIT 1
                """,
                parameters,
            ).fetchone()
        return None if row is None else str(row["run_id"])

    def read_latest_status(
        self, market: str | None = None
    ) -> dict[str, Any] | None:
        run_id = self.latest_run_id(market)
        return None if run_id is None else self.read_status(run_id)

    def read_latest_feed_anchor(self, market: str) -> dict[str, Any] | None:
        """Read only the latest run and wall-clock fields used by watchdogs."""

        selected_market = _required_text(market, "market").upper()
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT r.run_id, r.started_wall_ns, s.last_book_wall_ns
                FROM shadow_runs AS r
                JOIN shadow_state AS s ON s.run_id = r.run_id
                WHERE r.market = ?
                ORDER BY r.started_wall_ns DESC, r.rowid DESC
                LIMIT 1
                """,
                (selected_market,),
            ).fetchone()
        return None if row is None else dict(row)

    @staticmethod
    def _latest_health_rows(
        connection: sqlite3.Connection,
        run_id: str,
    ) -> list[sqlite3.Row]:
        return connection.execute(
            _LATEST_HEALTH_SQL,
            (run_id, run_id),
        ).fetchall()

    def read_status(self, run_id: str) -> dict[str, Any]:
        """Return a bounded status snapshot for a localhost read API."""

        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT r.*, s.*
                FROM shadow_runs AS r
                JOIN shadow_state AS s ON s.run_id = r.run_id
                WHERE r.run_id = ?
                """,
                (run_id,),
            ).fetchone()
            if row is None:
                raise ShadowInvariantError(f"unknown shadow run {run_id!r}")
            counts = connection.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM shadow_decisions
                        WHERE run_id = ?) AS decisions,
                    (SELECT COUNT(*) FROM shadow_orders
                        WHERE run_id = ? AND status = 'pending') AS pending_orders,
                    (SELECT COUNT(*) FROM shadow_fills
                        WHERE run_id = ?) AS fills,
                    (SELECT COUNT(*) FROM notification_outbox
                        WHERE run_id = ?
                          AND status IN ('pending', 'sending')) AS alerts_pending
                """,
                (run_id, run_id, run_id, run_id),
            ).fetchone()
            health_rows = self._latest_health_rows(connection, run_id)
            pending_order = connection.execute(
                """
                SELECT order_id, side, request_kind, requested_base,
                       requested_quote, due_monotonic_ns, created_wall_ns
                FROM shadow_orders
                WHERE run_id = ? AND status = 'pending'
                """,
                (run_id,),
            ).fetchone()
        result = dict(row)
        result["config"] = json.loads(result.pop("config_json"))
        result.update({key: int(counts[key]) for key in counts.keys()})
        result["health"] = [
            {
                "component": health["component"],
                "status": health["status"],
                "observed_wall_ns": health["observed_wall_ns"],
                "details": json.loads(health["details_json"]),
            }
            for health in health_rows
        ]
        result["live_order_routing"] = False
        result["orders_sent"] = 0
        result["position_quantity"] = result["base_quantity"]
        result["last_feed_wall_ns"] = result["last_book_wall_ns"]
        result["pending_order"] = (
            None if pending_order is None else dict(pending_order)
        )
        return result

    def enqueue_alert(
        self,
        *,
        alert_key: str,
        severity: str,
        payload: Mapping[str, Any],
        created_wall_ns: int,
        topic: str = "shadow_alert",
        run_id: str | None = None,
    ) -> str:
        """Idempotently enqueue an operator/watchdog alert."""

        with self.write_transaction() as connection:
            return self.enqueue_notification(
                connection,
                run_id=run_id,
                alert_key=alert_key,
                topic=topic,
                severity=severity,
                payload=payload,
                created_wall_ns=created_wall_ns,
            )

    def update_heartbeat(
        self,
        *,
        run_id: str,
        component: str,
        status: str,
        observed_wall_ns: int,
        details: Mapping[str, Any],
        event_key: str | None = None,
        alert_on_unhealthy: bool = True,
    ) -> str:
        """Thread-safe health write for services and external watchdogs."""

        if status not in ("ok", "degraded", "critical"):
            raise ShadowInvariantError(
                "health status must be ok, degraded, or critical"
            )
        run_id = _required_text(run_id, "run_id")
        component = _required_text(component, "component")
        _positive_int(observed_wall_ns, "observed_wall_ns")
        health_id = deterministic_shadow_id(
            "health",
            run_id,
            component,
            event_key or observed_wall_ns,
        )
        with self.write_transaction() as connection:
            self.insert_exact(
                connection,
                "shadow_health",
                "health_id",
                {
                    "health_id": health_id,
                    "run_id": run_id,
                    "component": component,
                    "status": status,
                    "observed_wall_ns": observed_wall_ns,
                    "details_json": canonical_json(dict(details)),
                },
            )
            if status != "ok" and alert_on_unhealthy:
                self.enqueue_notification(
                    connection,
                    run_id=run_id,
                    alert_key=f"health:{run_id}:{health_id}",
                    topic=f"shadow_health_{component}",
                    severity=(
                        "critical" if status == "critical" else "warning"
                    ),
                    payload={
                        "run_id": run_id,
                        "component": component,
                        "status": status,
                        "details": dict(details),
                    },
                    created_wall_ns=observed_wall_ns,
                )
        return health_id

    def read_health(self, run_id: str) -> list[dict[str, Any]]:
        """Return the latest persisted heartbeat for every component."""

        with self._connect() as connection:
            rows = self._latest_health_rows(connection, run_id)
        return [
            {
                "component": row["component"],
                "status": row["status"],
                "observed_wall_ns": row["observed_wall_ns"],
                "details": json.loads(row["details_json"]),
            }
            for row in rows
        ]

    def stop_run(
        self,
        run_id: str,
        *,
        reason: str,
        ended_wall_ns: int,
    ) -> int:
        """Gracefully stop while preserving account state for the next run."""

        reason = _required_text(reason, "reason")
        _positive_int(ended_wall_ns, "ended_wall_ns")
        with self.write_transaction() as connection:
            state = connection.execute(
                """
                SELECT lifecycle_status, halt_reason
                FROM shadow_state WHERE run_id = ?
                """,
                (run_id,),
            ).fetchone()
            if state is None:
                raise ShadowInvariantError(f"unknown shadow run {run_id!r}")
            pending = connection.execute(
                """
                UPDATE shadow_orders
                SET status = 'expired',
                    terminal_reason = 'graceful_stop',
                    completed_wall_ns = ?
                WHERE run_id = ? AND status = 'pending'
                """,
                (ended_wall_ns, run_id),
            )
            prior_halt_reason = state["halt_reason"]
            preserved_reason = (
                str(prior_halt_reason)
                if isinstance(prior_halt_reason, str)
                and (
                    prior_halt_reason.startswith("external_halt:")
                    or prior_halt_reason in LATCHED_RECOVERY_REASONS
                )
                else f"graceful:{reason}"
            )
            connection.execute(
                """
                UPDATE shadow_runs
                SET status = 'stopped', ended_wall_ns = ?, halt_reason = ?
                WHERE run_id = ?
                """,
                (ended_wall_ns, preserved_reason, run_id),
            )
            connection.execute(
                """
                UPDATE shadow_state
                SET lifecycle_status = 'stopped',
                    revision = revision + 1,
                    halt_reason = ?,
                    updated_wall_ns = ?
                WHERE run_id = ?
                """,
                (preserved_reason, ended_wall_ns, run_id),
            )
            self.enqueue_notification(
                connection,
                run_id=run_id,
                alert_key=f"run-stopped:{run_id}:{ended_wall_ns}",
                topic="shadow_run_stopped",
                severity="info",
                payload={
                    "run_id": run_id,
                    "reason": reason,
                    "expired_pending_orders": int(pending.rowcount),
                },
                created_wall_ns=ended_wall_ns,
            )
            return int(pending.rowcount)

    def recent_fills(
        self, run_id: str, *, limit: int = 100
    ) -> list[dict[str, Any]]:
        _positive_int(limit, "limit")
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM shadow_fills
                WHERE run_id = ?
                ORDER BY created_wall_ns DESC, fill_id DESC
                LIMIT ?
                """,
                (run_id, limit),
            ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["assumptions"] = json.loads(item.pop("assumptions_json"))
            result.append(item)
        return result

    def read_equity(
        self, run_id: str, *, limit: int = 1_000
    ) -> list[dict[str, Any]]:
        _positive_int(limit, "limit")
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT e.*
                FROM shadow_equity AS e
                JOIN (
                    SELECT equity_id
                    FROM shadow_equity
                    WHERE run_id = ?
                    ORDER BY book_monotonic_ns DESC, rowid DESC
                    LIMIT ?
                ) AS selected ON selected.equity_id = e.equity_id
                ORDER BY e.book_monotonic_ns ASC, e.rowid ASC
                """,
                (run_id, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def first_equity_since(
        self,
        *,
        market: str,
        wall_ns: int,
    ) -> float | None:
        """Return the first sampled liquidation equity across restart runs."""

        market = _required_text(market, "market").upper()
        _positive_int(wall_ns, "wall_ns", allow_zero=True)
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT e.equity_quote
                FROM shadow_equity AS e
                JOIN shadow_runs AS r ON r.run_id = e.run_id
                WHERE r.market = ?
                  AND e.book_wall_ns IS NOT NULL
                  AND e.book_wall_ns >= ?
                ORDER BY e.book_wall_ns ASC, e.rowid ASC
                LIMIT 1
                """,
                (market, wall_ns),
            ).fetchone()
        return None if row is None else float(row["equity_quote"])

    def pending_notifications(
        self, *, run_id: str | None = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        _positive_int(limit, "limit")
        predicate = "" if run_id is None else "AND run_id = ?"
        parameters: Sequence[Any] = (
            (limit,) if run_id is None else (run_id, limit)
        )
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM notification_outbox
                WHERE status IN ('pending', 'sending')
                {predicate}
                ORDER BY created_wall_ns ASC
                LIMIT ?
                """,
                parameters,
            ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["payload"] = json.loads(item.pop("payload_json"))
            result.append(item)
        return result

    def claim_notifications(
        self,
        *,
        worker_id: str,
        now_wall_ns: int,
        lease_ns: int,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Lease ready outbox rows so multiple notifiers cannot double-send."""

        worker_id = _required_text(worker_id, "worker_id")
        _positive_int(now_wall_ns, "now_wall_ns")
        _positive_int(lease_ns, "lease_ns")
        _positive_int(limit, "limit")
        with self.write_transaction() as connection:
            rows = connection.execute(
                """
                SELECT notification_id
                FROM notification_outbox
                WHERE available_wall_ns <= ?
                  AND (
                    status = 'pending'
                    OR (
                        status = 'sending'
                        AND lease_expires_wall_ns <= ?
                    )
                  )
                ORDER BY created_wall_ns ASC
                LIMIT ?
                """,
                (now_wall_ns, now_wall_ns, limit),
            ).fetchall()
            ids = [str(row["notification_id"]) for row in rows]
            for notification_id in ids:
                connection.execute(
                    """
                    UPDATE notification_outbox
                    SET status = 'sending',
                        lease_owner = ?,
                        lease_expires_wall_ns = ?
                    WHERE notification_id = ?
                    """,
                    (worker_id, now_wall_ns + lease_ns, notification_id),
                )
            if not ids:
                return []
            placeholders = ", ".join("?" for _ in ids)
            claimed = connection.execute(
                f"""
                SELECT * FROM notification_outbox
                WHERE notification_id IN ({placeholders})
                ORDER BY created_wall_ns ASC
                """,
                ids,
            ).fetchall()
        result: list[dict[str, Any]] = []
        for row in claimed:
            item = dict(row)
            item["payload"] = json.loads(item.pop("payload_json"))
            result.append(item)
        return result

    def mark_notification_delivered(
        self,
        notification_id: str,
        *,
        worker_id: str,
        delivered_wall_ns: int,
    ) -> None:
        with self.write_transaction() as connection:
            cursor = connection.execute(
                """
                UPDATE notification_outbox
                SET status = 'delivered',
                    delivered_wall_ns = ?,
                    lease_owner = NULL,
                    lease_expires_wall_ns = NULL,
                    last_error = NULL
                WHERE notification_id = ?
                  AND status = 'sending'
                  AND lease_owner = ?
                """,
                (
                    delivered_wall_ns,
                    _required_text(notification_id, "notification_id"),
                    _required_text(worker_id, "worker_id"),
                ),
            )
            if cursor.rowcount != 1:
                raise ShadowInvariantError(
                    "notification lease is absent or owned by another worker"
                )

    def record_notification_failure(
        self,
        notification_id: str,
        *,
        worker_id: str,
        now_wall_ns: int,
        retry_at_wall_ns: int,
        error: str,
        max_attempts: int = 10,
    ) -> None:
        _positive_int(max_attempts, "max_attempts")
        with self.write_transaction() as connection:
            row = connection.execute(
                """
                SELECT attempt_count FROM notification_outbox
                WHERE notification_id = ?
                  AND status = 'sending'
                  AND lease_owner = ?
                """,
                (
                    _required_text(notification_id, "notification_id"),
                    _required_text(worker_id, "worker_id"),
                ),
            ).fetchone()
            if row is None:
                raise ShadowInvariantError(
                    "notification lease is absent or owned by another worker"
                )
            attempts = int(row["attempt_count"]) + 1
            status = "dead_letter" if attempts >= max_attempts else "pending"
            connection.execute(
                """
                UPDATE notification_outbox
                SET status = ?,
                    attempt_count = ?,
                    available_wall_ns = ?,
                    lease_owner = NULL,
                    lease_expires_wall_ns = NULL,
                    last_error = ?
                WHERE notification_id = ?
                """,
                (
                    status,
                    attempts,
                    max(now_wall_ns, retry_at_wall_ns),
                    _redact_error(_required_text(error, "error")),
                    notification_id,
                ),
            )

    def table_counts(self, run_id: str) -> dict[str, int]:
        """Small diagnostic helper used by smoke tests and operators."""

        table_names = (
            "shadow_decisions",
            "shadow_orders",
            "shadow_fills",
            "shadow_equity",
            "shadow_health",
            "notification_outbox",
        )
        with self._connect() as connection:
            return {
                table: int(
                    connection.execute(
                        f"SELECT COUNT(*) FROM {table} WHERE run_id = ?",
                        (run_id,),
                    ).fetchone()[0]
                )
                for table in table_names
            }
