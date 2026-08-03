#!/usr/bin/env python3
"""Fail-closed D2 bounded-diagnostic to public-feed-observe transition.

The helper validates one stopped, flat D2 ledger and its online backup before
pointing the instance at a brand-new observe ledger path.  It never copies,
moves, deletes, or writes the old ledger or its backup.  The default is a
read-only dry run; configuration changes require ``--apply``.

The mac-studio orchestration wrapper must unload every LaunchAgent for the
instance before invoking ``--apply``.  This standalone helper proves that the
old shadow writer lock is free, but does not claim to inspect launchd state.
"""

from __future__ import annotations

import argparse
import copy
import decimal
import errno
import fcntl
import hashlib
import json
import math
import os
import re
import sqlite3
import stat
import sys
import tempfile
import tomllib
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence


INSTANCE_PROFILES: dict[str, tuple[str, int]] = {
    "d2-btc": ("KRW-BTC", 8774),
    "d2-eth": ("KRW-ETH", 8775),
    "d2-xrp": ("KRW-XRP", 8776),
    "d2-sol": ("KRW-SOL", 8777),
}

DIAGNOSTIC_MODEL = "diagnostic-bounded-v1"
OBSERVE_MODEL = "observe-public-feed-v1"
VERSION_RE = re.compile(r"[0-9a-f]{12}\Z")
SOURCE_NAME_RE = re.compile(
    r"shadow-diagnostic-bounded-v1-[0-9a-f]{12}\.db\Z"
)
POSITION_EPSILON = 1e-12

REQUIRED_TABLE_COLUMNS: dict[str, set[str]] = {
    "shadow_runs": {
        "run_id",
        "schema_version",
        "mode",
        "market",
        "status",
        "started_wall_ns",
        "ended_wall_ns",
        "restart_of_run_id",
        "config_hash",
        "config_json",
        "code_version",
        "halt_reason",
    },
    "shadow_state": {
        "run_id",
        "revision",
        "lifecycle_status",
        "cash_quote",
        "base_quantity",
        "average_cost_quote",
        "realized_pnl_quote",
        "cumulative_fees_quote",
        "last_equity_quote",
        "peak_equity_quote",
        "max_drawdown",
        "halt_reason",
        "updated_wall_ns",
    },
    "shadow_decisions": {"decision_id", "run_id"},
    "shadow_orders": {"order_id", "run_id", "status"},
    "shadow_fills": {"fill_id", "run_id"},
    "shadow_equity": {"equity_id", "run_id"},
    "shadow_health": {
        "health_id",
        "run_id",
        "component",
        "status",
        "observed_wall_ns",
        "details_json",
    },
    "notification_outbox": {"notification_id", "run_id", "status"},
}


class TransitionError(RuntimeError):
    """The requested transition did not satisfy a fail-closed invariant."""


def _normalized_absolute(path: Path, label: str) -> Path:
    if not path.is_absolute():
        raise TransitionError(f"{label} must be an absolute path: {path}")
    if any(part in {".", ".."} for part in path.parts):
        raise TransitionError(f"{label} must be lexically normalized: {path}")
    normalized = Path(os.path.normpath(os.fspath(path)))
    if normalized != path:
        raise TransitionError(f"{label} must be lexically normalized: {path}")
    return path


def _reject_symlink_components(path: Path, label: str) -> None:
    """Reject both live and dangling symlinks in an absolute path."""

    current = Path(path.anchor)
    for part in path.parts[1:]:
        current = current / part
        try:
            metadata = os.lstat(current)
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(metadata.st_mode):
            raise TransitionError(f"{label} has a symlink component: {current}")


def _require_within(path: Path, root: Path, label: str) -> None:
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise TransitionError(f"{label} escapes {root}: {path}") from exc


def _check_directory(path: Path, label: str, *, owner_only: bool = False) -> None:
    _reject_symlink_components(path, label)
    try:
        metadata = os.stat(path, follow_symlinks=False)
    except FileNotFoundError as exc:
        raise TransitionError(f"{label} is missing: {path}") from exc
    if not stat.S_ISDIR(metadata.st_mode):
        raise TransitionError(f"{label} must be a directory: {path}")
    if metadata.st_uid != os.geteuid():
        raise TransitionError(f"{label} must be owned by the current user: {path}")
    if owner_only and stat.S_IMODE(metadata.st_mode) & 0o077:
        raise TransitionError(f"{label} must be owner-only: {path}")


def _regular_metadata(
    path: Path,
    label: str,
    *,
    exact_mode: int | None = None,
) -> os.stat_result:
    _reject_symlink_components(path, label)
    try:
        metadata = os.stat(path, follow_symlinks=False)
    except FileNotFoundError as exc:
        raise TransitionError(f"{label} is missing: {path}") from exc
    if not stat.S_ISREG(metadata.st_mode):
        raise TransitionError(f"{label} must be a regular file: {path}")
    if metadata.st_nlink != 1:
        raise TransitionError(f"{label} must not be hard-linked: {path}")
    if metadata.st_uid != os.geteuid():
        raise TransitionError(f"{label} must be owned by the current user: {path}")
    if exact_mode is not None and stat.S_IMODE(metadata.st_mode) != exact_mode:
        raise TransitionError(
            f"{label} mode must be {exact_mode:04o}: {path}"
        )
    return metadata


def _read_regular_bytes(
    path: Path,
    label: str,
    *,
    exact_mode: int | None = None,
) -> bytes:
    expected = _regular_metadata(path, label, exact_mode=exact_mode)
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise TransitionError(f"cannot safely open {label}: {path}: {exc}") from exc
    try:
        actual = os.fstat(descriptor)
        if not stat.S_ISREG(actual.st_mode) or actual.st_nlink != 1:
            raise TransitionError(f"{label} changed during validation: {path}")
        if (actual.st_dev, actual.st_ino) != (expected.st_dev, expected.st_ino):
            raise TransitionError(f"{label} changed during validation: {path}")
        chunks: list[bytes] = []
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        final = os.fstat(descriptor)
        if (
            final.st_size != actual.st_size
            or final.st_mtime_ns != actual.st_mtime_ns
            or final.st_ino != actual.st_ino
        ):
            raise TransitionError(f"{label} changed while it was read: {path}")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _sha256_regular(path: Path, label: str) -> str:
    return hashlib.sha256(_read_regular_bytes(path, label)).hexdigest()


def _load_toml(path: Path) -> tuple[bytes, dict[str, Any]]:
    raw = _read_regular_bytes(path, "config.toml", exact_mode=0o600)
    try:
        text = raw.decode("utf-8")
        loaded = tomllib.loads(text)
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise TransitionError(f"config.toml is invalid: {exc}") from exc
    if not isinstance(loaded, dict):
        raise TransitionError("config.toml must contain a TOML object")
    return raw, loaded


def _parse_runtime(path: Path) -> tuple[bytes, dict[str, str]]:
    raw = _read_regular_bytes(path, "runtime.env", exact_mode=0o600)
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise TransitionError("runtime.env must be UTF-8") from exc
    values: dict[str, str] = {}
    for line_number, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        match = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_]*)=(.*)", line)
        if match is None:
            raise TransitionError(
                f"runtime.env has an invalid line at {line_number}"
            )
        key, value = match.groups()
        if key in values:
            raise TransitionError(f"runtime.env has duplicate key: {key}")
        values[key] = value
    return raw, values


def _mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TransitionError(f"{label} must be a TOML table")
    return value


def _decimal_exact(value: object, expected: str, label: str) -> None:
    if isinstance(value, bool):
        raise TransitionError(f"{label} must equal {expected}")
    try:
        parsed = decimal.Decimal(str(value))
    except decimal.InvalidOperation as exc:
        raise TransitionError(f"{label} must equal {expected}") from exc
    if not parsed.is_finite() or parsed != decimal.Decimal(expected):
        raise TransitionError(f"{label} must equal {expected}, found {value!r}")


def _runtime_exact(runtime: Mapping[str, str], key: str, expected: str) -> None:
    actual = runtime.get(key)
    if actual != expected:
        raise TransitionError(
            f"runtime {key} must be {expected!r}, found {actual!r}"
        )


def _validate_profile(
    *,
    home: Path,
    instance: str,
    config: Mapping[str, Any],
    runtime: Mapping[str, str],
) -> tuple[Path, str, int]:
    market, port = INSTANCE_PROFILES[instance]
    data = _mapping(config.get("data"), "[data]")
    risk = _mapping(config.get("risk"), "[risk]")
    shadow = _mapping(config.get("shadow"), "[shadow]")
    operations = _mapping(config.get("operations"), "[operations]")

    if data.get("market") != market:
        raise TransitionError(
            f"data.market must be {market!r} for {instance}, "
            f"found {data.get('market')!r}"
        )
    if shadow.get("mode") != "diagnostic":
        raise TransitionError("shadow.mode must be 'diagnostic'")
    if shadow.get("model_version") != DIAGNOSTIC_MODEL:
        raise TransitionError(
            f"shadow.model_version must be {DIAGNOSTIC_MODEL!r}"
        )
    _decimal_exact(shadow.get("max_daily_loss_pct"), "0.10", "shadow.max_daily_loss_pct")
    _decimal_exact(shadow.get("max_drawdown_pct"), "0.10", "shadow.max_drawdown_pct")
    _decimal_exact(shadow.get("initial_cash"), "5000000", "shadow.initial_cash")
    _decimal_exact(risk.get("initial_cash"), "5000000", "risk.initial_cash")
    _decimal_exact(shadow.get("order_quote"), "25000", "shadow.order_quote")

    database_value = shadow.get("database_path")
    if not isinstance(database_value, str) or not database_value:
        raise TransitionError("shadow.database_path must be a non-empty string")
    database = Path(database_value)
    _normalized_absolute(database, "shadow.database_path")
    if database.parent != home / "data":
        raise TransitionError(
            "shadow.database_path must be directly inside the instance data directory"
        )
    if SOURCE_NAME_RE.fullmatch(database.name) is None:
        raise TransitionError(
            "shadow.database_path must use "
            "shadow-diagnostic-bounded-v1-<12 lowercase hex>.db"
        )
    _reject_symlink_components(database, "shadow.database_path")

    if operations.get("bind_host") != "127.0.0.1":
        raise TransitionError("operations.bind_host must be '127.0.0.1'")
    if operations.get("port") != port:
        raise TransitionError(
            f"operations.port must be {port} for {instance}, "
            f"found {operations.get('port')!r}"
        )
    configured_backup = operations.get("backup_dir")
    if configured_backup != str(home / "backups"):
        raise TransitionError(
            f"operations.backup_dir must be {home / 'backups'}"
        )

    required_runtime = {
        "COINPILOT_BOUNDED_SHADOW": "1",
        "COINPILOT_ENABLE_SHADOW": "1",
        "COINPILOT_ENABLE_PAPER": "0",
        "COINPILOT_ENABLE_NOTIFIER": "0",
        "COINPILOT_ENABLE_WEB": "1",
        "COINPILOT_ENABLE_WATCHDOG": "1",
        "COINPILOT_WEB_HOST": "127.0.0.1",
        "COINPILOT_WEB_PORT": str(port),
        "COINPILOT_SHADOW_COOLDOWN_SECONDS": "3600",
        "COINPILOT_SHADOW_MAX_ROUND_TRIPS_PER_DAY": "24",
        "COINPILOT_SHADOW_RESERVE_FULL_ORDER_LOSS": "1",
        "COINPILOT_SHADOW_EXECUTION_SPREAD_RECHECK": "1",
    }
    for key, expected in required_runtime.items():
        _runtime_exact(runtime, key, expected)
    return database, market, port


def _validate_target_absent(target: Path) -> tuple[Path, ...]:
    lock = target.parent / f".{target.name}.shadow-run.lock"
    candidates = (
        target,
        Path(f"{target}-wal"),
        Path(f"{target}-shm"),
        Path(f"{target}-journal"),
        lock,
    )
    for candidate in candidates:
        _reject_symlink_components(candidate, "target ledger artifact")
        if candidate.exists() or candidate.is_symlink():
            raise TransitionError(
                f"target ledger artifact already exists: {candidate}"
            )
    return candidates


@contextmanager
def _exclusive_old_shadow_lock(database: Path) -> Iterator[Path]:
    lock = database.parent / f".{database.name}.shadow-run.lock"
    _regular_metadata(lock, "old shadow process lock")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(lock, flags)
    except OSError as exc:
        raise TransitionError(
            f"cannot safely open old shadow process lock: {lock}: {exc}"
        ) from exc
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            raise TransitionError(
                f"old shadow process lock is not a private regular file: {lock}"
            )
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno in (errno.EACCES, errno.EAGAIN):
                raise TransitionError(
                    "old shadow process lock is held; the writer is still running"
                ) from exc
            raise TransitionError(f"cannot lock old shadow process lock: {exc}") from exc
        yield lock
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


def _readonly_connection(path: Path, label: str) -> sqlite3.Connection:
    _regular_metadata(path, label)
    try:
        connection = sqlite3.connect(
            f"{path.as_uri()}?mode=ro&immutable=1",
            uri=True,
            timeout=10.0,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        connection.execute("PRAGMA busy_timeout = 10000")
        connection.execute("BEGIN")
        return connection
    except sqlite3.Error as exc:
        raise TransitionError(f"cannot open {label} read-only: {exc}") from exc


def _pragma_ok(connection: sqlite3.Connection, pragma: str, label: str) -> None:
    rows = connection.execute(f"PRAGMA {pragma}").fetchall()
    values = [str(row[0]) for row in rows]
    if values != ["ok"]:
        raise TransitionError(f"{label} PRAGMA {pragma} failed: {values[:5]!r}")


def _validate_schema(connection: sqlite3.Connection, label: str) -> tuple[tuple[Any, ...], ...]:
    table_rows = connection.execute(
        "SELECT name FROM sqlite_schema WHERE type = 'table'"
    ).fetchall()
    table_names = {str(row[0]) for row in table_rows}
    missing_tables = sorted(set(REQUIRED_TABLE_COLUMNS) - table_names)
    if missing_tables:
        raise TransitionError(f"{label} schema is missing tables: {missing_tables}")
    for table, required in REQUIRED_TABLE_COLUMNS.items():
        columns = {
            str(row[1])
            for row in connection.execute(f'PRAGMA table_info("{table}")')
        }
        missing_columns = sorted(required - columns)
        if missing_columns:
            raise TransitionError(
                f"{label} schema table {table} is missing columns: {missing_columns}"
            )
    return tuple(
        tuple(row)
        for row in connection.execute(
            """
            SELECT type, name, tbl_name, sql
            FROM sqlite_schema
            WHERE name NOT LIKE 'sqlite_%'
            ORDER BY type, name, tbl_name
            """
        ).fetchall()
    )


def _logical_sqlite_sha256(connection: sqlite3.Connection) -> str:
    """Hash the complete immutable logical snapshot of a checkpointed DB."""

    digest = hashlib.sha256()
    for statement in connection.iterdump():
        digest.update(statement.encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def _finite(value: object, label: str, *, minimum: float | None = None) -> float:
    if isinstance(value, bool):
        raise TransitionError(f"{label} must be finite numeric data")
    try:
        parsed = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError, OverflowError) as exc:
        raise TransitionError(f"{label} must be finite numeric data") from exc
    if not math.isfinite(parsed) or (minimum is not None and parsed < minimum):
        raise TransitionError(f"{label} is outside its valid range")
    return parsed


def _normal_stop_reason(value: object) -> bool:
    return value is None or (
        isinstance(value, str) and value.startswith("graceful:")
    )


def _terminal_snapshot(
    connection: sqlite3.Connection,
    *,
    market: str,
    label: str,
    enforce_safety: bool,
) -> tuple[dict[str, Any], dict[str, int], dict[str, Any]]:
    row = connection.execute(
        """
        SELECT
            r.run_id, r.schema_version, r.mode AS run_mode,
            r.market, r.status AS run_status, r.started_wall_ns,
            r.ended_wall_ns, r.restart_of_run_id, r.config_hash,
            r.code_version, r.halt_reason AS run_halt_reason,
            s.revision, s.lifecycle_status, s.cash_quote,
            s.base_quantity, s.average_cost_quote, s.realized_pnl_quote,
            s.cumulative_fees_quote, s.last_equity_quote,
            s.peak_equity_quote, s.max_drawdown,
            s.halt_reason AS state_halt_reason, s.updated_wall_ns
        FROM shadow_runs AS r
        JOIN shadow_state AS s USING (run_id)
        ORDER BY r.started_wall_ns DESC, r.run_id DESC
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        raise TransitionError(f"{label} has no terminal shadow run/state")
    terminal = dict(row)

    counts_row = connection.execute(
        """
        SELECT
          (SELECT COUNT(*) FROM shadow_decisions) AS decisions,
          (SELECT COUNT(*) FROM shadow_orders) AS orders,
          (SELECT COUNT(*) FROM shadow_fills) AS fills,
          (SELECT COUNT(*) FROM shadow_orders WHERE status = 'pending') AS pending,
          (SELECT COUNT(*) FROM notification_outbox) AS outbox
        """
    ).fetchone()
    if counts_row is None:
        raise TransitionError(f"{label} count snapshot is unavailable")
    counts = {key: int(counts_row[key]) for key in counts_row.keys()}

    health_row = connection.execute(
        """
        SELECT component, status, observed_wall_ns, details_json
        FROM shadow_health
        WHERE run_id = ? AND component = 'shadow_engine'
        ORDER BY observed_wall_ns DESC, rowid DESC
        LIMIT 1
        """,
        (terminal["run_id"],),
    ).fetchone()
    if health_row is None:
        raise TransitionError(f"{label} latest run has no shadow_engine health row")
    try:
        details = json.loads(str(health_row["details_json"]))
    except (json.JSONDecodeError, TypeError) as exc:
        raise TransitionError(f"{label} latest health details are invalid JSON") from exc
    if not isinstance(details, dict):
        raise TransitionError(f"{label} latest health details must be an object")
    health = {
        "component": str(health_row["component"]),
        "status": str(health_row["status"]),
        "observed_wall_ns": int(health_row["observed_wall_ns"]),
        "live_order_routing": details.get("live_order_routing"),
        "orders_sent": details.get("orders_sent"),
    }
    for optional in ("simulated", "own_execution"):
        if optional in details:
            health[optional] = details[optional]

    if enforce_safety:
        markets = {
            str(item[0])
            for item in connection.execute("SELECT DISTINCT market FROM shadow_runs")
        }
        if markets != {market} or terminal["market"] != market:
            raise TransitionError(
                f"{label} market lineage must contain only {market}: {sorted(markets)}"
            )
        if terminal["schema_version"] != 1 or terminal["run_mode"] != "shadow":
            raise TransitionError(f"{label} latest run has an unsupported schema/mode")
        if terminal["run_status"] != "stopped" or terminal["lifecycle_status"] != "stopped":
            raise TransitionError(
                f"{label} latest run/state must both be stopped, found "
                f"{terminal['run_status']!r}/{terminal['lifecycle_status']!r}"
            )
        if terminal["ended_wall_ns"] is None:
            raise TransitionError(f"{label} stopped run has no ended_wall_ns")
        if terminal["run_halt_reason"] != terminal["state_halt_reason"]:
            raise TransitionError(f"{label} run/state stop reasons do not match")
        if not _normal_stop_reason(terminal["run_halt_reason"]):
            raise TransitionError(
                f"{label} latest run is halted: {terminal['run_halt_reason']!r}"
            )
        bad_halts = connection.execute(
            """
            SELECT COUNT(*)
            FROM shadow_runs AS r
            JOIN shadow_state AS s USING (run_id)
            WHERE (r.halt_reason IS NOT NULL AND r.halt_reason NOT LIKE 'graceful:%')
               OR (s.halt_reason IS NOT NULL AND s.halt_reason NOT LIKE 'graceful:%')
               OR r.status = 'halted_recovery'
               OR s.lifecycle_status = 'halted_recovery'
            """
        ).fetchone()
        if bad_halts is None or int(bad_halts[0]) != 0:
            raise TransitionError(f"{label} lineage contains a latched halt")
        base_quantity = _finite(
            terminal["base_quantity"], "base_quantity", minimum=0.0
        )
        average_cost = _finite(
            terminal["average_cost_quote"], "average_cost_quote", minimum=0.0
        )
        if base_quantity > POSITION_EPSILON or average_cost != 0.0:
            raise TransitionError(
                f"{label} latest state is not flat: base={base_quantity}, "
                f"average_cost={average_cost}"
            )
        if counts["pending"] != 0:
            raise TransitionError(
                f"{label} has {counts['pending']} pending shadow order(s)"
            )
        for field, minimum in (
            ("cash_quote", 0.0),
            ("realized_pnl_quote", None),
            ("cumulative_fees_quote", 0.0),
            ("last_equity_quote", 0.0),
            ("peak_equity_quote", 0.0),
            ("max_drawdown", 0.0),
        ):
            _finite(terminal[field], field, minimum=minimum)
        if float(terminal["max_drawdown"]) > 1.0:
            raise TransitionError(f"{label} max_drawdown exceeds 1")
        if health["live_order_routing"] is not False:
            raise TransitionError(
                f"{label} latest health does not prove live_order_routing=false"
            )
        orders_sent = health["orders_sent"]
        if isinstance(orders_sent, bool) or not isinstance(orders_sent, int) or orders_sent != 0:
            raise TransitionError(
                f"{label} latest health does not prove orders_sent=0"
            )
        if "simulated" in health and health["simulated"] is not True:
            raise TransitionError(f"{label} latest health contradicts simulated=true")
        if "own_execution" in health and health["own_execution"] is not False:
            raise TransitionError(
                f"{label} latest health contradicts own_execution=false"
            )
    return terminal, counts, health


def _inspect_database(
    path: Path,
    *,
    market: str,
    label: str,
    enforce_safety: bool,
) -> dict[str, Any]:
    connection = _readonly_connection(path, label)
    try:
        _pragma_ok(connection, "quick_check", label)
        _pragma_ok(connection, "integrity_check", label)
        foreign_key_rows = connection.execute("PRAGMA foreign_key_check").fetchall()
        if foreign_key_rows:
            raise TransitionError(
                f"{label} has {len(foreign_key_rows)} foreign-key violation(s)"
            )
        schema = _validate_schema(connection, label)
        terminal, counts, health = _terminal_snapshot(
            connection,
            market=market,
            label=label,
            enforce_safety=enforce_safety,
        )
        return {
            "schema": schema,
            "logical_sha256": _logical_sqlite_sha256(connection),
            "terminal": terminal,
            "counts": counts,
            "health": health,
        }
    except sqlite3.Error as exc:
        raise TransitionError(f"{label} SQLite validation failed: {exc}") from exc
    finally:
        connection.close()


def _validate_backup_sidecar(backup: Path, digest: str) -> Path:
    sidecar = backup.with_name(f"{backup.name}.sha256")
    raw = _read_regular_bytes(sidecar, "backup SHA-256 sidecar")
    expected = f"{digest}  {backup.name}\n".encode("ascii")
    if raw != expected:
        raise TransitionError(
            f"backup SHA-256 sidecar must exactly contain the verified digest: {sidecar}"
        )
    return sidecar


def _compare_backup(source: Mapping[str, Any], backup: Mapping[str, Any]) -> None:
    if source["schema"] != backup["schema"]:
        raise TransitionError("backup SQLite schema does not match the source ledger")
    if source["terminal"] != backup["terminal"]:
        raise TransitionError("backup terminal run/state does not match the source ledger")
    if source["counts"] != backup["counts"]:
        raise TransitionError("backup logical row counts do not match the source ledger")
    if source["health"] != backup["health"]:
        raise TransitionError("backup latest health does not match the source ledger")
    if source["logical_sha256"] != backup["logical_sha256"]:
        raise TransitionError(
            "backup logical SQLite snapshot does not match the source ledger"
        )


def _sqlite_sidecar_hashes(database: Path, label: str) -> dict[str, str]:
    hashes: dict[str, str] = {}
    for suffix in ("-wal", "-shm"):
        candidate = Path(f"{database}{suffix}")
        _reject_symlink_components(candidate, f"{label} {suffix} sidecar")
        if candidate.exists() or candidate.is_symlink():
            hashes[suffix[1:]] = _sha256_regular(
                candidate, f"{label} {suffix} sidecar"
            )
    return hashes


def _toml_basic_string(value: str) -> str:
    # JSON strings are valid TOML basic strings for the path characters used by
    # the managed instance layout.
    return json.dumps(value, ensure_ascii=False)


def _replace_toml_string(
    text: str,
    *,
    section: str,
    key: str,
    expected: str,
    replacement: str,
) -> str:
    current_section: str | None = None
    matches = 0
    output: list[str] = []
    section_re = re.compile(r"^\s*\[([^\]]+)\]\s*(?:#.*)?(?:\r?\n)?$")
    value_re = re.compile(
        rf'^(?P<prefix>\s*{re.escape(key)}\s*=\s*)'
        r'(?P<value>"(?:[^"\\]|\\.)*")'
        r'(?P<suffix>\s*(?:#.*)?(?:\r?\n)?)$'
    )
    for line in text.splitlines(keepends=True):
        section_match = section_re.fullmatch(line)
        if section_match is not None:
            current_section = section_match.group(1).strip()
        if current_section == section:
            value_match = value_re.fullmatch(line)
            if value_match is not None:
                try:
                    parsed = tomllib.loads(f"value = {value_match.group('value')}\n")[
                        "value"
                    ]
                except tomllib.TOMLDecodeError as exc:
                    raise TransitionError(
                        f"cannot safely rewrite {section}.{key}"
                    ) from exc
                if parsed != expected:
                    raise TransitionError(
                        f"{section}.{key} changed during rewrite: {parsed!r}"
                    )
                line = (
                    value_match.group("prefix")
                    + _toml_basic_string(replacement)
                    + value_match.group("suffix")
                )
                matches += 1
        output.append(line)
    if matches != 1:
        raise TransitionError(
            f"config.toml must contain exactly one simple {section}.{key} assignment"
        )
    return "".join(output)


def _render_config(
    original: bytes,
    parsed: Mapping[str, Any],
    *,
    source: Path,
    target: Path,
) -> bytes:
    try:
        text = original.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise TransitionError("config.toml must be UTF-8") from exc
    text = _replace_toml_string(
        text,
        section="shadow",
        key="mode",
        expected="diagnostic",
        replacement="observe",
    )
    text = _replace_toml_string(
        text,
        section="shadow",
        key="model_version",
        expected=DIAGNOSTIC_MODEL,
        replacement=OBSERVE_MODEL,
    )
    text = _replace_toml_string(
        text,
        section="shadow",
        key="database_path",
        expected=str(source),
        replacement=str(target),
    )
    encoded = text.encode("utf-8")
    try:
        updated = tomllib.loads(text)
    except tomllib.TOMLDecodeError as exc:
        raise TransitionError("rewritten config.toml is invalid") from exc
    expected = copy.deepcopy(dict(parsed))
    expected_shadow = expected.get("shadow")
    if not isinstance(expected_shadow, dict):
        raise TransitionError("[shadow] cannot be rewritten safely")
    expected_shadow["mode"] = "observe"
    expected_shadow["model_version"] = OBSERVE_MODEL
    expected_shadow["database_path"] = str(target)
    if updated != expected:
        raise TransitionError("config rewrite changed fields outside the transition")
    return encoded


def _replace_runtime_value(
    text: str,
    *,
    key: str,
    expected: str,
    replacement: str,
) -> str:
    matches = 0
    output: list[str] = []
    pattern = re.compile(
        rf"^(?P<prefix>{re.escape(key)}=)(?P<value>[^\r\n]*)(?P<ending>\r?\n)?$"
    )
    for line in text.splitlines(keepends=True):
        match = pattern.fullmatch(line)
        if match is not None:
            if match.group("value") != expected:
                raise TransitionError(
                    f"runtime {key} changed during rewrite: {match.group('value')!r}"
                )
            line = (
                match.group("prefix")
                + replacement
                + (match.group("ending") or "")
            )
            matches += 1
        output.append(line)
    if matches != 1:
        raise TransitionError(
            f"runtime.env must contain exactly one {key} assignment"
        )
    return "".join(output)


def _render_runtime(original: bytes, parsed: Mapping[str, str]) -> bytes:
    try:
        text = original.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise TransitionError("runtime.env must be UTF-8") from exc
    text = _replace_runtime_value(
        text,
        key="COINPILOT_BOUNDED_SHADOW",
        expected="1",
        replacement="0",
    )
    encoded = text.encode("utf-8")
    values: dict[str, str] = {}
    for line in text.splitlines():
        if not line or line.lstrip().startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if not separator or key in values:
            raise TransitionError("rewritten runtime.env is invalid")
        values[key] = value
    expected_values = dict(parsed)
    expected_values["COINPILOT_BOUNDED_SHADOW"] = "0"
    if values != expected_values:
        raise TransitionError("runtime rewrite changed fields outside the transition")
    return encoded


def _split_terminal(
    terminal: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    run_fields = (
        "run_id",
        "schema_version",
        "run_mode",
        "market",
        "run_status",
        "started_wall_ns",
        "ended_wall_ns",
        "restart_of_run_id",
        "config_hash",
        "code_version",
        "run_halt_reason",
    )
    state_fields = tuple(key for key in terminal if key not in run_fields)
    return (
        {key: terminal[key] for key in run_fields},
        {key: terminal[key] for key in state_fields},
    )


def _receipt_document(
    *,
    instance: str,
    market: str,
    port: int,
    version: str,
    source: Path,
    source_sha256: str,
    source_sidecars: Mapping[str, str],
    source_logical_sha256: str,
    backup: Path,
    backup_sha256: str,
    backup_logical_sha256: str,
    sidecar: Path,
    target: Path,
    terminal: Mapping[str, Any],
    counts: Mapping[str, int],
    health: Mapping[str, Any],
    config_before: bytes,
    runtime_before: bytes,
) -> dict[str, Any]:
    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )
    final_run, final_state = _split_terminal(terminal)
    return {
        "schema_version": 1,
        "transition": "d2-diagnostic-to-public-feed-observe",
        "instance": instance,
        "market": market,
        "version": version,
        "timestamp_utc": timestamp,
        "source": {
            "database_path": str(source),
            "database_sha256": source_sha256,
            "database_sha256_scope": (
                "complete stopped SQLite main file; WAL/SHM absence verified"
            ),
            "sqlite_sidecar_sha256": dict(source_sidecars),
            "logical_snapshot_sha256": source_logical_sha256,
            "logical_snapshot_scope": (
                "complete immutable read-only SQLite main-file snapshot; "
                "WAL/SHM absence verified"
            ),
            "backup_path": str(backup),
            "backup_sha256": backup_sha256,
            "backup_logical_snapshot_sha256": backup_logical_sha256,
            "backup_sidecar_path": str(sidecar),
            "config_sha256": hashlib.sha256(config_before).hexdigest(),
            "runtime_sha256": hashlib.sha256(runtime_before).hexdigest(),
        },
        "final": {
            "run": final_run,
            "state": final_state,
            "counts": dict(counts),
            "health": dict(health),
            "pnl": {
                "realized_pnl_quote": terminal["realized_pnl_quote"],
                "cumulative_fees_quote": terminal["cumulative_fees_quote"],
                "last_equity_quote": terminal["last_equity_quote"],
            },
        },
        "target": {
            "database_path": str(target),
            "profile": {
                "shadow_mode": "observe",
                "model_version": OBSERVE_MODEL,
                "bounded_shadow": 0,
                "initial_cash_quote": 5_000_000,
                "order_quote": 25_000,
                "bind_host": "127.0.0.1",
                "port": port,
            },
        },
        "safety": {
            "simulated": True,
            "own_execution": False,
            "public_feed_only": True,
            "live_order_routing": False,
            "orders_sent": 0,
            "source_writer_lock_acquired": True,
            "source_wal_shm_absent": True,
            "backup_wal_shm_absent": True,
            "source_ledger_preserved": True,
            "backup_verified": True,
            "flat": True,
            "pending_orders": 0,
            "latched_halt": False,
            "target_preexisting": False,
        },
        "orchestration": {
            "all_instance_launchagents_unloaded": {
                "required_before_apply": True,
                "verified_by_helper": False,
                "enforced_by": "scripts/mac-studio wrapper",
            },
            "old_shadow_writer_stopped": {
                "verified_by_helper": True,
                "evidence": "stopped ledger state and nonblocking exclusive process lock",
            },
        },
    }


def _stage_file(path: Path, content: bytes) -> Path:
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        return temporary
    except BaseException:
        try:
            os.close(descriptor)
        except OSError:
            pass
        temporary.unlink(missing_ok=True)
        raise


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_restore(path: Path, content: bytes) -> None:
    temporary = _stage_file(path, content)
    try:
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _apply_transition(
    *,
    config_path: Path,
    runtime_path: Path,
    receipt_path: Path,
    config_before: bytes,
    runtime_before: bytes,
    config_after: bytes,
    runtime_after: bytes,
    receipt: Mapping[str, Any],
    target_artifacts: Sequence[Path],
) -> None:
    if receipt_path.exists() or receipt_path.is_symlink():
        raise TransitionError(f"transition receipt already exists: {receipt_path}")
    for candidate in target_artifacts:
        if candidate.exists() or candidate.is_symlink():
            raise TransitionError(f"target ledger artifact already exists: {candidate}")

    receipt_bytes = (
        json.dumps(
            receipt,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")
    staged_config = _stage_file(config_path, config_after)
    staged_runtime = _stage_file(runtime_path, runtime_after)
    staged_receipt = _stage_file(receipt_path, receipt_bytes)
    config_replaced = False
    runtime_replaced = False
    try:
        if _read_regular_bytes(config_path, "config.toml", exact_mode=0o600) != config_before:
            raise TransitionError("config.toml changed before commit")
        if _read_regular_bytes(runtime_path, "runtime.env", exact_mode=0o600) != runtime_before:
            raise TransitionError("runtime.env changed before commit")
        for candidate in target_artifacts:
            if candidate.exists() or candidate.is_symlink():
                raise TransitionError(f"target ledger artifact appeared: {candidate}")
        if receipt_path.exists() or receipt_path.is_symlink():
            raise TransitionError(f"transition receipt appeared: {receipt_path}")

        # Config first is intentionally fail-closed: if power is lost before
        # runtime.env is replaced, the bounded helper rejects observe mode.
        os.replace(staged_config, config_path)
        config_replaced = True
        _fsync_directory(config_path.parent)
        os.replace(staged_runtime, runtime_path)
        runtime_replaced = True
        _fsync_directory(runtime_path.parent)
        os.replace(staged_receipt, receipt_path)
        _fsync_directory(receipt_path.parent)
    except BaseException:
        # Best-effort rollback covers ordinary write/rename failures.  The
        # config-first ordering remains fail-closed for an abrupt power loss.
        if runtime_replaced:
            _atomic_restore(runtime_path, runtime_before)
        if config_replaced:
            _atomic_restore(config_path, config_before)
        raise
    finally:
        for temporary in (staged_config, staged_runtime, staged_receipt):
            temporary.unlink(missing_ok=True)

    _regular_metadata(config_path, "updated config.toml", exact_mode=0o600)
    _regular_metadata(runtime_path, "updated runtime.env", exact_mode=0o600)
    _regular_metadata(receipt_path, "transition receipt", exact_mode=0o600)
    if _read_regular_bytes(config_path, "updated config.toml") != config_after:
        raise TransitionError("updated config.toml failed byte verification")
    if _read_regular_bytes(runtime_path, "updated runtime.env") != runtime_after:
        raise TransitionError("updated runtime.env failed byte verification")
    stored_receipt = _read_regular_bytes(receipt_path, "transition receipt")
    if stored_receipt != receipt_bytes:
        raise TransitionError("transition receipt failed byte verification")


def execute(
    *,
    home: Path,
    instance: str,
    backup: Path,
    version: str,
    apply: bool,
) -> dict[str, Any]:
    if instance not in INSTANCE_PROFILES:
        raise TransitionError(
            f"instance must be one of: {', '.join(INSTANCE_PROFILES)}"
        )
    if VERSION_RE.fullmatch(version) is None:
        raise TransitionError("version must be exactly 12 lowercase hexadecimal characters")
    home = _normalized_absolute(home, "--home")
    backup = _normalized_absolute(backup, "--backup")
    if home.name != instance:
        raise TransitionError(f"--home basename must exactly match --instance {instance}")
    _check_directory(home, "instance home", owner_only=True)
    for name in ("config", "data", "backups", "state"):
        _check_directory(home / name, f"instance {name} directory", owner_only=True)

    config_path = home / "config" / "config.toml"
    runtime_path = home / "config" / "runtime.env"
    config_before, config = _load_toml(config_path)
    runtime_before, runtime = _parse_runtime(runtime_path)
    source, market, port = _validate_profile(
        home=home,
        instance=instance,
        config=config,
        runtime=runtime,
    )
    _regular_metadata(source, "source shadow database")

    backups_root = home / "backups"
    _require_within(backup, backups_root, "--backup")
    _regular_metadata(backup, "backup database")
    if backup == source:
        raise TransitionError("backup database must be distinct from the source ledger")

    target = home / "data" / f"shadow-observe-public-feed-v1-{version}.db"
    target_artifacts = _validate_target_absent(target)
    receipt_path = home / "state" / f"d2-observe-transition-{version}.json"
    _reject_symlink_components(receipt_path, "transition receipt")
    if receipt_path.exists() or receipt_path.is_symlink():
        raise TransitionError(f"transition receipt already exists: {receipt_path}")

    config_after = _render_config(
        config_before,
        config,
        source=source,
        target=target,
    )
    runtime_after = _render_runtime(runtime_before, runtime)

    with _exclusive_old_shadow_lock(source) as old_lock:
        source_sidecars = _sqlite_sidecar_hashes(
            source, "source shadow database"
        )
        if source_sidecars:
            raise TransitionError(
                "stopped source must be checkpointed with no WAL/SHM sidecars; "
                f"found: {sorted(source_sidecars)}"
            )
        backup_sidecars = _sqlite_sidecar_hashes(backup, "backup database")
        if backup_sidecars:
            raise TransitionError(
                "verified backup must be self-contained with no WAL/SHM sidecars; "
                f"found: {sorted(backup_sidecars)}"
            )
        source_sha256 = _sha256_regular(source, "source shadow database")
        backup_sha256 = _sha256_regular(backup, "backup database")
        sidecar = _validate_backup_sidecar(backup, backup_sha256)

        source_snapshot = _inspect_database(
            source,
            market=market,
            label="source shadow database",
            enforce_safety=True,
        )
        backup_snapshot = _inspect_database(
            backup,
            market=market,
            label="backup database",
            enforce_safety=True,
        )
        _compare_backup(source_snapshot, backup_snapshot)
        # Detect non-cooperating mutation after the SQLite snapshots and before
        # any configuration write.  The service's own writer is excluded by
        # the process lock held across this whole block.
        if _sha256_regular(source, "source shadow database") != source_sha256:
            raise TransitionError("source shadow database changed during validation")
        if _sha256_regular(backup, "backup database") != backup_sha256:
            raise TransitionError("backup database changed during validation")
        if (
            _sqlite_sidecar_hashes(source, "source shadow database")
            != source_sidecars
        ):
            raise TransitionError("source SQLite sidecars changed during validation")
        if _sqlite_sidecar_hashes(backup, "backup database") != backup_sidecars:
            raise TransitionError("backup SQLite sidecars changed during validation")
        _validate_backup_sidecar(backup, backup_sha256)
        _validate_target_absent(target)

        receipt = _receipt_document(
            instance=instance,
            market=market,
            port=port,
            version=version,
            source=source,
            source_sha256=source_sha256,
            source_sidecars=source_sidecars,
            source_logical_sha256=source_snapshot["logical_sha256"],
            backup=backup,
            backup_sha256=backup_sha256,
            backup_logical_sha256=backup_snapshot["logical_sha256"],
            sidecar=sidecar,
            target=target,
            terminal=source_snapshot["terminal"],
            counts=source_snapshot["counts"],
            health=source_snapshot["health"],
            config_before=config_before,
            runtime_before=runtime_before,
        )
        if apply:
            _apply_transition(
                config_path=config_path,
                runtime_path=runtime_path,
                receipt_path=receipt_path,
                config_before=config_before,
                runtime_before=runtime_before,
                config_after=config_after,
                runtime_after=runtime_after,
                receipt=receipt,
                target_artifacts=target_artifacts,
            )

    return {
        "status": "applied" if apply else "dry-run",
        "valid": True,
        "instance": instance,
        "market": market,
        "source_database": str(source),
        "backup_database": str(backup),
        "target_database": str(target),
        "receipt": str(receipt_path),
        "old_shadow_lock": str(old_lock),
        "profile": {
            "shadow_mode": "observe",
            "model_version": OBSERVE_MODEL,
            "bounded_shadow": 0,
        },
        "safety": receipt["safety"],
        "orchestration": receipt["orchestration"],
        "writes": 3 if apply else 0,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Validate and safely stage one stopped D2 diagnostic instance for "
            "a fresh public-feed observe ledger."
        )
    )
    parser.add_argument("--home", required=True, type=Path)
    parser.add_argument("--instance", required=True)
    parser.add_argument("--backup", required=True, type=Path)
    parser.add_argument("--version", required=True)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true", help="write the transition")
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="explicitly select the default read-only validation",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = execute(
            home=args.home,
            instance=args.instance,
            backup=args.backup,
            version=args.version,
            apply=bool(args.apply),
        )
    except (TransitionError, OSError, sqlite3.Error, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        json.dumps(
            result,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
            allow_nan=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
