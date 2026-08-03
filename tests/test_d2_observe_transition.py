from __future__ import annotations

import copy
import fcntl
import hashlib
import json
import os
import sqlite3
import stat
import tomllib
from dataclasses import dataclass
from pathlib import Path

import pytest

from scripts import manage_d2_observe_transition as transition


VERSION = "1234abcdef56"


@dataclass(frozen=True)
class Fixture:
    home: Path
    source: Path
    backup: Path
    config: Path
    runtime: Path
    lock: Path


def _write_private(path: Path, content: str | bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if isinstance(content, str):
        path.write_text(content, encoding="utf-8")
    else:
        path.write_bytes(content)
    path.chmod(0o600)


def _create_database(path: Path) -> None:
    connection = sqlite3.connect(path)
    assert connection.execute("PRAGMA journal_mode = WAL").fetchone()[0] == "wal"
    connection.execute("PRAGMA foreign_keys = ON")
    connection.executescript(
        """
        CREATE TABLE shadow_runs (
            run_id TEXT PRIMARY KEY,
            schema_version INTEGER NOT NULL,
            mode TEXT NOT NULL,
            market TEXT NOT NULL,
            status TEXT NOT NULL,
            started_wall_ns INTEGER NOT NULL,
            ended_wall_ns INTEGER,
            restart_of_run_id TEXT REFERENCES shadow_runs(run_id),
            config_hash TEXT NOT NULL,
            config_json TEXT NOT NULL,
            code_version TEXT NOT NULL,
            halt_reason TEXT
        );
        CREATE TABLE shadow_state (
            run_id TEXT PRIMARY KEY REFERENCES shadow_runs(run_id),
            revision INTEGER NOT NULL,
            lifecycle_status TEXT NOT NULL,
            cash_quote REAL NOT NULL,
            base_quantity REAL NOT NULL,
            average_cost_quote REAL NOT NULL,
            realized_pnl_quote REAL NOT NULL,
            cumulative_fees_quote REAL NOT NULL,
            last_equity_quote REAL NOT NULL,
            peak_equity_quote REAL NOT NULL,
            max_drawdown REAL NOT NULL,
            warmup_books_seen INTEGER NOT NULL,
            last_capture_id TEXT,
            last_connection_id TEXT,
            last_book_ordinal INTEGER,
            last_book_monotonic_ns INTEGER,
            last_book_wall_ns INTEGER,
            halt_reason TEXT,
            updated_wall_ns INTEGER NOT NULL
        );
        CREATE TABLE shadow_decisions (
            decision_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL REFERENCES shadow_runs(run_id)
        );
        CREATE TABLE shadow_orders (
            order_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL REFERENCES shadow_runs(run_id),
            status TEXT NOT NULL
        );
        CREATE TABLE shadow_fills (
            fill_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL REFERENCES shadow_runs(run_id)
        );
        CREATE TABLE shadow_equity (
            equity_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL REFERENCES shadow_runs(run_id)
        );
        CREATE TABLE shadow_health (
            health_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL REFERENCES shadow_runs(run_id),
            component TEXT NOT NULL,
            status TEXT NOT NULL,
            observed_wall_ns INTEGER NOT NULL,
            details_json TEXT NOT NULL
        );
        CREATE TABLE notification_outbox (
            notification_id TEXT PRIMARY KEY,
            run_id TEXT REFERENCES shadow_runs(run_id),
            status TEXT NOT NULL
        );
        """
    )
    connection.execute(
        """
        INSERT INTO shadow_runs VALUES (
          'run-1', 1, 'shadow', 'KRW-BTC', 'stopped',
          1000000000, 2000000000, NULL,
          'config-hash', '{}', 'code-version', 'graceful:sigterm'
        )
        """
    )
    connection.execute(
        """
        INSERT INTO shadow_state VALUES (
          'run-1', 42, 'stopped', 4999000.0, 0.0, 0.0,
          -1000.0, 500.0, 4999000.0, 5000000.0, 0.0002,
          100, 'capture', 'connection', 10, 10, 1900000000,
          'graceful:sigterm', 2000000000
        )
        """
    )
    connection.executemany(
        "INSERT INTO shadow_decisions VALUES (?, 'run-1')",
        [("decision-1",), ("decision-2",)],
    )
    connection.executemany(
        "INSERT INTO shadow_orders VALUES (?, 'run-1', 'filled')",
        [("order-1",), ("order-2",)],
    )
    connection.executemany(
        "INSERT INTO shadow_fills VALUES (?, 'run-1')",
        [("fill-1",), ("fill-2",)],
    )
    connection.execute("INSERT INTO shadow_equity VALUES ('equity-1', 'run-1')")
    connection.execute(
        "INSERT INTO shadow_health VALUES (?, ?, ?, ?, ?, ?)",
        (
            "health-1",
            "run-1",
            "shadow_engine",
            "ok",
            1900000000,
            json.dumps(
                {
                    "simulated": True,
                    "own_execution": False,
                    "live_order_routing": False,
                    "orders_sent": 0,
                },
                sort_keys=True,
            ),
        ),
    )
    connection.executemany(
        "INSERT INTO notification_outbox VALUES (?, 'run-1', ?)",
        [("outbox-1", "delivered"), ("outbox-2", "pending")],
    )
    connection.commit()
    assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
    connection.close()
    path.chmod(0o600)


def _refresh_backup(fixture: Fixture) -> None:
    fixture.backup.unlink(missing_ok=True)
    source = sqlite3.connect(fixture.source)
    backup = sqlite3.connect(fixture.backup)
    try:
        source.backup(backup)
    finally:
        backup.close()
        source.close()
    fixture.backup.chmod(0o600)
    digest = hashlib.sha256(fixture.backup.read_bytes()).hexdigest()
    _write_private(
        fixture.backup.with_name(f"{fixture.backup.name}.sha256"),
        f"{digest}  {fixture.backup.name}\n",
    )


def _fixture(tmp_path: Path) -> Fixture:
    home = tmp_path / "d2-btc"
    for path in (
        home,
        home / "config",
        home / "data",
        home / "backups",
        home / "state",
    ):
        path.mkdir(parents=True, exist_ok=True)
        path.chmod(0o700)
    source = home / "data" / "shadow-diagnostic-bounded-v1-a1b2c3d4e5f6.db"
    backup = home / "backups" / "coinpilot-20260803T010203Z.sqlite"
    config = home / "config" / "config.toml"
    runtime = home / "config" / "runtime.env"
    lock = source.parent / f".{source.name}.shadow-run.lock"
    _create_database(source)
    _write_private(
        config,
        f"""
[data]
market = "KRW-BTC"

[risk]
initial_cash = 5000000
risk_per_trade = 0.0075

[shadow]
# These three fields are the only fields the transition may rewrite.
mode = "diagnostic"
database_path = "{source}"
archive_root = "{home / 'raw'}"
initial_cash = 5000000
order_quote = 25000
fee_rate = 0.0005
max_daily_loss_pct = 0.10
max_drawdown_pct = 0.10
model_version = "diagnostic-bounded-v1"

[operations]
bind_host = "127.0.0.1"
port = 8774
backup_dir = "{home / 'backups'}"
heartbeat_seconds = 10
""".lstrip(),
    )
    _write_private(
        runtime,
        """# Runtime comment and ordering must be preserved.
COINPILOT_WEB_HOST=127.0.0.1
COINPILOT_WEB_PORT=8774
COINPILOT_ENABLE_SHADOW=1
COINPILOT_ENABLE_PAPER=0
COINPILOT_ENABLE_NOTIFIER=0
COINPILOT_ENABLE_WEB=1
COINPILOT_ENABLE_WATCHDOG=1
COINPILOT_BOUNDED_SHADOW=1
COINPILOT_SHADOW_COOLDOWN_SECONDS=3600
COINPILOT_SHADOW_MAX_ROUND_TRIPS_PER_DAY=24
COINPILOT_SHADOW_RESERVE_FULL_ORDER_LOSS=1
COINPILOT_SHADOW_EXECUTION_SPREAD_RECHECK=1
COINPILOT_LOG_MAX_MB=100
""",
    )
    _write_private(lock, b"")
    fixture = Fixture(home, source, backup, config, runtime, lock)
    _refresh_backup(fixture)
    return fixture


def _tree_bytes(root: Path) -> dict[str, tuple[str, bytes | str, int]]:
    result: dict[str, tuple[str, bytes | str, int]] = {}
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root).as_posix()
        metadata = path.lstat()
        mode = stat.S_IMODE(metadata.st_mode)
        if path.is_symlink():
            result[relative] = ("symlink", os.readlink(path), mode)
        elif path.is_file():
            result[relative] = ("file", path.read_bytes(), mode)
        else:
            result[relative] = ("directory", "", mode)
    return result


def _run(fixture: Fixture, *, apply: bool = False) -> dict[str, object]:
    return transition.execute(
        home=fixture.home,
        instance="d2-btc",
        backup=fixture.backup,
        version=VERSION,
        apply=apply,
    )


def test_happy_dry_run_performs_no_writes(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    before = _tree_bytes(fixture.home)

    result = _run(fixture)

    assert result["status"] == "dry-run"
    assert result["valid"] is True
    assert result["writes"] == 0
    assert result["safety"]["live_order_routing"] is False
    assert result["safety"]["orders_sent"] == 0
    assert _tree_bytes(fixture.home) == before


def test_apply_atomically_updates_only_profile_and_writes_receipt(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path)
    old_config_bytes = fixture.config.read_bytes()
    old_runtime_bytes = fixture.runtime.read_bytes()
    old_config = tomllib.loads(old_config_bytes.decode())
    old_source = fixture.source.read_bytes()
    old_backup = fixture.backup.read_bytes()
    config_inode = fixture.config.stat().st_ino
    runtime_inode = fixture.runtime.stat().st_ino

    result = _run(fixture, apply=True)

    assert result["status"] == "applied"
    target = fixture.home / "data" / f"shadow-observe-public-feed-v1-{VERSION}.db"
    receipt_path = fixture.home / "state" / f"d2-observe-transition-{VERSION}.json"
    assert not target.exists()
    assert not Path(f"{target}-wal").exists()
    assert not Path(f"{target}-shm").exists()
    assert fixture.source.read_bytes() == old_source
    assert fixture.backup.read_bytes() == old_backup

    new_config = tomllib.loads(fixture.config.read_text())
    expected_config = copy.deepcopy(old_config)
    expected_config["shadow"]["mode"] = "observe"
    expected_config["shadow"]["model_version"] = "observe-public-feed-v1"
    expected_config["shadow"]["database_path"] = str(target)
    assert new_config == expected_config
    assert fixture.config.stat().st_ino != config_inode
    assert stat.S_IMODE(fixture.config.stat().st_mode) == 0o600
    assert fixture.runtime.stat().st_ino != runtime_inode
    assert stat.S_IMODE(fixture.runtime.stat().st_mode) == 0o600
    assert fixture.runtime.read_bytes() == old_runtime_bytes.replace(
        b"COINPILOT_BOUNDED_SHADOW=1",
        b"COINPILOT_BOUNDED_SHADOW=0",
    )

    receipt = json.loads(receipt_path.read_text())
    assert stat.S_IMODE(receipt_path.stat().st_mode) == 0o600
    assert receipt["source"]["database_sha256"] == hashlib.sha256(
        old_source
    ).hexdigest()
    assert receipt["source"]["backup_sha256"] == hashlib.sha256(
        old_backup
    ).hexdigest()
    assert (
        receipt["source"]["logical_snapshot_sha256"]
        == receipt["source"]["backup_logical_snapshot_sha256"]
    )
    assert "WAL/SHM absence verified" in receipt["source"][
        "database_sha256_scope"
    ]
    assert receipt["final"]["run"]["run_id"] == "run-1"
    assert receipt["final"]["run"]["run_status"] == "stopped"
    assert receipt["final"]["state"]["lifecycle_status"] == "stopped"
    assert receipt["final"]["state"]["base_quantity"] == 0
    assert receipt["final"]["counts"] == {
        "decisions": 2,
        "orders": 2,
        "fills": 2,
        "pending": 0,
        "outbox": 2,
    }
    assert receipt["final"]["pnl"] == {
        "realized_pnl_quote": -1000.0,
        "cumulative_fees_quote": 500.0,
        "last_equity_quote": 4999000.0,
    }
    assert receipt["target"]["database_path"] == str(target)
    assert receipt["target"]["profile"]["bounded_shadow"] == 0
    assert receipt["safety"]["source_ledger_preserved"] is True
    launchd = receipt["orchestration"]["all_instance_launchagents_unloaded"]
    assert launchd == {
        "required_before_apply": True,
        "verified_by_helper": False,
        "enforced_by": "scripts/mac-studio wrapper",
    }


def test_running_state_and_held_writer_lock_fail_closed(tmp_path: Path) -> None:
    running = _fixture(tmp_path / "running")
    connection = sqlite3.connect(running.source)
    connection.execute(
        "UPDATE shadow_runs SET status = 'running', ended_wall_ns = NULL, halt_reason = NULL"
    )
    connection.execute(
        "UPDATE shadow_state SET lifecycle_status = 'running', halt_reason = NULL"
    )
    connection.commit()
    connection.close()
    _refresh_backup(running)
    with pytest.raises(transition.TransitionError, match="must both be stopped"):
        _run(running)

    locked = _fixture(tmp_path / "locked")
    descriptor = os.open(locked.lock, os.O_RDONLY)
    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        with pytest.raises(transition.TransitionError, match="writer is still running"):
            _run(locked)
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (
            "UPDATE shadow_state SET base_quantity = 0.25, average_cost_quote = 100",
            "not flat",
        ),
        (
            "INSERT INTO shadow_orders VALUES ('pending-order', 'run-1', 'pending')",
            "pending shadow order",
        ),
        (
            "UPDATE shadow_runs SET halt_reason = 'external_halt:daily_loss'; "
            "UPDATE shadow_state SET halt_reason = 'external_halt:daily_loss'",
            "halted",
        ),
    ],
)
def test_terminal_nonflat_pending_and_halt_failures(
    tmp_path: Path,
    mutation: str,
    message: str,
) -> None:
    fixture = _fixture(tmp_path)
    connection = sqlite3.connect(fixture.source)
    connection.executescript(mutation)
    connection.commit()
    connection.close()
    _refresh_backup(fixture)

    with pytest.raises(transition.TransitionError, match=message):
        _run(fixture)


@pytest.mark.parametrize(
    ("path_name", "old", "new", "message"),
    [
        ("config", 'mode = "diagnostic"', 'mode = "observe"', "shadow.mode"),
        ("config", "max_drawdown_pct = 0.10", "max_drawdown_pct = 0.11", "max_drawdown"),
        (
            "runtime",
            "COINPILOT_ENABLE_NOTIFIER=0",
            "COINPILOT_ENABLE_NOTIFIER=1",
            "ENABLE_NOTIFIER",
        ),
        (
            "runtime",
            "COINPILOT_WEB_PORT=8774",
            "COINPILOT_WEB_PORT=8775",
            "WEB_PORT",
        ),
    ],
)
def test_profile_drift_fails_closed(
    tmp_path: Path,
    path_name: str,
    old: str,
    new: str,
    message: str,
) -> None:
    fixture = _fixture(tmp_path)
    path = getattr(fixture, path_name)
    _write_private(path, path.read_text().replace(old, new, 1))

    with pytest.raises(transition.TransitionError, match=message):
        _run(fixture)


def test_backup_checksum_and_logical_mismatch_fail_closed(tmp_path: Path) -> None:
    checksum_fixture = _fixture(tmp_path / "checksum")
    sidecar = checksum_fixture.backup.with_name(
        f"{checksum_fixture.backup.name}.sha256"
    )
    _write_private(sidecar, "0" * 64 + f"  {checksum_fixture.backup.name}\n")
    with pytest.raises(transition.TransitionError, match="sidecar"):
        _run(checksum_fixture)

    logical_fixture = _fixture(tmp_path / "logical")
    connection = sqlite3.connect(logical_fixture.backup)
    connection.execute("UPDATE shadow_state SET cash_quote = cash_quote - 1")
    connection.commit()
    connection.close()
    digest = hashlib.sha256(logical_fixture.backup.read_bytes()).hexdigest()
    _write_private(
        logical_fixture.backup.with_name(f"{logical_fixture.backup.name}.sha256"),
        f"{digest}  {logical_fixture.backup.name}\n",
    )
    with pytest.raises(transition.TransitionError, match="terminal run/state"):
        _run(logical_fixture)


def test_symlink_and_hardlink_paths_fail_closed(tmp_path: Path) -> None:
    symlink_fixture = _fixture(tmp_path / "symlink")
    linked_backup = symlink_fixture.home / "backups" / "linked.sqlite"
    linked_backup.symlink_to(symlink_fixture.backup)
    with pytest.raises(transition.TransitionError, match="symlink"):
        transition.execute(
            home=symlink_fixture.home,
            instance="d2-btc",
            backup=linked_backup,
            version=VERSION,
            apply=False,
        )

    hardlink_fixture = _fixture(tmp_path / "hardlink")
    linked_backup = hardlink_fixture.home / "backups" / "linked.sqlite"
    os.link(hardlink_fixture.backup, linked_backup)
    with pytest.raises(transition.TransitionError, match="hard-linked"):
        transition.execute(
            home=hardlink_fixture.home,
            instance="d2-btc",
            backup=linked_backup,
            version=VERSION,
            apply=False,
        )


def test_existing_target_or_sidecar_fails_without_changes(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    target = fixture.home / "data" / f"shadow-observe-public-feed-v1-{VERSION}.db"
    _write_private(Path(f"{target}-wal"), b"reserved")
    before = _tree_bytes(fixture.home)

    with pytest.raises(transition.TransitionError, match="already exists"):
        _run(fixture, apply=True)

    assert _tree_bytes(fixture.home) == before


def test_apply_rolls_back_if_second_atomic_replace_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _fixture(tmp_path)
    old_config = fixture.config.read_bytes()
    old_runtime = fixture.runtime.read_bytes()
    real_replace = transition.os.replace
    failed = False

    def fail_runtime_once(source: Path, destination: Path) -> None:
        nonlocal failed
        if Path(destination) == fixture.runtime and not failed:
            failed = True
            raise OSError("injected runtime replace failure")
        real_replace(source, destination)

    monkeypatch.setattr(transition.os, "replace", fail_runtime_once)
    with pytest.raises(OSError, match="injected runtime"):
        _run(fixture, apply=True)

    assert fixture.config.read_bytes() == old_config
    assert fixture.runtime.read_bytes() == old_runtime
    assert not (
        fixture.home / "state" / f"d2-observe-transition-{VERSION}.json"
    ).exists()
    assert not list(fixture.home.rglob("*.tmp"))
