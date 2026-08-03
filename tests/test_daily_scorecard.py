from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from scripts.coinpilot_daily_scorecard import (
    C2_INSTANCES,
    D2_INSTANCES,
    FillRecord,
    ScorecardError,
    SlackDeliveryError,
    _artifact_paths,
    _period_fill_metrics,
    build_scorecard,
    latest_due_window,
    run_delivery,
    scorecard_markdown,
    select_scheduled_window,
    slack_message,
    window_for_date,
    write_report_artifacts,
)


UTC = timezone.utc


class RecordingClient:
    def __init__(self) -> None:
        self.messages: list[dict[str, object]] = []

    def send(self, message) -> None:
        self.messages.append(dict(message))


class FailingClient:
    def send(self, message) -> None:
        raise SlackDeliveryError("rate limited", retry_after_seconds=1200)


class BlockingClient:
    def __init__(self) -> None:
        self.messages: list[dict[str, object]] = []
        self.started = threading.Event()
        self.release = threading.Event()

    def send(self, message) -> None:
        self.messages.append(dict(message))
        self.started.set()
        if not self.release.wait(timeout=30):
            raise SlackDeliveryError("test delivery release timed out")


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _write_executable(path: Path, text: str) -> None:
    _write(path, text)
    path.chmod(0o755)


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _reporting_paths(root: Path) -> tuple[Path, Path]:
    reporting = root / "reporting"
    return reporting / "state", reporting / "reports"


def _package_manifest(instance_home: Path) -> dict[str, object]:
    root = instance_home / "venv/lib/python3.12/site-packages/coinpilot"
    _write(root / "__init__.py", "__version__ = 'test'\n")
    files = tuple(sorted(root.rglob("*.py")))
    aggregate = hashlib.sha256()
    for path in files:
        aggregate.update(path.relative_to(root).as_posix().encode())
        aggregate.update(b"\0")
        aggregate.update(path.read_bytes())
        aggregate.update(b"\0")
    return {
        "aggregate_sha256": aggregate.hexdigest(),
        "file_count": len(files),
        "scope": "sorted installed coinpilot/**/*.py",
    }


def _d2_database(
    path: Path,
    *,
    market: str,
    start_ns: int,
    end_ns: int,
    generated_ns: int,
    installed_config: dict[str, object],
    config_hash: str,
    code_version: str,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE shadow_runs (
          run_id TEXT PRIMARY KEY, schema_version INTEGER, mode TEXT,
          market TEXT, status TEXT, started_wall_ns INTEGER,
          ended_wall_ns INTEGER, restart_of_run_id TEXT, config_hash TEXT,
          config_json TEXT, code_version TEXT, halt_reason TEXT
        );
        CREATE TABLE shadow_state (
          run_id TEXT PRIMARY KEY, revision INTEGER, lifecycle_status TEXT,
          cash_quote REAL, base_quantity REAL, average_cost_quote REAL,
          realized_pnl_quote REAL, cumulative_fees_quote REAL,
          last_equity_quote REAL, peak_equity_quote REAL, max_drawdown REAL,
          warmup_books_seen INTEGER, last_capture_id TEXT,
          last_connection_id TEXT, last_book_ordinal INTEGER,
          last_book_monotonic_ns INTEGER, last_book_wall_ns INTEGER,
          halt_reason TEXT, updated_wall_ns INTEGER
        );
        CREATE TABLE shadow_decisions (
          decision_id TEXT PRIMARY KEY, run_id TEXT
        );
        CREATE TABLE shadow_orders (
          order_id TEXT PRIMARY KEY, run_id TEXT, status TEXT
        );
        CREATE TABLE shadow_fills (
          fill_id TEXT PRIMARY KEY, run_id TEXT, created_wall_ns INTEGER,
          side TEXT, filled_base REAL, filled_quote REAL, fee_quote REAL
        );
        CREATE TABLE shadow_equity (
          equity_id TEXT PRIMARY KEY, run_id TEXT, equity_quote REAL,
          cash_quote REAL, base_quantity REAL, drawdown REAL,
          created_wall_ns INTEGER
        );
        CREATE TABLE shadow_health (
          health_id TEXT PRIMARY KEY, run_id TEXT, component TEXT,
          status TEXT, observed_wall_ns INTEGER, details_json TEXT
        );
        CREATE TABLE notification_outbox (
          notification_id TEXT PRIMARY KEY, run_id TEXT, topic TEXT,
          severity TEXT, status TEXT, created_wall_ns INTEGER
        );
        """
    )
    connection.execute(
        "INSERT INTO shadow_runs VALUES (?, 1, 'shadow', ?, 'running', ?, NULL, NULL, ?, ?, ?, NULL)",
        (
            "run-1",
            market,
            start_ns - 2 * 3600_000_000_000,
            config_hash,
            _canonical_json(installed_config),
            code_version,
        ),
    )
    pnl = (396.0 - 0.198) - (400.0 + 0.2)
    connection.execute(
        "INSERT INTO shadow_state VALUES (?, 10, 'running', ?, 0, 0, ?, ?, ?, 1000, ?, 100, 'capture', 'connection', 10, 10, ?, NULL, ?)",
        (
            "run-1",
            1000.0 + pnl,
            pnl,
            0.398,
            1000.0 + pnl,
            abs(pnl) / 1000.0,
            generated_ns - 1_000_000_000,
            generated_ns - 1_000_000_000,
        ),
    )
    connection.executemany(
        "INSERT INTO shadow_fills VALUES (?, 'run-1', ?, ?, ?, ?, ?)",
        [
            ("buy", start_ns - 3600_000_000_000, "buy", 4.0, 400.0, 0.2),
            ("sell", start_ns + 3600_000_000_000, "sell", 4.0, 396.0, 0.198),
        ],
    )
    connection.executemany(
        "INSERT INTO shadow_equity VALUES (?, 'run-1', ?, ?, 0, ?, ?)",
        [
            ("eq-start", 1000.0, 1000.0, 0.0, start_ns),
            ("eq-mid", 995.0, 995.0, 0.005, start_ns + 12 * 3600_000_000_000),
            ("eq-end", 1000.0 + pnl, 1000.0 + pnl, abs(pnl) / 1000.0, end_ns - 5_000_000_000),
        ],
    )
    connection.execute(
        "INSERT INTO shadow_health VALUES ('health', 'run-1', 'shadow_engine', 'ok', ?, ?)",
        (
            generated_ns - 1_000_000_000,
            json.dumps(
                {
                    "simulated": True,
                    "own_execution": False,
                    "live_order_routing": False,
                    "orders_sent": 0,
                }
            ),
        ),
    )
    connection.commit()
    connection.close()


def _make_d2(
    root: Path,
    *,
    instance: str,
    market: str,
    start_ns: int,
    end_ns: int,
    generated_ns: int,
) -> None:
    home = root / "instances" / instance
    database = home / "data" / "shadow-diagnostic-bounded-v1-test.db"
    installed_config: dict[str, object] = {
        "market": market,
        "initial_cash_quote": 1000.0,
        "fee_rate": 0.0005,
        "latency_ns": 100_000_000,
        "max_book_gap_ns": 1_000_000_000,
        "warmup_books": 100,
        "max_order_quote": 100.0,
        "equity_sample_interval_ns": 5_000_000_000,
        "health_sample_interval_ns": 10_000_000_000,
        "one_position_only": True,
        "position_epsilon": 1e-12,
        "mode": "shadow",
    }
    config_hash = hashlib.sha256(
        _canonical_json(installed_config).encode("utf-8")
    ).hexdigest()
    service_body = "#!/bin/sh\nexit 0\n"
    bounded_body = "#!/bin/sh\nexit 0\n"
    code_material = ":".join(
        (
            hashlib.sha256(service_body.encode()).hexdigest(),
            hashlib.sha256(bounded_body.encode()).hexdigest(),
            instance,
        )
    )
    code_version = (
        "shadow-deployment-v1:"
        + hashlib.sha256(code_material.encode()).hexdigest()
    )
    installed_identity = {
        "config": installed_config,
        "config_hash": config_hash,
        "code_version": code_version,
    }
    identity_json = _canonical_json(installed_identity)
    _write_executable(
        home / "venv/bin/python",
        "#!/bin/sh\nprintf '%s\\n' '" + identity_json + "'\n",
    )
    _write_executable(home / "bin/coinpilot-service", service_body)
    _write_executable(home / "bin/coinpilot-bounded-shadow", bounded_body)
    _write(
        home / "config/config.toml",
        f"""
[data]
market = "{market}"

[shadow]
mode = "diagnostic"
database_path = "{database}"
model_version = "diagnostic-bounded-v1"
initial_cash = 1000.0
order_quote = 100.0
fee_rate = 0.0005
latency_ms = 100
max_book_gap_ms = 1000
warmup_books = 100
equity_sample_seconds = 5
health_sample_seconds = 10
max_daily_loss_pct = 0.10
max_drawdown_pct = 0.10

[operations]
stale_after_seconds = 60
""".strip()
        + "\n",
    )
    _write(
        home / "config/runtime.env",
        "\n".join(
            [
                "COINPILOT_ENABLE_SHADOW=1",
                "COINPILOT_ENABLE_PAPER=0",
                "COINPILOT_ENABLE_NOTIFIER=0",
                "COINPILOT_BOUNDED_SHADOW=1",
                "COINPILOT_SHADOW_COOLDOWN_SECONDS=3600",
                "COINPILOT_SHADOW_MAX_ROUND_TRIPS_PER_DAY=24",
                "COINPILOT_SHADOW_RESERVE_FULL_ORDER_LOSS=1",
                "COINPILOT_SHADOW_EXECUTION_SPREAD_RECHECK=1",
            ]
        )
        + "\n",
    )
    _d2_database(
        database,
        market=market,
        start_ns=start_ns,
        end_ns=end_ns,
        generated_ns=generated_ns,
        installed_config=installed_config,
        config_hash=config_hash,
        code_version=code_version,
    )


def _set_d2_observe_profile(root: Path, instance: str) -> Path:
    home = root / "instances" / instance
    config_path = home / "config/config.toml"
    runtime_path = home / "config/runtime.env"
    config_text = config_path.read_text(encoding="utf-8")
    config_text = config_text.replace(
        'mode = "diagnostic"',
        'mode = "observe"',
        1,
    ).replace(
        'model_version = "diagnostic-bounded-v1"',
        'model_version = "observe-public-feed-v1"',
        1,
    )
    _write(config_path, config_text)
    _write(
        runtime_path,
        runtime_path.read_text(encoding="utf-8").replace(
            "COINPILOT_BOUNDED_SHADOW=1",
            "COINPILOT_BOUNDED_SHADOW=0",
        ),
    )

    database = home / "data/shadow-diagnostic-bounded-v1-test.db"
    connection = sqlite3.connect(database)
    stored_config = json.loads(
        connection.execute(
            "SELECT config_json FROM shadow_runs WHERE run_id = 'run-1'"
        ).fetchone()[0]
    )
    config_hash = hashlib.sha256(
        _canonical_json(stored_config).encode("utf-8")
    ).hexdigest()
    code_version = "shadow-deployment-v1:observe-without-environment-override"
    connection.execute("DELETE FROM shadow_decisions")
    connection.execute("DELETE FROM shadow_orders")
    connection.execute("DELETE FROM shadow_fills")
    connection.execute(
        """
        UPDATE shadow_state
        SET cash_quote = 1000, base_quantity = 0, average_cost_quote = 0,
            realized_pnl_quote = 0, cumulative_fees_quote = 0,
            last_equity_quote = 1000, peak_equity_quote = 1000,
            max_drawdown = 0
        WHERE run_id = 'run-1'
        """
    )
    connection.execute(
        """
        UPDATE shadow_equity
        SET equity_quote = 1000, cash_quote = 1000,
            base_quantity = 0, drawdown = 0
        WHERE run_id = 'run-1'
        """
    )
    connection.execute(
        """
        UPDATE shadow_runs
        SET config_hash = ?, config_json = ?, code_version = ?
        WHERE run_id = 'run-1'
        """,
        (config_hash, _canonical_json(stored_config), code_version),
    )
    connection.commit()
    connection.close()

    installed_identity = _canonical_json(
        {
            "config": stored_config,
            "config_hash": config_hash,
            "code_version": code_version,
        }
    )
    wrong_identity = _canonical_json(
        {
            "config": stored_config,
            "config_hash": config_hash,
            "code_version": "wrong-bounded-environment-override",
        }
    )
    _write_executable(
        home / "venv/bin/python",
        "#!/bin/sh\n"
        "if [ -n \"${COINPILOT_CODE_VERSION:-}\" ]; then\n"
        f"  printf '%s\\n' '{wrong_identity}'\n"
        "else\n"
        f"  printf '%s\\n' '{installed_identity}'\n"
        "fi\n",
    )
    return database


def _c2_database(
    path: Path,
    *,
    account_key: str,
    market: str,
    fingerprint: str,
    start_ns: int,
    generated_ns: int,
    with_trade: bool,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE candles (
          market TEXT, interval_minutes INTEGER, timestamp TEXT,
          open REAL, high REAL, low REAL, close REAL,
          volume REAL, quote_volume REAL
        );
        CREATE TABLE paper_state (
          account_key TEXT PRIMARY KEY, state_json TEXT, updated_at TEXT
        );
        CREATE TABLE paper_events (
          event_id TEXT PRIMARY KEY, account_key TEXT,
          timestamp TEXT, event_type TEXT, payload_json TEXT
        );
        """
    )
    event_time = datetime.fromtimestamp((start_ns - 7200_000_000_000) / 1e9, UTC).isoformat()
    connection.execute(
        "INSERT INTO paper_events VALUES (?, ?, ?, 'paper_initialized', '{}')",
        (f"{account_key}:initialized", account_key, event_time),
    )
    pnl = 0.0
    fees = 0.0
    if with_trade:
        fills = [
            (
                f"{account_key}:buy",
                datetime.fromtimestamp((start_ns - 3600_000_000_000) / 1e9, UTC).isoformat(),
                {
                    "side": "buy",
                    "quantity": 4.0,
                    "notional": 400.0,
                    "fee": 0.2,
                    "slippage_cost": 0.4,
                    "reason": "model_entry",
                },
            ),
            (
                f"{account_key}:sell",
                datetime.fromtimestamp((start_ns + 3600_000_000_000) / 1e9, UTC).isoformat(),
                {
                    "side": "sell",
                    "quantity": 4.0,
                    "notional": 396.0,
                    "fee": 0.198,
                    "slippage_cost": 0.396,
                    "reason": "model_exit",
                },
            ),
        ]
        for event_id, timestamp, payload in fills:
            connection.execute(
                "INSERT INTO paper_events VALUES (?, ?, ?, 'fill', ?)",
                (event_id, account_key, timestamp, json.dumps(payload)),
            )
        pnl = (396.0 - 0.198) - (400.0 + 0.2)
        fees = 0.398
    updated = datetime.fromtimestamp((generated_ns - 1_000_000_000) / 1e9, UTC).isoformat()
    state = {
        "schema_version": 8,
        "market": market,
        "config_fingerprint": fingerprint,
        "cash": 1000.0 + pnl,
        "quantity": 0.0,
        "realized_pnl": pnl,
        "peak_equity": 1000.0,
        "halt_state": "ACTIVE",
        "revision": 10,
        "updated_at": updated,
        "fees": fees,
    }
    connection.execute(
        "INSERT INTO paper_state VALUES (?, ?, ?)",
        (account_key, json.dumps(state), updated),
    )
    connection.commit()
    connection.close()


def _make_c2(
    root: Path,
    *,
    instance: str,
    market: str,
    start_ns: int,
    generated_ns: int,
    with_trade: bool,
) -> None:
    home = root / "instances" / instance
    database = home / "data/coinpilot-c2.db"
    account_name = f"c2-forward-v1-{market.lower()}"
    account_key = f"paper-v8:{account_name}:{market}:60m"
    fingerprint = f"fingerprint-{instance}"
    config_path = home / "config/paper.toml"
    _write(
        config_path,
        f"""
[data]
market = "{market}"
interval_minutes = 60
api_base_url = "https://api.upbit.com"
database_path = "{database}"

[paper]
account_name = "{account_name}"
poll_seconds = 30

[risk]
initial_cash = 1000.0
fee_rate = 0.0005
slippage_bps = 5.0
""".strip()
        + "\n",
    )
    _write(
        home / "config/runtime.env",
        "\n".join(
            [
                "COINPILOT_ENABLE_SHADOW=0",
                "COINPILOT_ENABLE_PAPER=1",
                "COINPILOT_ENABLE_NOTIFIER=0",
                "COINPILOT_ENABLE_WEB=0",
                "COINPILOT_ENABLE_WATCHDOG=0",
            ]
        )
        + "\n",
    )
    package = _package_manifest(home)
    manifest = {
        "schema_version": 1,
        "installed_package": package,
        "market_config": {
            "paper_account_key": account_key,
            "paper_config_fingerprint": fingerprint,
            "config_sha256": hashlib.sha256(config_path.read_bytes()).hexdigest(),
        },
        "safety": {
            "public_only": True,
            "own_execution": False,
            "live_order_routing": False,
            "orders_sent": 0,
            "credential_fields": [],
        },
    }
    manifest_path = home / "config/paper.manifest.json"
    _write(manifest_path, json.dumps(manifest))
    installed_identity = {
        "valid": True,
        "market": market,
        "paper_account_key": account_key,
        "paper_config_fingerprint": fingerprint,
        "public_only": True,
        "own_execution": False,
        "live_order_routing": False,
        "orders_sent": 0,
    }
    _write_executable(
        home / "venv/bin/python",
        "#!/bin/sh\nexec \"$@\"\n",
    )
    _write_executable(
        home / "bin/coinpilot-c2-config",
        "#!/bin/sh\nprintf '%s\\n' '"
        + _canonical_json(installed_identity)
        + "'\n",
    )
    _c2_database(
        database,
        account_key=account_key,
        market=market,
        fingerprint=fingerprint,
        start_ns=start_ns,
        generated_ns=generated_ns,
        with_trade=with_trade,
    )


@pytest.fixture
def reporting_root(tmp_path: Path) -> tuple[Path, object, int]:
    root = tmp_path / "Coinpilot"
    (root / "instances").mkdir(parents=True)
    window = window_for_date(date(2026, 7, 22))
    generated_ns = window.end_wall_ns + 10 * 60 * 1_000_000_000
    for instance, market in D2_INSTANCES.items():
        _make_d2(
            root,
            instance=instance,
            market=market,
            start_ns=window.start_wall_ns,
            end_ns=window.end_wall_ns,
            generated_ns=generated_ns,
        )
    for index, (instance, market) in enumerate(C2_INSTANCES.items()):
        _make_c2(
            root,
            instance=instance,
            market=market,
            start_ns=window.start_wall_ns,
            generated_ns=generated_ns,
            with_trade=index == 0,
        )
    return root, window, generated_ns


def test_latest_due_window_uses_0010_kst_deadline() -> None:
    before = datetime.fromisoformat("2026-07-23T00:09:59+09:00")
    at_due = datetime.fromisoformat("2026-07-23T00:10:00+09:00")
    assert latest_due_window(int(before.timestamp() * 1e9)).report_date == date(2026, 7, 21)
    assert latest_due_window(int(at_due.timestamp() * 1e9)).report_date == date(2026, 7, 22)


def test_fill_replay_attributes_cross_midnight_trade_to_close_date() -> None:
    window = window_for_date(date(2026, 7, 22))
    fills = [
        FillRecord("buy", window.start_wall_ns - 1, "buy", 4, 400, 0.2),
        FillRecord("sell", window.start_wall_ns + 1, "sell", 4, 396, 0.198),
        FillRecord("next", window.end_wall_ns, "buy", 1, 100, 0.05),
    ]
    metrics = _period_fill_metrics(
        fills,
        start_wall_ns=window.start_wall_ns,
        end_wall_ns=window.end_wall_ns,
    )
    assert metrics["fill_count"] == 1
    assert metrics["completed_trades"] == 1
    assert metrics["realized_pnl_quote"] == pytest.approx(-4.398)
    assert metrics["fees_quote"] == pytest.approx(0.198)


def test_partial_sell_recognizes_accounting_pnl_before_round_trip() -> None:
    window = window_for_date(date(2026, 7, 22))
    fills = [
        FillRecord("buy", window.start_wall_ns - 1, "buy", 4, 400, 0.2),
        FillRecord(
            "partial",
            window.start_wall_ns + 1,
            "sell",
            1.5,
            148.5,
            0.07425,
        ),
        FillRecord(
            "close",
            window.start_wall_ns + 2,
            "sell",
            2.5,
            247.5,
            0.12375,
        ),
    ]

    partial = _period_fill_metrics(
        fills,
        start_wall_ns=window.start_wall_ns,
        end_wall_ns=window.start_wall_ns + 2,
    )
    completed = _period_fill_metrics(
        fills,
        start_wall_ns=window.start_wall_ns,
        end_wall_ns=window.end_wall_ns,
    )

    assert partial["completed_trades"] == 0
    assert partial["accounting_realized_pnl_quote"] == pytest.approx(-1.64925)
    assert partial["completed_round_trip_pnl_quote"] == pytest.approx(0.0)
    assert completed["completed_trades"] == 1
    assert completed["accounting_realized_pnl_quote"] == pytest.approx(-4.398)
    assert completed["completed_round_trip_pnl_quote"] == pytest.approx(-4.398)


def test_d2_equity_uses_boundary_anchor(reporting_root) -> None:
    root, window, generated_ns = reporting_root

    report = build_scorecard(
        root=root,
        window=window,
        generated_wall_ns=generated_ns,
    )

    for row in report["D2"]["instances"]:
        equity = row["equity"]
        assert equity["full_window"] is True
        assert equity["valuation_quality"] == "sampled_shadow_equity_with_boundary_anchor"
        assert equity["start_sample_wall_ns"] == window.start_wall_ns
        assert equity["start_boundary_lag_seconds"] == pytest.approx(0.0)
        assert equity["start_equity_quote"] == pytest.approx(1000.0)
        assert equity["sampled_daily_drawdown"] == pytest.approx(0.005)


def test_build_scorecard_reads_only_allowlisted_ledgers_and_reconciles(
    reporting_root,
) -> None:
    root, window, generated_ns = reporting_root
    databases = tuple((root / "instances").glob("*/data/*.db"))
    before = {
        path: (hashlib.sha256(path.read_bytes()).hexdigest(), path.stat().st_mtime_ns)
        for path in databases
    }

    report = build_scorecard(root=root, window=window, generated_wall_ns=generated_ns)

    after = {
        path: (hashlib.sha256(path.read_bytes()).hexdigest(), path.stat().st_mtime_ns)
        for path in databases
    }
    assert before == after
    assert report["safety"] == {
        "verified": True,
        "simulated": True,
        "own_execution": False,
        "live_order_routing": False,
        "orders_sent": 0,
        "target": {
            "simulated": True,
            "own_execution": False,
            "live_order_routing": False,
            "orders_sent": 0,
        },
    }
    assert [row["instance"] for row in report["D2"]["instances"]] == list(D2_INSTANCES)
    assert [row["instance"] for row in report["C2"]["instances"]] == list(C2_INSTANCES)
    assert report["D2"]["summary"]["daily"]["completed_trades"] == 4
    assert report["C2"]["summary"]["daily"]["completed_trades"] == 1
    assert all(
        row["safety"]["current_config_fingerprint"]
        and row["safety"]["installed_code_fingerprint"]
        for row in report["D2"]["instances"]
    )
    assert report["C2"]["instances"][0]["reconciliation"]["ok"] is True
    assert all(
        row["safety"]["installed_runtime_verification"]
        for row in report["C2"]["instances"]
    )
    assert report["source_policy"]["legacy_instances_excluded"] == ["btc", "eth", "xrp", "sol"]
    assert report["source_policy"]["d2_and_c2_returns_combined"] is False
    assert all(
        row["runtime_profile"] == "bounded_diagnostic"
        and row["strategy_role"] == "bounded_diagnostic_not_validated_alpha"
        and row["safety"]["bounded_profile"] is True
        and row["safety"]["observe_profile"] is False
        and row["safety"]["simulation_contract_source"]
        == "installed_bounded_runtime_identity"
        for row in report["D2"]["instances"]
    )


def test_d2_approved_observe_profile_uses_unoverridden_installed_identity(
    reporting_root,
) -> None:
    root, window, generated_ns = reporting_root
    _set_d2_observe_profile(root, "d2-btc")

    report = build_scorecard(
        root=root,
        window=window,
        generated_wall_ns=generated_ns,
    )
    row = report["D2"]["instances"][0]

    assert report["safety"]["verified"] is True
    assert row["runtime_profile"] == "public_feed_observe"
    assert row["strategy_role"] == "public_feed_observation_no_orders"
    assert row["operations"]["decision_count"] == 0
    assert row["operations"]["fill_count"] == 0
    assert row["current"]["pending_orders"] == 0
    assert row["safety"]["bounded_profile"] is False
    assert row["safety"]["observe_profile"] is True
    assert row["safety"]["observe_zero_activity"] is True
    assert row["safety"]["installed_code_fingerprint"] is True
    assert row["safety"]["simulation_contract_source"] == (
        "installed_observe_runtime_identity"
    )
    assert row["safety"]["verified"] is True
    markdown = scorecard_markdown(report)
    assert "D2 approved shadow profiles" in markdown
    assert "current active ledger" in markdown
    assert "자동 합산하지 않습니다" in markdown


def test_d2_observe_only_improvement_uses_observation_quality_not_pnl(
    reporting_root,
) -> None:
    root, window, generated_ns = reporting_root
    for instance in ("d2-btc", "d2-eth", "d2-xrp", "d2-sol"):
        _set_d2_observe_profile(root, instance)

    report = build_scorecard(
        root=root,
        window=window,
        generated_wall_ns=generated_ns,
    )
    observations = report["improvement"]["observations"]

    assert report["safety"]["verified"] is True
    assert any(
        "D2 observe 활성 원장은 주문 없는 공개피드 관찰 구간" in item
        for item in observations
    )
    assert not any(item.startswith("D2 회계 실현손익") for item in observations)
    assert not any(item.startswith("D2 최근 7일") for item in observations)
    assert not any("D2 누적 수수료" in item for item in observations)


@pytest.mark.parametrize("activity", ["decision", "fill", "pending_order"])
def test_d2_observe_profile_fails_closed_on_order_activity(
    reporting_root,
    activity: str,
) -> None:
    root, window, generated_ns = reporting_root
    database = _set_d2_observe_profile(root, "d2-btc")
    connection = sqlite3.connect(database)
    if activity == "decision":
        connection.execute(
            "INSERT INTO shadow_decisions VALUES ('unexpected', 'run-1')"
        )
    elif activity == "fill":
        connection.execute(
            "INSERT INTO shadow_fills VALUES ('unexpected', 'run-1', ?, 'buy', 1, 100, 0.05)",
            (window.start_wall_ns + 1,),
        )
    else:
        connection.execute(
            "INSERT INTO shadow_orders VALUES ('unexpected', 'run-1', 'pending')"
        )
    connection.commit()
    connection.close()

    report = build_scorecard(
        root=root,
        window=window,
        generated_wall_ns=generated_ns,
    )
    row = report["D2"]["instances"][0]

    assert row["available"] is True
    assert row["safety"]["observe_profile"] is True
    assert row["safety"]["observe_zero_activity"] is False
    assert row["safety"]["simulation_contract"] is False
    assert row["safety"]["verified"] is False
    assert report["safety"]["verified"] is False


def test_d2_observe_profile_requires_exact_model_and_runtime_flags(
    reporting_root,
) -> None:
    root, window, generated_ns = reporting_root
    _set_d2_observe_profile(root, "d2-btc")
    config_path = root / "instances/d2-btc/config/config.toml"
    _write(
        config_path,
        config_path.read_text(encoding="utf-8").replace(
            'model_version = "observe-public-feed-v1"',
            'model_version = "observe-public-feed-v2"',
        ),
    )

    report = build_scorecard(
        root=root,
        window=window,
        generated_wall_ns=generated_ns,
    )
    row = report["D2"]["instances"][0]

    assert row["runtime_profile"] == "unapproved"
    assert row["safety"]["approved_profile"] is False
    assert row["safety"]["simulation_contract"] is False
    assert row["safety"]["verified"] is False


def test_d2_pending_audit_backlog_is_reported_without_notifier_failure(
    reporting_root,
) -> None:
    root, window, generated_ns = reporting_root
    database = (
        root / "instances/d2-btc/data/shadow-diagnostic-bounded-v1-test.db"
    )
    connection = sqlite3.connect(database)
    connection.executemany(
        "INSERT INTO notification_outbox VALUES (?, 'run-1', ?, ?, ?, ?)",
        [
            (
                "pending-warning",
                "shadow_feed_continuity",
                "warning",
                "pending",
                generated_ns - 7200 * 1_000_000_000,
            ),
            (
                "pending-critical",
                "shadow_halted",
                "critical",
                "pending",
                generated_ns - 3600 * 1_000_000_000,
            ),
            (
                "sending-warning",
                "shadow_restart_recovery",
                "warning",
                "sending",
                generated_ns - 1800 * 1_000_000_000,
            ),
            (
                "delivered-warning",
                "shadow_restart_recovery",
                "warning",
                "delivered",
                generated_ns - 10_800 * 1_000_000_000,
            ),
        ],
    )
    connection.commit()
    connection.close()

    report = build_scorecard(
        root=root,
        window=window,
        generated_wall_ns=generated_ns,
    )
    row = report["D2"]["instances"][0]
    operations = row["operations"]

    assert report["safety"]["verified"] is True
    assert operations["pending_outbox"] == 3
    assert operations["pending_warning"] == 2
    assert operations["pending_critical"] == 1
    assert operations["oldest_pending_age_seconds"] == pytest.approx(7200.0)
    markdown = scorecard_markdown(report)
    message = json.dumps(slack_message(report), ensure_ascii=False)
    for rendered in (markdown, message):
        assert "audit unresolved/warn/crit 3/2/1, oldest 7200s" in rendered
        assert "notifier OFF" in rendered
        assert "관찰 품질" in rendered


def test_c2_archived_accounts_are_counted_but_excluded(reporting_root) -> None:
    root, window, generated_ns = reporting_root
    database = root / "instances/c2-btc/data/coinpilot-c2.db"
    archived_account = "paper-v8:archived-btc:KRW-BTC:60m"
    connection = sqlite3.connect(database)
    connection.execute(
        "INSERT INTO paper_state VALUES (?, ?, ?)",
        (archived_account, '{"archived": true}', "not-an-active-timestamp"),
    )
    connection.execute(
        "INSERT INTO paper_events VALUES (?, ?, ?, ?, ?)",
        (
            "archived-malformed-event",
            archived_account,
            "not-an-active-timestamp",
            "fill",
            '{"side": "sell", "notional": "poison"}',
        ),
    )
    connection.commit()
    connection.close()

    report = build_scorecard(
        root=root,
        window=window,
        generated_wall_ns=generated_ns,
    )
    row = report["C2"]["instances"][0]

    assert report["safety"]["verified"] is True
    assert row["available"] is True
    assert row["safety"]["verified"] is True
    assert row["database_health"]["selected_paper_state_rows"] == 1
    assert row["database_health"]["archived_paper_state_rows_excluded"] == 1
    assert row["database_health"]["archived_events_excluded"] == 1
    assert row["metrics"]["daily"]["completed_trades"] == 1


def test_c2_manifest_active_account_mismatch_is_rejected(reporting_root) -> None:
    root, window, generated_ns = reporting_root
    manifest_path = root / "instances/c2-btc/config/paper.manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["market_config"]["paper_account_key"] = (
        "paper-v8:archived-btc:KRW-BTC:60m"
    )
    _write(manifest_path, _canonical_json(manifest) + "\n")

    report = build_scorecard(
        root=root,
        window=window,
        generated_wall_ns=generated_ns,
    )
    row = report["C2"]["instances"][0]

    assert report["safety"]["verified"] is False
    assert row["available"] is False
    assert "account key does not match the frozen policy" in row["error"]


@pytest.mark.parametrize(
    ("family", "instance", "relative_path", "credential_key"),
    [
        ("D2", "d2-btc", "config/runtime.env", "UPBIT_ACCESS_KEY"),
        ("C2", "c2-btc", "config/runtime.env", "AWS_ACCESS_KEY_ID"),
        ("D2", "d2-btc", "config/config.toml", "COINPILOT_API_KEY"),
        ("C2", "c2-btc", "config/paper.toml", "SLACK_WEBHOOK_URL"),
    ],
)
def test_prefixed_credential_keys_are_rejected_in_runtime_or_config(
    reporting_root,
    family: str,
    instance: str,
    relative_path: str,
    credential_key: str,
) -> None:
    root, window, generated_ns = reporting_root
    path = root / "instances" / instance / relative_path
    separator = "=" if path.suffix == ".env" else " = "
    _write(
        path,
        path.read_text(encoding="utf-8")
        + f"{credential_key}{separator}\"must-not-be-read\"\n",
    )

    report = build_scorecard(
        root=root,
        window=window,
        generated_wall_ns=generated_ns,
    )
    row = next(
        item
        for item in report[family]["instances"]
        if item["instance"] == instance
    )

    assert row["available"] is False
    assert row["safety"]["verified"] is False
    assert "forbidden credential field" in row["error"]
    assert report["safety"]["verified"] is False


def test_public_api_url_and_keychain_account_are_not_credentials(
    reporting_root,
) -> None:
    root, window, generated_ns = reporting_root
    runtime_path = root / "instances/d2-btc/config/runtime.env"
    _write(
        runtime_path,
        runtime_path.read_text(encoding="utf-8")
        + "COINPILOT_KEYCHAIN_ACCOUNT=coinpilot-shadow\n",
    )

    report = build_scorecard(
        root=root,
        window=window,
        generated_wall_ns=generated_ns,
    )

    assert report["safety"]["verified"] is True
    assert all(row["available"] for row in report["D2"]["instances"])
    assert all(row["available"] for row in report["C2"]["instances"])


@pytest.mark.parametrize(
    ("scenario", "error_fragment"),
    [
        ("cycle", "restart lineage contains a cycle"),
        ("missing_predecessor", "predecessor is missing"),
        ("orphan", "branch or orphan run"),
        ("branch", "branch or orphan run"),
    ],
)
def test_d2_invalid_restart_lineage_is_rejected(
    reporting_root,
    scenario: str,
    error_fragment: str,
) -> None:
    root, window, generated_ns = reporting_root
    database = (
        root
        / "instances/d2-btc/data/shadow-diagnostic-bounded-v1-test.db"
    )
    connection = sqlite3.connect(database)

    if scenario == "cycle":
        connection.execute(
            "UPDATE shadow_runs SET restart_of_run_id = 'run-1' WHERE run_id = 'run-1'"
        )
    elif scenario == "missing_predecessor":
        connection.execute(
            "UPDATE shadow_runs SET restart_of_run_id = 'missing' WHERE run_id = 'run-1'"
        )
    else:
        base_run = list(
            connection.execute(
                "SELECT * FROM shadow_runs WHERE run_id = 'run-1'"
            ).fetchone()
        )
        base_state = list(
            connection.execute(
                "SELECT * FROM shadow_state WHERE run_id = 'run-1'"
            ).fetchone()
        )

        def insert_run(
            run_id: str,
            *,
            offset_ns: int,
            predecessor: str | None,
            with_state: bool,
        ) -> None:
            run = list(base_run)
            run[0] = run_id
            run[5] = int(run[5]) + offset_ns
            run[7] = predecessor
            connection.execute(
                f"INSERT INTO shadow_runs VALUES ({', '.join('?' for _ in run)})",
                run,
            )
            if with_state:
                state = list(base_state)
                state[0] = run_id
                connection.execute(
                    f"INSERT INTO shadow_state VALUES ({', '.join('?' for _ in state)})",
                    state,
                )

        if scenario == "orphan":
            insert_run(
                "orphan-run",
                offset_ns=-1,
                predecessor=None,
                with_state=False,
            )
        else:
            insert_run(
                "branch-a",
                offset_ns=1,
                predecessor="run-1",
                with_state=True,
            )
            insert_run(
                "branch-b",
                offset_ns=2,
                predecessor="run-1",
                with_state=True,
            )
    connection.commit()
    connection.close()

    report = build_scorecard(
        root=root,
        window=window,
        generated_wall_ns=generated_ns,
    )
    row = report["D2"]["instances"][0]

    assert report["safety"]["verified"] is False
    assert row["available"] is False
    assert error_fragment in row["error"]


@pytest.mark.parametrize(
    ("identity_field", "safety_field"),
    [
        ("config_hash", "current_config_fingerprint"),
        ("code_version", "installed_code_fingerprint"),
    ],
)
def test_d2_installed_identity_mismatch_is_rejected(
    reporting_root,
    identity_field: str,
    safety_field: str,
) -> None:
    root, window, generated_ns = reporting_root
    home = root / "instances/d2-btc"
    database = home / "data/shadow-diagnostic-bounded-v1-test.db"
    connection = sqlite3.connect(database)
    stored = connection.execute(
        "SELECT config_json, config_hash, code_version FROM shadow_runs WHERE run_id = 'run-1'"
    ).fetchone()
    connection.close()
    installed_identity = {
        "config": json.loads(stored[0]),
        "config_hash": stored[1],
        "code_version": stored[2],
    }
    installed_identity[identity_field] = f"tampered-installed-{identity_field}"
    _write_executable(
        home / "venv/bin/python",
        "#!/bin/sh\nprintf '%s\\n' '"
        + _canonical_json(installed_identity)
        + "'\n",
    )

    report = build_scorecard(
        root=root,
        window=window,
        generated_wall_ns=generated_ns,
    )
    row = report["D2"]["instances"][0]

    assert row["available"] is True
    assert row["safety"][safety_field] is False
    assert row["safety"]["verified"] is False
    assert report["safety"]["verified"] is False


@pytest.mark.parametrize(
    ("health_field", "unsafe_value"),
    [("simulated", False), ("own_execution", True)],
)
def test_d2_health_must_prove_simulation_only(
    reporting_root,
    health_field: str,
    unsafe_value: bool,
) -> None:
    root, window, generated_ns = reporting_root
    database = (
        root
        / "instances/d2-btc/data/shadow-diagnostic-bounded-v1-test.db"
    )
    connection = sqlite3.connect(database)
    details = json.loads(
        connection.execute(
            "SELECT details_json FROM shadow_health WHERE health_id = 'health'"
        ).fetchone()[0]
    )
    details[health_field] = unsafe_value
    connection.execute(
        "UPDATE shadow_health SET details_json = ? WHERE health_id = 'health'",
        (json.dumps(details),),
    )
    connection.commit()
    connection.close()

    report = build_scorecard(
        root=root,
        window=window,
        generated_wall_ns=generated_ns,
    )
    row = report["D2"]["instances"][0]

    assert row["available"] is True
    assert row["safety"]["verified"] is False
    assert report["safety"]["verified"] is False


def test_legacy_missing_simulation_fields_use_installed_identity_fallback(
    reporting_root,
) -> None:
    root, window, generated_ns = reporting_root

    d2_database = (
        root
        / "instances/d2-btc/data/shadow-diagnostic-bounded-v1-test.db"
    )
    connection = sqlite3.connect(d2_database)
    details = json.loads(
        connection.execute(
            "SELECT details_json FROM shadow_health WHERE health_id = 'health'"
        ).fetchone()[0]
    )
    details.pop("simulated")
    details.pop("own_execution")
    connection.execute(
        "UPDATE shadow_health SET details_json = ? WHERE health_id = 'health'",
        (json.dumps(details),),
    )
    connection.commit()
    connection.close()

    c2_home = root / "instances/c2-btc"
    manifest_path = c2_home / "config/paper.manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["safety"].pop("simulated", None)
    manifest["safety"].pop("own_execution")
    _write(manifest_path, _canonical_json(manifest) + "\n")
    installed_identity = {
        "valid": True,
        "market": "KRW-BTC",
        "paper_account_key": manifest["market_config"]["paper_account_key"],
        "paper_config_fingerprint": manifest["market_config"][
            "paper_config_fingerprint"
        ],
        "public_only": True,
        "live_order_routing": False,
        "orders_sent": 0,
    }
    _write_executable(
        c2_home / "bin/coinpilot-c2-config",
        "#!/bin/sh\nprintf '%s\\n' '"
        + _canonical_json(installed_identity)
        + "'\n",
    )

    report = build_scorecard(
        root=root,
        window=window,
        generated_wall_ns=generated_ns,
    )
    d2_row = report["D2"]["instances"][0]
    c2_row = report["C2"]["instances"][0]

    assert report["safety"]["verified"] is True
    assert report["safety"]["simulated"] is True
    assert report["safety"]["own_execution"] is False
    for row, source in (
        (d2_row, "installed_bounded_runtime_identity"),
        (c2_row, "installed_frozen_public_paper_identity"),
    ):
        assert row["safety"]["simulation_contract"] is True
        assert row["safety"]["simulation_contract_source"] == source
        assert row["safety"]["simulated"] is True
        assert row["safety"]["own_execution"] is False
        assert row["safety"]["verified"] is True


@pytest.mark.parametrize("halted_fail_closed", [False, True])
def test_d2_exact_risk_boundary_requires_fail_closed_halt(
    reporting_root,
    halted_fail_closed: bool,
) -> None:
    root, window, generated_ns = reporting_root
    database = (
        root
        / "instances/d2-btc/data/shadow-diagnostic-bounded-v1-test.db"
    )
    connection = sqlite3.connect(database)
    connection.execute(
        "UPDATE shadow_state SET max_drawdown = 0.10 WHERE run_id = 'run-1'"
    )
    if halted_fail_closed:
        connection.execute(
            """
            UPDATE shadow_state
            SET lifecycle_status = 'halted', halt_reason = 'max_drawdown'
            WHERE run_id = 'run-1'
            """
        )
        connection.execute(
            """
            UPDATE shadow_runs
            SET status = 'halted', halt_reason = 'max_drawdown'
            WHERE run_id = 'run-1'
            """
        )
    connection.commit()
    connection.close()

    report = build_scorecard(
        root=root,
        window=window,
        generated_wall_ns=generated_ns,
    )
    row = report["D2"]["instances"][0]

    assert row["available"] is True
    assert row["current"]["pending_orders"] == 0
    assert row["safety"]["risk_boundary_breached"] is True
    assert row["safety"]["risk_boundary_fail_closed"] is halted_fail_closed
    assert row["safety"]["verified"] is halted_fail_closed
    assert report["safety"]["verified"] is halted_fail_closed


@pytest.mark.parametrize("source", ["manifest", "installed_identity"])
def test_c2_own_execution_claim_is_rejected(reporting_root, source: str) -> None:
    root, window, generated_ns = reporting_root
    home = root / "instances/c2-btc"
    manifest_path = home / "config/paper.manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if source == "manifest":
        manifest["safety"]["own_execution"] = True
        _write(manifest_path, _canonical_json(manifest) + "\n")
    else:
        identity = {
            "valid": True,
            "market": "KRW-BTC",
            "paper_account_key": manifest["market_config"]["paper_account_key"],
            "paper_config_fingerprint": manifest["market_config"][
                "paper_config_fingerprint"
            ],
            "public_only": True,
            "own_execution": True,
            "live_order_routing": False,
            "orders_sent": 0,
        }
        _write_executable(
            home / "bin/coinpilot-c2-config",
            "#!/bin/sh\nprintf '%s\\n' '" + _canonical_json(identity) + "'\n",
        )

    report = build_scorecard(
        root=root,
        window=window,
        generated_wall_ns=generated_ns,
    )
    row = report["C2"]["instances"][0]

    assert row["available"] is True
    assert row["safety"]["verified"] is False
    assert report["safety"]["verified"] is False


def test_post_window_and_post_generated_fills_reconcile_but_do_not_leak_daily(
    reporting_root,
) -> None:
    root, window, generated_ns = reporting_root
    buy_ns = generated_ns + 1_000_000_000
    sell_ns = generated_ns + 2_000_000_000
    extra_pnl = (99.0 - 0.0495) - (100.0 + 0.05)

    d2_database = (
        root
        / "instances/d2-btc/data/shadow-diagnostic-bounded-v1-test.db"
    )
    connection = sqlite3.connect(d2_database)
    connection.executemany(
        "INSERT INTO shadow_fills VALUES (?, 'run-1', ?, ?, 1.0, ?, ?)",
        [
            ("post-buy", buy_ns, "buy", 100.0, 0.05),
            ("post-sell", sell_ns, "sell", 99.0, 0.0495),
        ],
    )
    connection.execute(
        """
        UPDATE shadow_state
        SET cash_quote = cash_quote + ?,
            realized_pnl_quote = realized_pnl_quote + ?,
            cumulative_fees_quote = cumulative_fees_quote + ?,
            updated_wall_ns = ?
        WHERE run_id = 'run-1'
        """,
        (extra_pnl, extra_pnl, 0.0995, sell_ns),
    )
    connection.commit()
    connection.close()

    c2_home = root / "instances/c2-btc"
    manifest = json.loads(
        (c2_home / "config/paper.manifest.json").read_text(encoding="utf-8")
    )
    account_key = manifest["market_config"]["paper_account_key"]
    c2_database = c2_home / "data/coinpilot-c2.db"
    connection = sqlite3.connect(c2_database)
    for event_id, timestamp_ns, side, notional, fee in (
        ("post-buy", buy_ns, "buy", 100.0, 0.05),
        ("post-sell", sell_ns, "sell", 99.0, 0.0495),
    ):
        payload = {
            "side": side,
            "quantity": 1.0,
            "notional": notional,
            "fee": fee,
            "slippage_cost": 0.05,
            "reason": "post_window_test",
        }
        timestamp = datetime.fromtimestamp(timestamp_ns / 1e9, UTC).isoformat()
        connection.execute(
            "INSERT INTO paper_events VALUES (?, ?, ?, 'fill', ?)",
            (event_id, account_key, timestamp, json.dumps(payload)),
        )
    state_row = connection.execute(
        "SELECT state_json FROM paper_state WHERE account_key = ?",
        (account_key,),
    ).fetchone()
    state = json.loads(state_row[0])
    state["cash"] += extra_pnl
    state["realized_pnl"] += extra_pnl
    state["revision"] += 1
    state["updated_at"] = datetime.fromtimestamp(sell_ns / 1e9, UTC).isoformat()
    connection.execute(
        "UPDATE paper_state SET state_json = ?, updated_at = ? WHERE account_key = ?",
        (json.dumps(state), state["updated_at"], account_key),
    )
    connection.commit()
    connection.close()

    report = build_scorecard(
        root=root,
        window=window,
        generated_wall_ns=generated_ns,
    )
    d2_btc = report["D2"]["instances"][0]
    c2_btc = report["C2"]["instances"][0]

    assert report["safety"]["verified"] is True
    assert d2_btc["metrics"]["daily"]["fill_count"] == 1
    assert d2_btc["metrics"]["daily"]["realized_pnl_quote"] == pytest.approx(
        -4.398
    )
    assert d2_btc["current"]["realized_pnl_quote"] == pytest.approx(
        -4.398 + extra_pnl
    )
    assert d2_btc["reconciliation"]["ok"] is True
    assert c2_btc["metrics"]["daily"]["fill_count"] == 1
    assert c2_btc["metrics"]["daily"]["realized_pnl_quote"] == pytest.approx(
        -4.398
    )
    assert c2_btc["current"]["realized_pnl_quote"] == pytest.approx(
        -4.398 + extra_pnl
    )
    assert c2_btc["reconciliation"]["ok"] is True


def test_c2_non_flat_stale_closed_candle_is_labeled_unavailable(
    reporting_root,
) -> None:
    root, window, generated_ns = reporting_root
    home = root / "instances/c2-btc"
    manifest = json.loads(
        (home / "config/paper.manifest.json").read_text(encoding="utf-8")
    )
    account_key = manifest["market_config"]["paper_account_key"]
    database = home / "data/coinpilot-c2.db"
    buy_ns = window.end_wall_ns - 30 * 60 * 1_000_000_000
    stale_candle_ns = window.start_wall_ns - 3 * 60 * 60 * 1_000_000_000
    connection = sqlite3.connect(database)
    connection.execute(
        "INSERT INTO candles VALUES (?, 60, ?, 100, 100, 100, 100, 1, 100)",
        (
            "KRW-BTC",
            datetime.fromtimestamp(stale_candle_ns / 1e9, UTC).isoformat(),
        ),
    )
    payload = {
        "side": "buy",
        "quantity": 1.0,
        "notional": 100.0,
        "fee": 0.05,
        "slippage_cost": 0.05,
        "reason": "stale_mark_test",
    }
    connection.execute(
        "INSERT INTO paper_events VALUES ('open-buy', ?, ?, 'fill', ?)",
        (
            account_key,
            datetime.fromtimestamp(buy_ns / 1e9, UTC).isoformat(),
            json.dumps(payload),
        ),
    )
    state_row = connection.execute(
        "SELECT state_json FROM paper_state WHERE account_key = ?",
        (account_key,),
    ).fetchone()
    state = json.loads(state_row[0])
    state["cash"] -= 100.05
    state["quantity"] = 1.0
    state["revision"] += 1
    state["updated_at"] = datetime.fromtimestamp(
        (generated_ns - 1_000_000_000) / 1e9,
        UTC,
    ).isoformat()
    connection.execute(
        "UPDATE paper_state SET state_json = ?, updated_at = ? WHERE account_key = ?",
        (json.dumps(state), state["updated_at"], account_key),
    )
    connection.commit()
    connection.close()

    report = build_scorecard(
        root=root,
        window=window,
        generated_wall_ns=generated_ns,
    )
    row = report["C2"]["instances"][0]

    assert row["reconciliation"]["ok"] is True
    assert row["equity"]["end"]["available"] is False
    assert (
        row["equity"]["end"]["valuation_quality"]
        == "unavailable_non_flat_stale_closed_candle"
    )
    assert row["current"]["equity_quote"] is None
    assert (
        row["current"]["equity_valuation_quality"]
        == "unavailable_non_flat_stale_closed_candle"
    )


@pytest.mark.parametrize(
    ("bad_close", "error_fragment"),
    [
        pytest.param(0.0, "closed candle mark must be positive", id="zero"),
        pytest.param(
            "NaN",
            "closed candle mark is outside its valid range",
            id="nan",
        ),
    ],
)
def test_c2_non_flat_invalid_candle_close_fails_source_safety(
    reporting_root,
    bad_close: float | str,
    error_fragment: str,
) -> None:
    root, window, generated_ns = reporting_root
    database = root / "instances/c2-btc/data/coinpilot-c2.db"
    candle_ns = window.start_wall_ns - 30 * 60 * 1_000_000_000
    connection = sqlite3.connect(database)
    connection.execute(
        "INSERT INTO candles VALUES (?, 60, ?, 100, 100, 100, ?, 1, 100)",
        (
            "KRW-BTC",
            datetime.fromtimestamp(candle_ns / 1e9, UTC).isoformat(),
            bad_close,
        ),
    )
    connection.commit()
    connection.close()

    report = build_scorecard(
        root=root,
        window=window,
        generated_wall_ns=generated_ns,
    )
    row = report["C2"]["instances"][0]

    assert row["available"] is False
    assert row["safety"]["verified"] is False
    assert error_fragment in row["error"]
    assert report["C2"]["summary"]["safety_verified"] is False
    assert report["safety"]["verified"] is False
    assert "SOURCE ERROR" in json.dumps(slack_message(report), ensure_ascii=False)


def test_d2_warmup_is_alive_but_not_ready(reporting_root) -> None:
    root, window, generated_ns = reporting_root
    database = (
        root
        / "instances/d2-btc/data/shadow-diagnostic-bounded-v1-test.db"
    )
    connection = sqlite3.connect(database)
    connection.execute("UPDATE shadow_runs SET status = 'warmup' WHERE run_id = 'run-1'")
    connection.execute(
        "UPDATE shadow_state SET lifecycle_status = 'warmup' WHERE run_id = 'run-1'"
    )
    connection.commit()
    connection.close()

    report = build_scorecard(
        root=root,
        window=window,
        generated_wall_ns=generated_ns,
    )
    row = report["D2"]["instances"][0]

    assert report["safety"]["verified"] is True
    assert row["current"]["alive_ok"] is True
    assert row["current"]["status_ok"] is True
    assert row["current"]["ready"] is False
    assert report["D2"]["summary"]["current_status_ok"] is True


def test_slack_and_markdown_include_all_markets_and_safety(reporting_root) -> None:
    root, window, generated_ns = reporting_root
    report = build_scorecard(root=root, window=window, generated_wall_ns=generated_ns)
    message = slack_message(report)
    encoded = json.dumps(message, ensure_ascii=False)
    markdown = scorecard_markdown(report)
    for market in ("KRW-BTC", "KRW-ETH", "KRW-XRP", "KRW-SOL"):
        assert market in encoded
        assert market in markdown
    assert "simulated=true" in encoded
    assert "own_execution=false" in encoded
    assert "live_order_routing=false" in encoded
    assert "orders_sent=0" in encoded
    assert "자동 전략 변경" in markdown
    assert len(message["blocks"]) <= 50
    for block in message["blocks"]:
        if isinstance(block.get("text"), dict):
            assert len(block["text"]["text"]) <= 3000


def test_slack_and_markdown_label_equity_coverage_quality(reporting_root) -> None:
    root, window, generated_ns = reporting_root

    d2_database = (
        root
        / "instances/d2-btc/data/shadow-diagnostic-bounded-v1-test.db"
    )
    connection = sqlite3.connect(d2_database)
    connection.execute("DELETE FROM shadow_equity WHERE equity_id = 'eq-start'")
    connection.commit()
    connection.close()

    c2_home = root / "instances/c2-eth"
    manifest = json.loads(
        (c2_home / "config/paper.manifest.json").read_text(encoding="utf-8")
    )
    account_key = manifest["market_config"]["paper_account_key"]
    c2_database = c2_home / "data/coinpilot-c2.db"
    buy_ns = window.start_wall_ns - 45 * 60 * 1_000_000_000
    connection = sqlite3.connect(c2_database)
    connection.execute(
        "INSERT INTO paper_events VALUES (?, ?, ?, 'fill', ?)",
        (
            "coverage-open-buy",
            account_key,
            datetime.fromtimestamp(buy_ns / 1e9, UTC).isoformat(),
            json.dumps(
                {
                    "side": "buy",
                    "quantity": 1.0,
                    "notional": 100.0,
                    "fee": 0.05,
                    "slippage_cost": 0.05,
                    "reason": "coverage_quality_test",
                }
            ),
        ),
    )
    for candle_ns in (
        window.start_wall_ns - 30 * 60 * 1_000_000_000,
        window.end_wall_ns - 30 * 60 * 1_000_000_000,
    ):
        connection.execute(
            "INSERT INTO candles VALUES (?, 60, ?, 100, 100, 100, 100, 1, 100)",
            (
                "KRW-ETH",
                datetime.fromtimestamp(candle_ns / 1e9, UTC).isoformat(),
            ),
        )
    state_row = connection.execute(
        "SELECT state_json FROM paper_state WHERE account_key = ?",
        (account_key,),
    ).fetchone()
    state = json.loads(state_row[0])
    state["cash"] = 899.95
    state["quantity"] = 1.0
    state["fees"] = 0.05
    state["revision"] += 1
    connection.execute(
        "UPDATE paper_state SET state_json = ? WHERE account_key = ?",
        (json.dumps(state), account_key),
    )
    connection.commit()
    connection.close()

    report = build_scorecard(
        root=root,
        window=window,
        generated_wall_ns=generated_ns,
    )
    message_text = json.dumps(slack_message(report), ensure_ascii=False)
    markdown = scorecard_markdown(report)

    assert report["safety"]["verified"] is True
    for coverage in ("partial(start +12.0h)", "estimated", "exact-flat"):
        assert coverage in message_text
        assert coverage in markdown


def test_delivery_freezes_artifact_and_deduplicates(reporting_root) -> None:
    root, window, generated_ns = reporting_root
    state_dir, artifact_dir = _reporting_paths(root)
    client = RecordingClient()

    first = run_delivery(
        root=root,
        state_dir=state_dir,
        artifact_dir=artifact_dir,
        window=window,
        keychain_service="unused",
        keychain_account="unused",
        now_wall_ns=generated_ns,
        client=client,
    )
    second = run_delivery(
        root=root,
        state_dir=state_dir,
        artifact_dir=artifact_dir,
        window=window,
        keychain_service="unused",
        keychain_account="unused",
        now_wall_ns=generated_ns + 1,
        client=client,
    )

    assert first["status"] == "delivered"
    assert second["status"] == "already_delivered"
    assert len(client.messages) == 1
    for path in artifact_dir.iterdir():
        assert os.stat(path).st_mode & 0o777 == 0o600
    state_path = state_dir / "deliveries/2026-07-22.json"
    assert os.stat(state_dir).st_mode & 0o777 == 0o700
    assert os.stat(artifact_dir).st_mode & 0o777 == 0o700
    assert os.stat(state_path.parent).st_mode & 0o777 == 0o700
    assert os.stat(state_dir / "daily-scorecard.lock").st_mode & 0o777 == 0o600
    assert os.stat(state_path).st_mode & 0o777 == 0o600
    state = json.loads(state_path.read_text())
    assert state["status"] == "delivered"
    assert state["report_id"] == "daily-scorecard:v1:2026-07-22"
    assert "hooks.slack.com" not in json.dumps(state)


def test_source_incomplete_only_sends_throttled_quality_alert(
    reporting_root,
) -> None:
    root, window, generated_ns = reporting_root
    state_dir, artifact_dir = _reporting_paths(root)
    approved = root / "instances/d2-btc/data/shadow-diagnostic-bounded-v1-test.db"
    approved.rename(root / "instances/d2-btc/data/shadow.db")
    client = RecordingClient()

    for now_ns in (
        generated_ns,
        generated_ns + 1_000_000_000,
        generated_ns + 6 * 60 * 60 * 1_000_000_000,
    ):
        with pytest.raises(
            ScorecardError,
            match="source completeness or safety validation failed",
        ):
            run_delivery(
                root=root,
                state_dir=state_dir,
                artifact_dir=artifact_dir,
                window=window,
                keychain_service="unused",
                keychain_account="unused",
                now_wall_ns=now_ns,
                client=client,
            )

    assert len(client.messages) == 2
    assert all("[QUALITY]" in str(message["text"]) for message in client.messages)
    assert artifact_dir.is_dir()
    assert list(artifact_dir.iterdir()) == []
    assert not (state_dir / "deliveries" / f"{window.report_date}.json").exists()
    incident_path = state_dir / "incidents" / f"{window.report_date}.json"
    incident = json.loads(incident_path.read_text(encoding="utf-8"))
    assert incident["final_scorecard_delivered"] is False
    assert incident["sent_wall_ns"] == generated_ns + 6 * 60 * 60 * 1_000_000_000
    assert os.stat(incident_path).st_mode & 0o777 == 0o600
    assert os.stat(incident_path.parent).st_mode & 0o777 == 0o700


@pytest.mark.parametrize("unsafe_target", ["receipt", "artifact"])
def test_delivery_rejects_non_private_receipt_or_artifact(
    reporting_root,
    unsafe_target: str,
) -> None:
    root, window, generated_ns = reporting_root
    state_dir, artifact_dir = _reporting_paths(root)
    client = RecordingClient()
    delivered = run_delivery(
        root=root,
        state_dir=state_dir,
        artifact_dir=artifact_dir,
        window=window,
        keychain_service="unused",
        keychain_account="unused",
        now_wall_ns=generated_ns,
        client=client,
    )
    state_path = state_dir / "deliveries" / f"{window.report_date}.json"
    unsafe_path = (
        state_path
        if unsafe_target == "receipt"
        else Path(delivered["artifacts"]["json"])
    )
    unsafe_path.chmod(0o644)

    with pytest.raises(
        ScorecardError,
        match="private regular file|artifact mode is unsafe",
    ):
        run_delivery(
            root=root,
            state_dir=state_dir,
            artifact_dir=artifact_dir,
            window=window,
            keychain_service="unused",
            keychain_account="unused",
            now_wall_ns=generated_ns + 1,
            client=client,
        )
    assert len(client.messages) == 1


def test_concurrent_delivery_uses_lock_and_deduplicates(reporting_root) -> None:
    root, window, generated_ns = reporting_root
    state_dir, artifact_dir = _reporting_paths(root)
    client = BlockingClient()
    results: list[dict[str, object]] = []
    errors: list[BaseException] = []

    def deliver() -> None:
        try:
            results.append(
                run_delivery(
                    root=root,
                    state_dir=state_dir,
                    artifact_dir=artifact_dir,
                    window=window,
                    keychain_service="unused",
                    keychain_account="unused",
                    now_wall_ns=generated_ns,
                    client=client,
                )
            )
        except BaseException as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    thread = threading.Thread(target=deliver, daemon=True)
    thread.start()
    try:
        assert client.started.wait(timeout=30)
        concurrent = run_delivery(
            root=root,
            state_dir=state_dir,
            artifact_dir=artifact_dir,
            window=window,
            keychain_service="unused",
            keychain_account="unused",
            now_wall_ns=generated_ns + 1,
            client=client,
        )
    finally:
        client.release.set()
    thread.join(timeout=30)

    assert not thread.is_alive()
    assert errors == []
    assert results[0]["status"] == "delivered"
    assert concurrent["status"] == "busy"
    again = run_delivery(
        root=root,
        state_dir=state_dir,
        artifact_dir=artifact_dir,
        window=window,
        keychain_service="unused",
        keychain_account="unused",
        now_wall_ns=generated_ns + 2,
        client=client,
    )
    assert again["status"] == "already_delivered"
    assert len(client.messages) == 1


def test_delivery_failure_uses_isolated_backoff_state(reporting_root) -> None:
    root, window, generated_ns = reporting_root
    state_dir, artifact_dir = _reporting_paths(root)
    result = run_delivery(
        root=root,
        state_dir=state_dir,
        artifact_dir=artifact_dir,
        window=window,
        keychain_service="unused",
        keychain_account="unused",
        now_wall_ns=generated_ns,
        client=FailingClient(),
    )
    assert result["status"] == "retry_scheduled"
    assert result["available_wall_ns"] >= generated_ns + 1200 * 1_000_000_000
    state = json.loads((state_dir / "deliveries/2026-07-22.json").read_text())
    assert state["status"] == "pending"
    assert state["last_error"] == "SlackDeliveryError"


def test_multi_day_catch_up_prefers_latest_then_oldest_and_stops_resending(
    reporting_root,
) -> None:
    root, latest, generated_ns = reporting_root
    state_dir, artifact_dir = _reporting_paths(root)
    client = RecordingClient()
    oldest = window_for_date(latest.report_date - timedelta(days=1))

    assert (
        select_scheduled_window(
            state_dir=state_dir,
            now_wall_ns=generated_ns,
        ).report_date
        == latest.report_date
    )
    latest_result = run_delivery(
        root=root,
        state_dir=state_dir,
        artifact_dir=artifact_dir,
        window=latest,
        keychain_service="unused",
        keychain_account="unused",
        now_wall_ns=generated_ns,
        client=client,
    )
    assert latest_result["status"] == "delivered"
    assert (
        select_scheduled_window(
            state_dir=state_dir,
            now_wall_ns=generated_ns + 1,
        ).report_date
        == oldest.report_date
    )
    oldest_result = run_delivery(
        root=root,
        state_dir=state_dir,
        artifact_dir=artifact_dir,
        window=oldest,
        keychain_service="unused",
        keychain_account="unused",
        now_wall_ns=generated_ns + 1,
        client=client,
    )
    assert oldest_result["status"] == "delivered"
    assert (
        select_scheduled_window(
            state_dir=state_dir,
            now_wall_ns=generated_ns + 2,
        ).report_date
        == latest.report_date
    )
    again = run_delivery(
        root=root,
        state_dir=state_dir,
        artifact_dir=artifact_dir,
        window=latest,
        keychain_service="unused",
        keychain_account="unused",
        now_wall_ns=generated_ns + 2,
        client=client,
    )
    assert again["status"] == "already_delivered"
    assert len(client.messages) == 2


def test_catch_up_retry_respects_backoff_and_active_lease(reporting_root) -> None:
    root, latest, generated_ns = reporting_root
    state_dir, artifact_dir = _reporting_paths(root)
    oldest = window_for_date(latest.report_date - timedelta(days=1))
    run_delivery(
        root=root,
        state_dir=state_dir,
        artifact_dir=artifact_dir,
        window=latest,
        keychain_service="unused",
        keychain_account="unused",
        now_wall_ns=generated_ns,
        client=RecordingClient(),
    )
    failed = run_delivery(
        root=root,
        state_dir=state_dir,
        artifact_dir=artifact_dir,
        window=oldest,
        keychain_service="unused",
        keychain_account="unused",
        now_wall_ns=generated_ns + 1,
        client=FailingClient(),
    )
    available_ns = int(failed["available_wall_ns"])

    assert (
        select_scheduled_window(
            state_dir=state_dir,
            now_wall_ns=generated_ns + 2,
        ).report_date
        == latest.report_date
    )
    assert (
        select_scheduled_window(
            state_dir=state_dir,
            now_wall_ns=available_ns,
        ).report_date
        == oldest.report_date
    )

    state_path = state_dir / "deliveries" / f"{oldest.report_date}.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["status"] = "sending"
    state["lease_expires_wall_ns"] = available_ns + 100 * 1_000_000_000
    _write(state_path, json.dumps(state, sort_keys=True))
    assert (
        select_scheduled_window(
            state_dir=state_dir,
            now_wall_ns=state["lease_expires_wall_ns"] - 1,
        ).report_date
        == latest.report_date
    )
    assert (
        select_scheduled_window(
            state_dir=state_dir,
            now_wall_ns=state["lease_expires_wall_ns"],
        ).report_date
        == oldest.report_date
    )


def test_tampered_delivery_receipt_is_rejected(reporting_root) -> None:
    root, window, generated_ns = reporting_root
    state_dir, artifact_dir = _reporting_paths(root)
    run_delivery(
        root=root,
        state_dir=state_dir,
        artifact_dir=artifact_dir,
        window=window,
        keychain_service="unused",
        keychain_account="unused",
        now_wall_ns=generated_ns,
        client=RecordingClient(),
    )
    state_path = state_dir / "deliveries" / f"{window.report_date}.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["report_id"] = "daily-scorecard:v1:1999-01-01"
    _write(state_path, json.dumps(state, sort_keys=True))

    with pytest.raises(ScorecardError, match="invalid delivery receipt"):
        select_scheduled_window(
            state_dir=state_dir,
            now_wall_ns=generated_ns + 1,
        )


def test_incomplete_json_artifact_bundle_recovers_without_overwriting_json(
    reporting_root,
) -> None:
    root, window, generated_ns = reporting_root
    _, artifact_dir = _reporting_paths(root)
    report = build_scorecard(
        root=root,
        window=window,
        generated_wall_ns=generated_ns,
    )
    paths = _artifact_paths(artifact_dir, window.report_date.isoformat())
    frozen_json = (
        json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    ).encode("utf-8")
    paths["json"].parent.mkdir(parents=True, exist_ok=True)
    paths["json"].write_bytes(frozen_json)
    paths["json"].chmod(0o600)

    artifacts = write_report_artifacts(report, artifact_dir)

    assert paths["json"].read_bytes() == frozen_json
    assert paths["markdown"].read_text(encoding="utf-8") == scorecard_markdown(
        report
    )
    assert paths["checksum"].is_file()
    assert artifacts["sha256"] == hashlib.sha256(frozen_json).hexdigest()


def test_delivery_refuses_future_report_date(reporting_root) -> None:
    root, window, generated_ns = reporting_root
    state_dir, artifact_dir = _reporting_paths(root)
    future = window_for_date(window.report_date + timedelta(days=1))
    client = RecordingClient()

    with pytest.raises(ScorecardError, match="incomplete report date"):
        run_delivery(
            root=root,
            state_dir=state_dir,
            artifact_dir=artifact_dir,
            window=future,
            keychain_service="unused",
            keychain_account="unused",
            now_wall_ns=generated_ns,
            client=client,
        )
    assert client.messages == []
    assert not state_dir.exists()
    assert not artifact_dir.exists()


@pytest.mark.parametrize(
    ("state_suffix", "artifact_suffix"),
    [
        (("reporting", "wrong-state"), ("reporting", "reports")),
        (("reporting", "state"), ("instances", "d2-btc", "data")),
    ],
)
def test_delivery_output_is_contained_to_reporting_home(
    reporting_root,
    state_suffix: tuple[str, ...],
    artifact_suffix: tuple[str, ...],
) -> None:
    root, window, generated_ns = reporting_root
    state_dir = root.joinpath(*state_suffix)
    artifact_dir = root.joinpath(*artifact_suffix)

    with pytest.raises(ScorecardError, match="directory must be exactly"):
        run_delivery(
            root=root,
            state_dir=state_dir,
            artifact_dir=artifact_dir,
            window=window,
            keychain_service="unused",
            keychain_account="unused",
            now_wall_ns=generated_ns,
            client=RecordingClient(),
        )


def test_missing_source_is_reported_without_falling_back_to_legacy(
    reporting_root,
) -> None:
    root, window, generated_ns = reporting_root
    approved = root / "instances/d2-btc/data/shadow-diagnostic-bounded-v1-test.db"
    legacy = root / "instances/d2-btc/data/shadow.db"
    approved.rename(legacy)

    report = build_scorecard(root=root, window=window, generated_wall_ns=generated_ns)

    btc = report["D2"]["instances"][0]
    assert btc["available"] is False
    assert report["safety"]["verified"] is False
    assert "SOURCE ERROR" in json.dumps(slack_message(report), ensure_ascii=False)
