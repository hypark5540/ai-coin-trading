from __future__ import annotations

import dataclasses
import importlib.util
import json
import os
import sqlite3
import sys
from pathlib import Path

import pytest

from coinpilot.config import AppConfig
from coinpilot.hft_depth import PublicOrderBook
from coinpilot.hft_shadow import ShadowConfig, ShadowIntent
from coinpilot.hft_shadow_signal import DiagnosticSnapshot
from coinpilot.hft_shadow_store import ShadowStore


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "coinpilot_bounded_shadow.py"
SPEC = importlib.util.spec_from_file_location("coinpilot_bounded_shadow", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
bounded = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = bounded
SPEC.loader.exec_module(bounded)

WALL_NS = 1_800_000_000_000_000_000


def _app_config(tmp_path: Path) -> AppConfig:
    base = AppConfig()
    return dataclasses.replace(
        base,
        shadow=dataclasses.replace(
            base.shadow,
            mode="diagnostic",
            database_path=str(tmp_path / "shadow.db"),
            archive_root=str(tmp_path / "raw"),
            initial_cash=5_000_000.0,
            order_quote=25_000.0,
            warmup_books=1,
            entry_threshold=0.30,
            max_spread_bps=10.0,
            max_daily_loss_pct=0.10,
            max_drawdown_pct=0.10,
            model_version="diagnostic-bounded-v1",
        ),
    ).validate()


def _book(
    ordinal: int,
    monotonic_ns: int,
    *,
    ask: float = 100.05,
    bid: float = 100.0,
) -> PublicOrderBook:
    return PublicOrderBook(
        capture_id="capture",
        connection_id="connection",
        ordinal=ordinal,
        market="KRW-BTC",
        received_monotonic_ns=monotonic_ns,
        received_wall_ns=WALL_NS + monotonic_ns,
        exchange_timestamp_ms=(WALL_NS + monotonic_ns) // 1_000_000,
        asks=((ask, 100.0),),
        bids=((bid, 100.0),),
    )


def _snapshot(ordinal: int, *, signal: float = 0.8) -> DiagnosticSnapshot:
    book = _book(ordinal, ordinal * 100_000_000)
    return DiagnosticSnapshot(
        book=book,
        ready=True,
        warmup_books_seen=100,
        book_imbalance_l5=0.8,
        trade_flow=0.8,
        signal=signal,
        spread_bps=(book.best_ask.price - book.best_bid.price)
        / book.mid_price
        * 10_000.0,
        model_version="diagnostic-bounded-v1",
    )


def _status(equity: float = 5_000_000.0) -> dict[str, object]:
    return {
        "base_quantity": 0.0,
        "pending_order": None,
        "last_equity_quote": equity,
        "peak_equity_quote": 5_000_000.0,
        "max_drawdown": max(0.0, 1.0 - equity / 5_000_000.0),
        "lifecycle_status": "running",
    }


def _settings(tmp_path: Path, *, max_round_trips: int = 24):
    return bounded.BoundedSettings(
        cooldown_seconds=3_600.0,
        max_round_trips_per_day=max_round_trips,
        reserve_full_order_loss=True,
        execution_spread_recheck=True,
        audit_dir=tmp_path / "audit",
    ).validate()


def test_runner_requires_exact_ten_percent_hard_stops(tmp_path: Path) -> None:
    config = _app_config(tmp_path)
    bounded._validate_ten_percent_policy(config)
    with pytest.raises(ValueError, match="must be exactly 0.10"):
        bounded._validate_ten_percent_policy(
            dataclasses.replace(
                config,
                shadow=dataclasses.replace(
                    config.shadow,
                    max_daily_loss_pct=0.01,
                ),
            ).validate()
        )


def test_daily_turnover_limit_is_read_from_the_durable_ledger(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _app_config(tmp_path)
    store = ShadowStore(config.shadow.database_path)
    bounded._SETTINGS = _settings(tmp_path)
    monkeypatch.setattr(
        bounded,
        "_activity_since",
        lambda *_args, **_kwargs: (
            24,
            WALL_NS - 10_000_000_000,
            24,
            WALL_NS - 20_000_000_000,
        ),
    )
    policy = bounded.BoundedDiagnosticPolicy(config, store)
    assert policy.intent(_snapshot(1), _status()) is None


def test_full_order_reserve_prevents_a_ten_percent_overshoot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _app_config(tmp_path)
    store = ShadowStore(config.shadow.database_path)
    bounded._SETTINGS = _settings(tmp_path)
    monkeypatch.setattr(
        bounded,
        "_activity_since",
        lambda *_args, **_kwargs: (0, None, 0, None),
    )
    policy = bounded.BoundedDiagnosticPolicy(config, store)
    assert policy.intent(_snapshot(1), _status()) is not None
    assert policy.intent(_snapshot(2), _status(4_520_000.0)) is None
    assert policy._risk_halt_reason == "daily_loss_entry_reserve"


def test_execution_book_spread_is_rechecked_before_simulated_fill(
    tmp_path: Path,
) -> None:
    bounded._SETTINGS = _settings(tmp_path)
    bounded._MAX_EXECUTION_SPREAD_BPS = 10.0
    store = ShadowStore(tmp_path / "execution.db")
    engine, _ = bounded.BoundedShadowEngine.start(
        store,
        ShadowConfig(
            market="KRW-BTC",
            warmup_books=1,
            latency_ns=50,
            max_book_gap_ns=1_000,
        ),
        run_id="bounded-run",
        started_wall_ns=WALL_NS - 1_000,
        code_version="test",
    )
    created = engine.process_book(
        _book(1, 100),
        ShadowIntent(
            action="buy",
            reason="diagnostic_entry",
            policy_version="diagnostic-bounded-v1",
            quote_notional=100.0,
        ),
    )
    assert created.order_id is not None
    resolved = engine.process_book(
        _book(2, 150, ask=101.0, bid=100.0)
    )
    assert resolved.fill_id is None
    with sqlite3.connect(store.path) as connection:
        order = connection.execute(
            "SELECT status, terminal_reason FROM shadow_orders WHERE order_id = ?",
            (created.order_id,),
        ).fetchone()
    assert order == ("expired", "execution_spread_too_wide")
    assert store.recent_fills(engine.run_id) == []
    activity = bounded._activity_since(
        store,
        market="KRW-BTC",
        wall_ns=WALL_NS + 200,
    )
    assert activity[2] == 1
    assert activity[3] == WALL_NS + 100


def test_multilevel_vwap_impact_blocks_entry_with_narrow_top_spread(
    tmp_path: Path,
) -> None:
    bounded._SETTINGS = _settings(tmp_path)
    bounded._MAX_EXECUTION_SPREAD_BPS = 10.0
    store = ShadowStore(tmp_path / "depth-impact.db")
    engine, _ = bounded.BoundedShadowEngine.start(
        store,
        ShadowConfig(
            market="KRW-BTC",
            warmup_books=1,
            latency_ns=50,
            max_book_gap_ns=1_000,
        ),
        run_id="depth-run",
        started_wall_ns=WALL_NS - 1_000,
        code_version="test",
    )
    created = engine.process_book(
        _book(1, 100),
        ShadowIntent(
            action="buy",
            reason="diagnostic_entry",
            policy_version="diagnostic-bounded-v1",
            quote_notional=100.0,
        ),
    )
    assert created.order_id is not None
    depth_book = dataclasses.replace(
        _book(2, 150),
        asks=((100.05, 0.1), (102.0, 100.0)),
    )
    resolved = engine.process_book(depth_book)
    assert resolved.fill_id is None
    with sqlite3.connect(store.path) as connection:
        order = connection.execute(
            "SELECT status, terminal_reason FROM shadow_orders WHERE order_id = ?",
            (created.order_id,),
        ).fetchone()
    assert order == ("expired", "execution_spread_too_wide")


def test_bounded_safety_profile_cannot_be_weakened(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    bounded._validate_safety_profile(settings)
    unsafe = (
        dataclasses.replace(settings, cooldown_seconds=0.0),
        dataclasses.replace(settings, max_round_trips_per_day=25),
        dataclasses.replace(settings, reserve_full_order_loss=False),
        dataclasses.replace(settings, execution_spread_recheck=False),
    )
    for candidate in unsafe:
        with pytest.raises(ValueError):
            bounded._validate_safety_profile(candidate)


def test_failed_entry_attempt_uses_durable_retry_backoff(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _app_config(tmp_path)
    store = ShadowStore(config.shadow.database_path)
    bounded._SETTINGS = _settings(tmp_path)
    monkeypatch.setattr(
        bounded,
        "_activity_since",
        lambda *_args, **_kwargs: (
            0,
            None,
            1,
            WALL_NS - 10_000_000_000,
        ),
    )
    policy = bounded.BoundedDiagnosticPolicy(config, store)
    assert policy.intent(_snapshot(1), _status()) is None


def _completed_round_trip(
    engine: bounded.BoundedShadowEngine,
    *,
    wall_base: int,
) -> None:
    buy = ShadowIntent(
        action="buy",
        reason="diagnostic_entry",
        policy_version="diagnostic-bounded-v1",
        quote_notional=100.0,
    )
    sell = ShadowIntent(
        action="sell",
        reason="diagnostic_signal_exit",
        policy_version="diagnostic-bounded-v1",
        base_quantity=0.9995002498750625,
    )
    first = dataclasses.replace(
        _book(1, 100),
        received_wall_ns=wall_base + 100,
    )
    second = dataclasses.replace(
        _book(2, 200),
        received_wall_ns=wall_base + 200,
    )
    engine.process_book(first, buy)
    buy_fill = engine.process_book(second)
    quantity = engine.status()["base_quantity"]
    assert buy_fill.fill_id is not None and quantity > 0
    sell = dataclasses.replace(sell, base_quantity=float(quantity))
    third = dataclasses.replace(
        _book(3, 300),
        received_wall_ns=wall_base + 300,
    )
    fourth = dataclasses.replace(
        _book(4, 400),
        received_wall_ns=wall_base + 400,
    )
    engine.process_book(third, sell)
    sell_fill = engine.process_book(fourth)
    assert sell_fill.fill_id is not None


def test_cooldown_survives_kst_midnight_and_lineage_restart(
    tmp_path: Path,
) -> None:
    bounded._SETTINGS = _settings(tmp_path)
    bounded._MAX_EXECUTION_SPREAD_BPS = 20.0
    store = ShadowStore(tmp_path / "midnight.db")
    engine, _ = bounded.BoundedShadowEngine.start(
        store,
        ShadowConfig(
            market="KRW-BTC",
            warmup_books=1,
            latency_ns=50,
            max_book_gap_ns=1_000,
        ),
        run_id="run-a",
        started_wall_ns=WALL_NS - 10_000,
        code_version="test",
    )
    next_day = bounded._kst_day_start_ns(WALL_NS) + 86_400_000_000_000
    _completed_round_trip(engine, wall_base=next_day - 1_000)
    engine.stop(reason="test", ended_wall_ns=next_day - 500)
    successor, _ = bounded.BoundedShadowEngine.start(
        store,
        engine.config,
        run_id="run-b",
        started_wall_ns=next_day + 100,
        code_version="test",
    )
    activity = bounded._activity_since(
        store,
        market="KRW-BTC",
        wall_ns=next_day + 1_000,
    )
    assert activity[0] == 0
    assert activity[1] == next_day - 600
    assert activity[2] == 0
    assert activity[3] == next_day - 900
    summary = bounded._fill_summary(store, successor.run_id)
    assert summary["fills"] == 2
    assert summary["sells"] == 1


def test_drawdown_audit_uses_peak_and_is_recoverable(tmp_path: Path) -> None:
    config = _app_config(tmp_path)
    settings = _settings(tmp_path)
    bounded._SETTINGS = settings
    store = ShadowStore(config.shadow.database_path)
    engine, _ = bounded.BoundedShadowEngine.start(
        store,
        ShadowConfig(
            market="KRW-BTC",
            initial_cash_quote=5_000_000.0,
            warmup_books=1,
        ),
        run_id="audit-run",
        started_wall_ns=WALL_NS - 1_000,
        code_version="test",
    )
    with store.write_transaction() as connection:
        connection.execute(
            """
            UPDATE shadow_state
            SET cash_quote = 5400000,
                last_equity_quote = 5400000,
                peak_equity_quote = 6000000,
                max_drawdown = 0.10,
                updated_wall_ns = ?
            WHERE run_id = ?
            """,
            (WALL_NS, engine.run_id),
        )
    engine.halt("max_drawdown", observed_wall_ns=WALL_NS + 1)
    audit_path = settings.audit_dir / "audit-run.halt-diagnostic.json"
    assert not audit_path.exists()
    assert bounded._materialize_missing_halt_audits(config, settings) == 1
    payload = json.loads(audit_path.read_text(encoding="utf-8"))
    assert payload["metrics"]["account_loss_quote"] == 0.0
    assert payload["metrics"]["drawdown_loss_quote"] == 600_000.0
    assert payload["metrics"]["drawdown_loss_pct"] == pytest.approx(0.10)
    assert payload["diagnosis"]["threshold_crossed"] is True
    assert payload["orders_sent"] == 0
    assert os.stat(audit_path).st_mode & 0o777 == 0o600
    assert bounded._materialize_missing_halt_audits(config, settings) == 0
