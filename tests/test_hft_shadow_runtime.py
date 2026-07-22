from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pytest

from coinpilot.hft_depth import PublicOrderBook
from coinpilot.hft_shadow import (
    LIVE_ORDER_ROUTING_SUPPORTED,
    ShadowConfig,
    ShadowEngine,
    ShadowIntent,
)
from coinpilot.hft_shadow_store import ShadowInvariantError, ShadowStore


WALL_BASE = 1_800_000_000_000_000_000


def _book(
    ordinal: int,
    monotonic_ns: int,
    *,
    capture_id: str = "capture-a",
    connection_id: str = "connection-a",
    asks: tuple[tuple[float, float], ...] = (
        (100.0, 1.0),
        (101.0, 1.0),
    ),
    bids: tuple[tuple[float, float], ...] = (
        (99.0, 2.0),
        (98.0, 2.0),
    ),
    gap_before: bool = False,
    gap_reason: str | None = None,
) -> PublicOrderBook:
    return PublicOrderBook(
        capture_id=capture_id,
        connection_id=connection_id,
        ordinal=ordinal,
        market="KRW-BTC",
        received_monotonic_ns=monotonic_ns,
        received_wall_ns=WALL_BASE + monotonic_ns,
        exchange_timestamp_ms=(WALL_BASE + monotonic_ns) // 1_000_000,
        asks=asks,
        bids=bids,
        gap_before=gap_before,
        gap_reason=gap_reason,
    )


def _config(**overrides: object) -> ShadowConfig:
    values: dict[str, object] = {
        "warmup_books": 1,
        "latency_ns": 50,
        "max_book_gap_ns": 100,
        "fee_rate": 0.001,
        "equity_sample_interval_ns": 5_000,
        "health_sample_interval_ns": 10_000,
    }
    values.update(overrides)
    return ShadowConfig(**values)  # type: ignore[arg-type]


def _engine(
    tmp_path: Path,
    *,
    run_id: str = "run-a",
    started_wall_ns: int = WALL_BASE - 1_000,
    config: ShadowConfig | None = None,
) -> tuple[ShadowStore, ShadowEngine]:
    store = ShadowStore(tmp_path / "shadow.db")
    engine, _ = ShadowEngine.start(
        store,
        config or _config(),
        run_id=run_id,
        started_wall_ns=started_wall_ns,
        code_version="test",
    )
    return store, engine


def _row(path: Path, query: str, parameters: tuple[object, ...] = ()) -> sqlite3.Row:
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    try:
        selected = connection.execute(query, parameters).fetchone()
    finally:
        connection.close()
    assert selected is not None
    return selected


def test_shadow_only_wal_partial_latency_and_idempotency(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="only mode='shadow'"):
        ShadowConfig(mode="live").validate()
    assert LIVE_ORDER_ROUTING_SUPPORTED is False

    store, engine = _engine(tmp_path)
    assert os.stat(store.path).st_mode & 0o777 == 0o600
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"

    first = engine.process_book(
        _book(1, 100),
        ShadowIntent(
            action="buy",
            reason="entry",
            policy_version="policy-v1",
            signal=0.9,
            quote_notional=250.0,
            features={"imbalance": 0.5},
        ),
    )
    assert first.lifecycle_status == "running"
    assert first.decision_status == "accepted"
    assert first.order_id is not None
    assert engine.status()["pending_order"]["order_id"] == first.order_id

    before_due = engine.process_book(_book(2, 149))
    assert before_due.fill_id is None
    assert before_due.base_quantity == 0

    # First book after the deadline has only 150.5 quote of visible asks.
    filled = engine.process_book(
        _book(
            3,
            150,
            asks=((100.0, 1.0), (101.0, 0.5)),
            bids=((99.0, 4.0),),
        )
    )
    assert filled.resolved_order_id == first.order_id
    assert filled.fill_id is not None
    assert filled.base_quantity == pytest.approx(1.5)
    assert filled.cash_quote == pytest.approx(10_000_000.0 - 150.6505)

    order = _row(
        store.path,
        """
        SELECT status, remaining_quote, terminal_reason
        FROM shadow_orders WHERE order_id = ?
        """,
        (first.order_id,),
    )
    assert order["status"] == "partially_filled"
    assert order["remaining_quote"] == pytest.approx(99.5)
    assert order["terminal_reason"] == (
        "visible_depth_exhausted_remainder_cancelled"
    )

    counts = store.table_counts(engine.run_id)
    duplicate = engine.process_book(
        _book(
            3,
            150,
            asks=((100.0, 1.0), (101.0, 0.5)),
            bids=((99.0, 4.0),),
        )
    )
    assert duplicate.duplicate_event is True
    assert store.table_counts(engine.run_id) == counts
    status = engine.status()
    assert status["mode"] == "shadow"
    assert status["live_order_routing"] is False
    assert status["orders_sent"] == 0
    assert status["pending_order"] is None


def test_gap_expires_pending_and_rewarms_without_a_fill(tmp_path: Path) -> None:
    store, engine = _engine(tmp_path)
    created = engine.process_book(
        _book(1, 100),
        ShadowIntent(
            action="buy",
            reason="entry",
            policy_version="policy-v1",
            quote_notional=100.0,
        ),
    )
    assert created.order_id is not None

    gap = engine.process_book(
        _book(
            2,
            200,
            gap_before=True,
            gap_reason="websocket_reconnect",
        )
    )
    assert gap.continuity_reason == "gap:websocket_reconnect"
    assert gap.lifecycle_status == "warmup"
    assert store.recent_fills(engine.run_id) == []
    order = _row(
        store.path,
        "SELECT status, terminal_reason FROM shadow_orders WHERE order_id = ?",
        (created.order_id,),
    )
    assert order["status"] == "expired"
    assert order["terminal_reason"] == (
        "continuity:gap:websocket_reconnect"
    )
    ready = engine.process_book(_book(3, 300))
    assert ready.lifecycle_status == "running"


def test_receive_interval_silence_keeps_runtime_ready_but_expires_stale_order(
    tmp_path: Path,
) -> None:
    store, engine = _engine(tmp_path)
    created = engine.process_book(
        _book(1, 100),
        ShadowIntent(
            action="buy",
            reason="entry",
            policy_version="policy-v1",
            quote_notional=100.0,
        ),
    )
    assert created.order_id is not None

    after_silence = engine.process_book(
        _book(
            2,
            300,
            gap_before=True,
            gap_reason="receive_interval",
        )
    )

    assert after_silence.continuity_reason is None
    assert after_silence.lifecycle_status == "running"
    assert store.recent_fills(engine.run_id) == []
    order = _row(
        store.path,
        "SELECT status, terminal_reason FROM shadow_orders WHERE order_id = ?",
        (created.order_id,),
    )
    assert order["status"] == "expired"
    assert order["terminal_reason"] == "no_fresh_book_after_latency"
    status = engine.status()
    assert status["warmup_books_seen"] == 1
    assert status["orders_sent"] == 0


def test_restart_expires_pending_and_open_position_liquidates_fresh(
    tmp_path: Path,
) -> None:
    store, first = _engine(tmp_path, run_id="run-1")
    buy = first.process_book(
        _book(1, 100),
        ShadowIntent(
            action="buy",
            reason="entry",
            policy_version="policy-v1",
            quote_notional=100.0,
        ),
    )
    assert buy.order_id is not None
    first.process_book(_book(2, 150, asks=((100.0, 5.0),)))
    assert first.status()["base_quantity"] > 0
    first.stop(reason="sigterm", ended_wall_ns=WALL_BASE + 200)

    second, start = ShadowEngine.start(
        store,
        _config(),
        run_id="run-2",
        started_wall_ns=WALL_BASE + 300,
        code_version="test",
    )
    assert start.restarted_from_run_id == "run-1"
    assert start.lifecycle_status == "halted_recovery"

    recovery = second.process_book(
        _book(
            1,
            1_000,
            capture_id="capture-b",
            connection_id="connection-b",
            bids=((97.0, 10.0),),
        )
    )
    assert recovery.decision_status == "accepted"
    assert recovery.fill_id is not None
    assert recovery.base_quantity == 0
    assert recovery.lifecycle_status == "warmup"
    fill = store.recent_fills(second.run_id)[0]
    assert fill["book_ordinal"] == 1
    decision = _row(
        store.path,
        """
        SELECT reason, policy_version FROM shadow_decisions
        WHERE decision_id = ?
        """,
        (recovery.decision_id,),
    )
    assert decision["reason"] == "gap_recovery_liquidation"
    assert decision["policy_version"] == "system-recovery-v1"
    assert second.process_book(
        _book(
            2,
            1_100,
            capture_id="capture-b",
            connection_id="connection-b",
        )
    ).lifecycle_status == "running"
    assert store.latest_run_id("krw-btc") == "run-2"
    assert store.read_latest_status()["orders_sent"] == 0


def test_fill_and_outbox_are_one_transaction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, engine = _engine(tmp_path)
    created = engine.process_book(
        _book(1, 100),
        ShadowIntent(
            action="buy",
            reason="entry",
            policy_version="policy-v1",
            quote_notional=100.0,
        ),
    )
    original = store.enqueue_notification

    def fail_fill_alert(*args: object, **kwargs: object) -> str:
        if kwargs.get("topic") == "shadow_fill":
            raise RuntimeError("alert storage unavailable")
        return original(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(store, "enqueue_notification", fail_fill_alert)
    with pytest.raises(RuntimeError, match="alert storage"):
        engine.process_book(_book(2, 150))

    assert store.recent_fills(engine.run_id) == []
    assert engine.status()["base_quantity"] == 0
    assert engine.status()["pending_order"]["order_id"] == created.order_id

    monkeypatch.setattr(store, "enqueue_notification", original)
    assert engine.process_book(_book(2, 150)).fill_id is not None


def test_outbox_leases_redacts_failures_and_halt_is_durable(
    tmp_path: Path,
) -> None:
    store, engine = _engine(tmp_path)
    engine.process_book(
        _book(1, 100),
        ShadowIntent(
            action="buy",
            reason="entry",
            policy_version="policy-v1",
            quote_notional=100.0,
        ),
    )
    expired = engine.halt("disk_low", observed_wall_ns=WALL_BASE + 150)
    assert expired == 1
    assert engine.status()["lifecycle_status"] == "halted_recovery"

    rejected = engine.process_book(
        _book(2, 200),
        ShadowIntent(
            action="buy",
            reason="must_not_resume",
            policy_version="policy-v1",
            quote_notional=100.0,
        ),
    )
    assert rejected.decision_status == "rejected"
    assert rejected.lifecycle_status == "halted_recovery"

    alert_id = store.enqueue_alert(
        run_id=engine.run_id,
        alert_key="watchdog:test",
        topic="watchdog",
        severity="critical",
        payload={"kind": "deadman"},
        created_wall_ns=WALL_BASE + 300,
    )
    # Repeating an identical alert is exactly idempotent.
    assert store.enqueue_alert(
        run_id=engine.run_id,
        alert_key="watchdog:test",
        topic="watchdog",
        severity="critical",
        payload={"kind": "deadman"},
        created_wall_ns=WALL_BASE + 300,
    ) == alert_id

    claimed = store.claim_notifications(
        worker_id="worker-a",
        now_wall_ns=WALL_BASE + 1_000,
        lease_ns=100,
        limit=100,
    )
    assert claimed
    assert store.claim_notifications(
        worker_id="worker-b",
        now_wall_ns=WALL_BASE + 1_050,
        lease_ns=100,
        limit=100,
    ) == []
    selected = next(item for item in claimed if item["notification_id"] == alert_id)
    fake_hook = "/".join(
        ("https://hooks.slack.com", "services", "T", "B", "SECRET")
    )
    store.record_notification_failure(
        selected["notification_id"],
        worker_id="worker-a",
        now_wall_ns=WALL_BASE + 1_050,
        retry_at_wall_ns=WALL_BASE + 1_200,
        error=f"POST {fake_hook} Bearer xoxb-secret failed",
    )
    persisted = _row(
        store.path,
        """
        SELECT status, attempt_count, last_error
        FROM notification_outbox WHERE notification_id = ?
        """,
        (alert_id,),
    )
    assert persisted["status"] == "pending"
    assert persisted["attempt_count"] == 1
    assert "SECRET" not in persisted["last_error"]
    assert "xoxb-secret" not in persisted["last_error"]
    reclaimed = store.claim_notifications(
        worker_id="worker-b",
        now_wall_ns=WALL_BASE + 1_300,
        lease_ns=100,
        limit=100,
    )
    reclaimed_alert = next(
        item for item in reclaimed if item["notification_id"] == alert_id
    )
    store.mark_notification_delivered(
        reclaimed_alert["notification_id"],
        worker_id="worker-b",
        delivered_wall_ns=WALL_BASE + 1_350,
    )
    # Mutable delivery state does not break alert-key idempotency.
    assert store.enqueue_alert(
        run_id=engine.run_id,
        alert_key="watchdog:test",
        topic="watchdog",
        severity="critical",
        payload={"kind": "deadman"},
        created_wall_ns=WALL_BASE + 300,
    ) == alert_id

    engine.stop(reason="sigterm", ended_wall_ns=WALL_BASE + 1_400)
    restarted, start = ShadowEngine.start(
        store,
        _config(),
        run_id="run-after-halt",
        started_wall_ns=WALL_BASE + 1_500,
    )
    assert start.lifecycle_status == "halted_recovery"
    assert restarted.status()["halt_reason"] == "external_halt:disk_low"


def test_periodic_rows_are_bounded_but_decisions_force_equity(
    tmp_path: Path,
) -> None:
    store, engine = _engine(
        tmp_path,
        config=_config(
            warmup_books=1,
            equity_sample_interval_ns=5_000,
            health_sample_interval_ns=10_000,
        ),
    )
    engine.process_book(_book(1, 100))
    for ordinal in range(2, 102):
        engine.process_book(_book(ordinal, 100 + ordinal))
    counts = store.table_counts(engine.run_id)
    assert counts["shadow_equity"] == 1
    assert counts["shadow_health"] == 1

    decision = engine.process_book(
        _book(102, 300),
        ShadowIntent(
            action="hold",
            reason="diagnostic",
            policy_version="policy-v1",
            signal=0.0,
        ),
    )
    assert decision.decision_status == "hold"
    assert store.table_counts(engine.run_id)["shadow_equity"] == 2

    engine.process_book(_book(103, 10_200))
    counts = store.table_counts(engine.run_id)
    assert counts["shadow_equity"] == 3
    assert counts["shadow_health"] == 2


def test_restart_with_changed_fingerprint_cannot_auto_resume(
    tmp_path: Path,
) -> None:
    store, first = _engine(tmp_path, run_id="fingerprint-1")
    first.process_book(_book(1, 100))
    first.stop(reason="deploy", ended_wall_ns=WALL_BASE + 150)
    changed, start = ShadowEngine.start(
        store,
        _config(fee_rate=0.002),
        run_id="fingerprint-2",
        started_wall_ns=WALL_BASE + 200,
        code_version="test-v2",
    )
    assert start.lifecycle_status == "halted_recovery"
    assert changed.status()["halt_reason"] == "restart_fingerprint_changed"
    assert changed.process_book(_book(1, 1_000)).lifecycle_status == (
        "halted_recovery"
    )


def test_external_halt_survives_feed_gap_and_repeated_halt_is_idempotent(
    tmp_path: Path,
) -> None:
    store, engine = _engine(tmp_path)
    engine.process_book(_book(1, 100))
    engine.halt("daily_loss", observed_wall_ns=WALL_BASE + 150)
    before = store.table_counts(engine.run_id)

    gap = engine.process_book(
        _book(
            2,
            200,
            gap_before=True,
            gap_reason="websocket_reconnect",
        )
    )
    assert gap.lifecycle_status == "halted_recovery"
    assert engine.status()["halt_reason"] == "external_halt:daily_loss"

    next_book = engine.process_book(_book(3, 300))
    assert next_book.lifecycle_status == "halted_recovery"
    assert engine.status()["halt_reason"] == "external_halt:daily_loss"

    assert engine.halt("daily_loss", observed_wall_ns=WALL_BASE + 350) == 0
    after = store.table_counts(engine.run_id)
    assert after["notification_outbox"] == before["notification_outbox"] + 1


def test_deterministic_source_collision_is_rejected(tmp_path: Path) -> None:
    _, engine = _engine(tmp_path)
    intent = ShadowIntent(
        action="hold",
        reason="first",
        policy_version="policy-v1",
    )
    engine.process_book(_book(1, 100), intent)
    with pytest.raises(ShadowInvariantError, match="different policy intent"):
        engine.process_book(
            _book(1, 100),
            ShadowIntent(
                action="hold",
                reason="changed",
                policy_version="policy-v1",
            ),
        )
    with pytest.raises(ShadowInvariantError, match="ordinal"):
        engine.process_book(
            _book(1, 101, asks=((500.0, 1.0),), bids=((499.0, 1.0),))
        )
