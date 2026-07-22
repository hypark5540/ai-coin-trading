from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from coinpilot.hft_shadow import ShadowConfig, ShadowEngine
from coinpilot.hft_shadow_store import ShadowStore
from coinpilot.shadow_ops import SlackDeliveryError
from scripts.coinpilot_hourly_notifier import (
    NANOSECONDS,
    NotifierSettings,
    SummaryWindow,
    aggregate_window,
    latest_completed_window,
    run_cycle,
    summary_slack_message,
)


HOUR = 3600 * NANOSECONDS
WINDOW_END = 1_800_003_600 * NANOSECONDS
WINDOW = SummaryWindow(WINDOW_END - HOUR, WINDOW_END)


class RecordingClient:
    def __init__(self) -> None:
        self.messages: list[dict[str, object]] = []

    def send(self, message) -> None:
        self.messages.append(dict(message))


class FailingClient:
    def send(self, message) -> None:
        raise SlackDeliveryError("rate limited", retry_after_seconds=17)


def _settings(path: Path) -> NotifierSettings:
    return NotifierSettings(
        config_path=path.parent / "config.toml",
        database_path=path,
        market="KRW-BTC",
        keychain_service="coinpilot-slack-webhook",
        keychain_account="coinpilot-shadow-btc",
    )


def _database(tmp_path: Path, *, with_trade: bool = True) -> Path:
    store = ShadowStore(tmp_path / "shadow.db")
    engine, _ = ShadowEngine.start(
        store,
        ShadowConfig(
            market="KRW-BTC",
            initial_cash_quote=5_000_000,
            warmup_books=1,
        ),
        run_id="run-hourly",
        started_wall_ns=WINDOW.start_wall_ns - 10 * NANOSECONDS,
        code_version="test",
    )
    with sqlite3.connect(store.path) as connection:
        connection.execute(
            "UPDATE shadow_runs SET status = 'running' WHERE run_id = ?",
            (engine.run_id,),
        )
        connection.execute(
            """
            UPDATE shadow_state
            SET lifecycle_status = 'running', last_equity_quote = 4999800,
                cash_quote = 4999800, updated_wall_ns = ?
            WHERE run_id = ?
            """,
            (WINDOW.end_wall_ns - NANOSECONDS, engine.run_id),
        )
        equity_rows = (
            (
                "equity-start",
                1,
                WINDOW.start_wall_ns - NANOSECONDS,
                5_000_000.0,
                0.0,
            ),
            (
                "equity-end",
                2,
                WINDOW.end_wall_ns - NANOSECONDS,
                4_999_800.0,
                0.00004,
            ),
        )
        for equity_id, ordinal, wall_ns, equity, drawdown in equity_rows:
            connection.execute(
                """
                INSERT INTO shadow_equity (
                    equity_id, run_id, capture_id, connection_id,
                    book_ordinal, book_monotonic_ns, book_wall_ns,
                    cash_quote, base_quantity, liquidation_bid,
                    liquidation_covered_base, equity_quote, peak_equity_quote,
                    drawdown, reason, created_wall_ns
                ) VALUES (?, ?, 'capture', 'connection', ?, ?, ?, ?, 0,
                          NULL, 0, ?, 5000000, ?, 'periodic', ?)
                """,
                (
                    equity_id,
                    engine.run_id,
                    ordinal,
                    ordinal,
                    wall_ns,
                    equity,
                    equity,
                    drawdown,
                    wall_ns,
                ),
            )
        if with_trade:
            fills = (
                (
                    "fill-buy",
                    "order-buy",
                    "buy",
                    125_000.0,
                    62.5,
                    100_000_000.0,
                    WINDOW.start_wall_ns + 10 * NANOSECONDS,
                ),
                (
                    "fill-sell",
                    "order-sell",
                    "sell",
                    124_900.0,
                    62.45,
                    99_920_000.0,
                    WINDOW.start_wall_ns + 20 * NANOSECONDS,
                ),
                (
                    "fill-next-window",
                    "order-next-window",
                    "buy",
                    125_000.0,
                    62.5,
                    100_000_000.0,
                    WINDOW.end_wall_ns,
                ),
            )
            for ordinal, (
                fill_id,
                order_id,
                side,
                quote,
                fee,
                price,
                wall_ns,
            ) in enumerate(fills, start=10):
                connection.execute(
                    """
                    INSERT INTO shadow_fills (
                        fill_id, order_id, run_id, capture_id, connection_id,
                        book_ordinal, book_monotonic_ns, book_wall_ns, side,
                        filled_base, filled_quote, fee_quote, vwap_price,
                        levels_consumed, execution_status, assumptions_json,
                        created_wall_ns
                    ) VALUES (?, ?, ?, 'capture', 'connection', ?, ?, ?, ?,
                              0.001, ?, ?, ?, 1, 'filled', '{}', ?)
                    """,
                    (
                        fill_id,
                        order_id,
                        engine.run_id,
                        ordinal,
                        ordinal,
                        wall_ns,
                        side,
                        quote,
                        fee,
                        price,
                        wall_ns,
                    ),
                )
    with store.write_transaction() as connection:
        store.enqueue_notification(
            connection,
            run_id=engine.run_id,
            alert_key="test-window-warning",
            topic="shadow_feed_continuity",
            severity="warning",
            payload={"reason": "test"},
            created_wall_ns=WINDOW.start_wall_ns + 30 * NANOSECONDS,
        )
    return store.path


def _outbox(path: Path) -> list[sqlite3.Row]:
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    try:
        return connection.execute(
            """
            SELECT topic, status, attempt_count, available_wall_ns,
                   payload_json, last_error
            FROM notification_outbox
            ORDER BY created_wall_ns, notification_id
            """
        ).fetchall()
    finally:
        connection.close()


def test_completed_window_is_fixed_and_honors_grace() -> None:
    before_grace = latest_completed_window(
        WINDOW_END + 14 * NANOSECONDS,
        grace_seconds=15,
    )
    after_grace = latest_completed_window(
        WINDOW_END + 15 * NANOSECONDS,
        grace_seconds=15,
    )

    assert before_grace.end_wall_ns == WINDOW_END - HOUR
    assert after_grace == WINDOW
    assert after_grace.label.endswith("KST")


def test_hourly_summary_aggregates_half_open_window_and_sends_once(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    client = RecordingClient()
    now = WINDOW.end_wall_ns + 16 * NANOSECONDS

    first = run_cycle(
        _settings(database),
        client,
        worker_id="worker-a",
        now_wall_ns=now,
    )
    second = run_cycle(
        _settings(database),
        client,
        worker_id="worker-b",
        now_wall_ns=now,
    )

    assert first["inserted"] == 1
    assert first["delivered"] == 1
    assert first["suppressed"] >= 2
    assert second["inserted"] == 0
    assert second["delivered"] == 0
    assert len(client.messages) == 1
    message = client.messages[0]
    assert "KRW-BTC" in str(message["text"])
    assert "simulated=true" in str(message["text"])
    assert "orders_sent=0" in str(message["text"])
    assert isinstance(message["blocks"], list)

    rows = _outbox(database)
    summaries = [row for row in rows if row["topic"] == "shadow_hourly_summary"]
    individual = [row for row in rows if row["topic"] != "shadow_hourly_summary"]
    assert len(summaries) == 1
    assert summaries[0]["status"] == "delivered"
    assert all(row["status"] == "dead_letter" for row in individual)
    assert all(
        row["last_error"] == "suppressed_by_hourly_summary_policy"
        for row in individual
    )

    payload = json.loads(summaries[0]["payload_json"])
    assert payload["fill_count"] == 2
    assert payload["buy_count"] == 1
    assert payload["sell_count"] == 1
    assert payload["completed_trades"] == 1
    assert payload["completed_pnl_quote"] == pytest.approx(-224.95)
    assert payload["fees_quote"] == pytest.approx(124.95)
    assert payload["equity_change_quote"] == pytest.approx(-200.0)
    assert payload["warning_events"] >= 1
    assert payload["orders_sent"] == 0


def test_zero_trade_window_still_formats_a_short_heartbeat(tmp_path: Path) -> None:
    database = _database(tmp_path, with_trade=False)
    with sqlite3.connect(database) as connection:
        connection.row_factory = sqlite3.Row
        summary = aggregate_window(
            connection,
            market="KRW-BTC",
            window=WINDOW,
        )

    message = summary_slack_message(summary)

    assert summary["fill_count"] == 0
    assert "체결 없음" in message["text"]
    assert "실주문 0건" in json.dumps(message["blocks"], ensure_ascii=False)


def test_failed_summary_is_retried_after_individual_alerts_are_suppressed(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    now = WINDOW.end_wall_ns + 16 * NANOSECONDS

    result = run_cycle(
        _settings(database),
        FailingClient(),
        worker_id="worker-fail",
        now_wall_ns=now,
    )

    assert result["failed"] == 1
    rows = _outbox(database)
    summary = next(row for row in rows if row["topic"] == "shadow_hourly_summary")
    assert summary["status"] == "pending"
    assert summary["attempt_count"] == 1
    assert summary["available_wall_ns"] == now + 17 * NANOSECONDS
    assert summary["last_error"] == "SlackDeliveryError"
    assert all(
        row["status"] == "dead_letter"
        for row in rows
        if row["topic"] != "shadow_hourly_summary"
    )
