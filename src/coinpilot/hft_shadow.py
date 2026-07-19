"""Stateful, public-market-data-only HFT shadow execution runtime.

This module cannot route a live order.  It accepts normalized public order-book
events, waits for a receive-monotonic latency deadline, and then simulates a
taker fill against the first later continuity-safe visible book.  Decisions,
orders, fills, account state, equity, health, and alert intents are committed to
SQLite as one event transaction.
"""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import time
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

from coinpilot.hft_depth import (
    IndependentTakerOrder,
    PublicOrderBook,
    sweep_visible_depth,
)
from coinpilot.hft_shadow_store import (
    ShadowInvariantError,
    ShadowRunStart,
    ShadowStore,
    canonical_json,
    deterministic_shadow_id,
)


LIVE_ORDER_ROUTING_SUPPORTED = False
SHADOW_MODE = "shadow"

ShadowAction = Literal["buy", "sell", "hold"]


def _finite(value: Any, field_name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{field_name} must be numeric") from exc
    if not math.isfinite(result):
        raise ValueError(f"{field_name} must be finite")
    return result


def _positive(value: Any, field_name: str) -> float:
    result = _finite(value, field_name)
    if result <= 0:
        raise ValueError(f"{field_name} must be positive")
    return result


def _nonnegative_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return value


def _text(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be non-empty")
    return value.strip()


@dataclass(frozen=True, slots=True)
class ShadowConfig:
    """Immutable execution and account guardrails for one shadow run."""

    market: str = "KRW-BTC"
    initial_cash_quote: float = 10_000_000.0
    fee_rate: float = 0.0005
    latency_ns: int = 50_000_000
    max_book_gap_ns: int = 1_000_000_000
    warmup_books: int = 10
    max_order_quote: float = 250_000.0
    equity_sample_interval_ns: int = 5_000_000_000
    health_sample_interval_ns: int = 10_000_000_000
    one_position_only: bool = True
    position_epsilon: float = 1e-12
    mode: str = SHADOW_MODE

    def validate(self) -> ShadowConfig:
        market = _text(self.market, "market").upper()
        if len(market.split("-")) != 2:
            raise ValueError("market must use QUOTE-BASE form")
        initial_cash = _positive(
            self.initial_cash_quote, "initial_cash_quote"
        )
        fee = _finite(self.fee_rate, "fee_rate")
        if not 0 <= fee < 1:
            raise ValueError("fee_rate must be in [0, 1)")
        latency = _nonnegative_int(self.latency_ns, "latency_ns")
        gap = _nonnegative_int(self.max_book_gap_ns, "max_book_gap_ns")
        if gap <= 0:
            raise ValueError("max_book_gap_ns must be positive")
        warmup = _nonnegative_int(self.warmup_books, "warmup_books")
        if warmup <= 0:
            raise ValueError("warmup_books must be positive")
        max_order = _positive(self.max_order_quote, "max_order_quote")
        equity_interval = _nonnegative_int(
            self.equity_sample_interval_ns,
            "equity_sample_interval_ns",
        )
        health_interval = _nonnegative_int(
            self.health_sample_interval_ns,
            "health_sample_interval_ns",
        )
        if equity_interval <= 0 or health_interval <= 0:
            raise ValueError("sample intervals must be positive")
        epsilon = _positive(self.position_epsilon, "position_epsilon")
        if not isinstance(self.one_position_only, bool):
            raise ValueError("one_position_only must be boolean")
        if self.mode != SHADOW_MODE:
            raise ValueError(
                "only mode='shadow' is supported; live order routing is absent"
            )
        return ShadowConfig(
            market=market,
            initial_cash_quote=initial_cash,
            fee_rate=fee,
            latency_ns=latency,
            max_book_gap_ns=gap,
            warmup_books=warmup,
            max_order_quote=max_order,
            equity_sample_interval_ns=equity_interval,
            health_sample_interval_ns=health_interval,
            one_position_only=self.one_position_only,
            position_epsilon=epsilon,
            mode=SHADOW_MODE,
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self.validate())

    @property
    def config_hash(self) -> str:
        return hashlib.sha256(
            canonical_json(self.to_dict()).encode("utf-8")
        ).hexdigest()


@dataclass(frozen=True, slots=True)
class ShadowIntent:
    """One policy output computed only from the current and prior events."""

    action: ShadowAction
    reason: str
    policy_version: str
    signal: float | None = None
    quote_notional: float | None = None
    base_quantity: float | None = None
    features: Mapping[str, Any] = field(default_factory=dict)

    def validate(self) -> ShadowIntent:
        if self.action not in ("buy", "sell", "hold"):
            raise ValueError("action must be buy, sell, or hold")
        reason = _text(self.reason, "reason")
        policy_version = _text(self.policy_version, "policy_version")
        signal = None if self.signal is None else _finite(self.signal, "signal")
        quote = (
            None
            if self.quote_notional is None
            else _positive(self.quote_notional, "quote_notional")
        )
        base = (
            None
            if self.base_quantity is None
            else _positive(self.base_quantity, "base_quantity")
        )
        if self.action == "buy" and (quote is None or base is not None):
            raise ValueError(
                "buy intent requires quote_notional and no base_quantity"
            )
        if self.action == "sell" and (base is None or quote is not None):
            raise ValueError(
                "sell intent requires base_quantity and no quote_notional"
            )
        if self.action == "hold" and (quote is not None or base is not None):
            raise ValueError("hold intent cannot contain an order quantity")
        features = dict(self.features)
        canonical_json(features)
        return ShadowIntent(
            action=self.action,
            reason=reason,
            policy_version=policy_version,
            signal=signal,
            quote_notional=quote,
            base_quantity=base,
            features=features,
        )


@dataclass(frozen=True, slots=True)
class ShadowStepResult:
    run_id: str
    lifecycle_status: str
    decision_id: str | None
    decision_status: str | None
    order_id: str | None
    resolved_order_id: str | None
    fill_id: str | None
    duplicate_event: bool
    ignored_event: bool
    continuity_reason: str | None
    cash_quote: float
    base_quantity: float
    equity_quote: float
    live_order_routing: bool = LIVE_ORDER_ROUTING_SUPPORTED


class ShadowEngine:
    """One stateful shadow account backed by :class:`ShadowStore`."""

    def __init__(
        self,
        store: ShadowStore,
        config: ShadowConfig,
        run_id: str,
    ) -> None:
        self.store = store
        self.config = config.validate()
        self.run_id = _text(run_id, "run_id")
        status = self.store.read_status(self.run_id)
        if status["mode"] != SHADOW_MODE:
            raise ShadowInvariantError("engine may only open a shadow run")
        if status["market"] != self.config.market:
            raise ShadowInvariantError("run market disagrees with config")
        if status["config_hash"] != self.config.config_hash:
            raise ShadowInvariantError("run config hash disagrees with config")

    @classmethod
    def start(
        cls,
        store: ShadowStore,
        config: ShadowConfig,
        *,
        run_id: str | None = None,
        started_wall_ns: int | None = None,
        code_version: str = "unknown",
    ) -> tuple[ShadowEngine, ShadowRunStart]:
        selected = config.validate()
        started = time.time_ns() if started_wall_ns is None else started_wall_ns
        if isinstance(started, bool) or not isinstance(started, int) or started <= 0:
            raise ValueError("started_wall_ns must be a positive integer")
        selected_run_id = run_id or deterministic_shadow_id(
            "run",
            selected.market,
            selected.config_hash,
            started,
        )
        result = store.start_run(
            run_id=selected_run_id,
            market=selected.market,
            initial_cash_quote=selected.initial_cash_quote,
            config=selected.to_dict(),
            config_hash=selected.config_hash,
            code_version=code_version,
            started_wall_ns=started,
            position_epsilon=selected.position_epsilon,
        )
        return cls(store, selected, selected_run_id), result

    @property
    def live_order_routing(self) -> Literal[False]:
        return False

    def status(self) -> dict[str, Any]:
        result = self.store.read_status(self.run_id)
        result["mode"] = SHADOW_MODE
        result["live_order_routing"] = False
        result["orders_sent"] = 0
        result["position_quantity"] = result["base_quantity"]
        result["last_feed_wall_ns"] = result["last_book_wall_ns"]
        return result

    def process_archive_record(
        self,
        record: Mapping[str, Any],
        intent: ShadowIntent | None = None,
    ) -> ShadowStepResult:
        """Consume one recorder schema-v1 envelope.

        Order-book events drive account state.  Public trade events are retained
        by the archive and only update feed health here; they can be incorporated
        into an external causal policy before a later book decision.
        """

        if not isinstance(record, Mapping):
            raise ValueError("archive record must be a mapping")
        if record.get("schema_version") != 1:
            raise ValueError("unsupported archive schema_version")
        event = record.get("event")
        if not isinstance(event, Mapping):
            raise ValueError("archive event must be a mapping")
        event_type = event.get("event_type")
        if event_type == "trade":
            if intent is not None:
                raise ValueError("an order intent must be anchored to an order book")
            state = self.store.get_state(self.run_id)
            return self._result_from_state(
                state,
                ignored_event=True,
            )
        if event_type != "orderbook":
            raise ValueError("archive event_type must be orderbook or trade")
        book = PublicOrderBook.from_archive_envelope(record)
        return self.process_book(book, intent)

    def process_book(
        self,
        book: PublicOrderBook,
        intent: ShadowIntent | None = None,
    ) -> ShadowStepResult:
        """Atomically apply one public book and optional policy intent."""

        if not isinstance(book, PublicOrderBook):
            raise TypeError("book must be a PublicOrderBook")
        if book.market != self.config.market:
            raise ValueError(
                f"book market {book.market} does not match {self.config.market}"
            )
        selected_intent = None if intent is None else intent.validate()
        event_wall_ns = self._event_wall_ns(book)

        with self.store.write_transaction() as connection:
            state_row = connection.execute(
                "SELECT * FROM shadow_state WHERE run_id = ?",
                (self.run_id,),
            ).fetchone()
            if state_row is None:
                raise ShadowInvariantError("shadow run state disappeared")
            state = dict(state_row)
            if state["lifecycle_status"] == "stopped":
                raise ShadowInvariantError("cannot process a stopped shadow run")

            duplicate = self._is_exact_duplicate(state, book)
            if duplicate:
                return self._duplicate_result(
                    connection,
                    state,
                    book,
                    selected_intent,
                )
            self._reject_regressed_or_colliding_book(state, book)

            continuity_reason = self._continuity_reason(state, book)
            resolved_order_id: str | None = None
            fill_id: str | None = None
            decision_id: str | None = None
            decision_status: str | None = None
            order_id: str | None = None
            recovery_event = False
            if continuity_reason is not None:
                expired = self._expire_pending(
                    connection,
                    reason=f"continuity:{continuity_reason}",
                    completed_wall_ns=event_wall_ns,
                )
                state["warmup_books_seen"] = 0
                if float(state["base_quantity"]) > self.config.position_epsilon:
                    state["lifecycle_status"] = "halted_recovery"
                    state["halt_reason"] = (
                        f"continuity_with_open_position:{continuity_reason}"
                    )
                else:
                    state["lifecycle_status"] = "warmup"
                    state["halt_reason"] = None
                connection.execute(
                    """
                    UPDATE shadow_runs
                    SET status = ?, halt_reason = ?
                    WHERE run_id = ?
                    """,
                    (
                        state["lifecycle_status"],
                        state["halt_reason"],
                        self.run_id,
                    ),
                )
                self.store.enqueue_notification(
                    connection,
                    run_id=self.run_id,
                    alert_key=(
                        f"continuity:{self.run_id}:{book.capture_id}:"
                        f"{book.connection_id}:{book.ordinal}"
                    ),
                    topic="shadow_feed_continuity",
                    severity=(
                        "critical"
                        if state["lifecycle_status"] == "halted_recovery"
                        else "warning"
                    ),
                    payload={
                        "run_id": self.run_id,
                        "reason": continuity_reason,
                        "expired_pending_orders": expired,
                        "lifecycle_status": state["lifecycle_status"],
                    },
                    created_wall_ns=event_wall_ns,
                )
            else:
                resolved_order_id, fill_id = self._resolve_pending(
                    connection,
                    state,
                    book,
                    event_wall_ns,
                )
                if (
                    state["lifecycle_status"] == "halted_recovery"
                    and self._is_automatic_recovery_halt(state)
                    and float(state["base_quantity"])
                    > self.config.position_epsilon
                ):
                    recovery_event = True
                    recovery_intent = ShadowIntent(
                        action="sell",
                        reason="gap_recovery_liquidation",
                        policy_version="system-recovery-v1",
                        base_quantity=float(state["base_quantity"]),
                        features={
                            "recovery_halt_reason": state["halt_reason"],
                            "live_order_routing": False,
                        },
                    )
                    (
                        decision_id,
                        decision_status,
                        order_id,
                    ) = self._record_intent(
                        connection,
                        state,
                        book,
                        recovery_intent,
                        event_wall_ns,
                        allow_recovery=True,
                        due_monotonic_ns=book.received_monotonic_ns,
                    )
                    resolved_order_id, fill_id = self._resolve_pending(
                        connection,
                        state,
                        book,
                        event_wall_ns,
                    )
                    if (
                        float(state["base_quantity"])
                        <= self.config.position_epsilon
                    ):
                        state["base_quantity"] = 0.0
                        state["average_cost_quote"] = 0.0
                        state["lifecycle_status"] = "warmup"
                        state["warmup_books_seen"] = 0
                        state["halt_reason"] = None
                        connection.execute(
                            """
                            UPDATE shadow_runs
                            SET status = 'warmup', halt_reason = NULL
                            WHERE run_id = ?
                            """,
                            (self.run_id,),
                        )
                if (
                    state["lifecycle_status"] == "warmup"
                    and not recovery_event
                ):
                    state["warmup_books_seen"] = (
                        int(state["warmup_books_seen"]) + 1
                    )
                    if (
                        int(state["warmup_books_seen"])
                        >= self.config.warmup_books
                    ):
                        state["lifecycle_status"] = "running"
                        state["halt_reason"] = None
                        connection.execute(
                            """
                            UPDATE shadow_runs
                            SET status = 'running', halt_reason = NULL
                            WHERE run_id = ?
                            """,
                            (self.run_id,),
                        )
                        self.store.enqueue_notification(
                            connection,
                            run_id=self.run_id,
                            alert_key=(
                                f"warmup-complete:{self.run_id}:"
                                f"{book.capture_id}:{book.connection_id}:"
                                f"{book.ordinal}"
                            ),
                            topic="shadow_ready",
                            severity="info",
                            payload={
                                "run_id": self.run_id,
                                "warmup_books": self.config.warmup_books,
                                "live_order_routing": False,
                            },
                            created_wall_ns=event_wall_ns,
                        )

            if selected_intent is not None and not recovery_event:
                (
                    decision_id,
                    decision_status,
                    order_id,
                ) = self._record_intent(
                    connection,
                    state,
                    book,
                    selected_intent,
                    event_wall_ns,
                )

            (
                equity_quote,
                liquidation_bid,
                liquidation_covered_base,
            ) = self._liquidation_equity(state, book)
            peak = max(float(state["peak_equity_quote"]), equity_quote)
            drawdown = 0.0 if peak <= 0 else max(0.0, 1.0 - equity_quote / peak)
            state["last_equity_quote"] = equity_quote
            state["peak_equity_quote"] = peak
            state["max_drawdown"] = max(
                float(state["max_drawdown"]), drawdown
            )
            state["last_capture_id"] = book.capture_id
            state["last_connection_id"] = book.connection_id
            state["last_book_ordinal"] = book.ordinal
            state["last_book_monotonic_ns"] = book.received_monotonic_ns
            state["last_book_wall_ns"] = book.received_wall_ns
            state["updated_wall_ns"] = event_wall_ns
            state["revision"] = int(state["revision"]) + 1

            connection.execute(
                """
                UPDATE shadow_state
                SET revision = ?,
                    lifecycle_status = ?,
                    cash_quote = ?,
                    base_quantity = ?,
                    average_cost_quote = ?,
                    realized_pnl_quote = ?,
                    cumulative_fees_quote = ?,
                    last_equity_quote = ?,
                    peak_equity_quote = ?,
                    max_drawdown = ?,
                    warmup_books_seen = ?,
                    last_capture_id = ?,
                    last_connection_id = ?,
                    last_book_ordinal = ?,
                    last_book_monotonic_ns = ?,
                    last_book_wall_ns = ?,
                    halt_reason = ?,
                    updated_wall_ns = ?
                WHERE run_id = ?
                """,
                (
                    state["revision"],
                    state["lifecycle_status"],
                    state["cash_quote"],
                    state["base_quantity"],
                    state["average_cost_quote"],
                    state["realized_pnl_quote"],
                    state["cumulative_fees_quote"],
                    state["last_equity_quote"],
                    state["peak_equity_quote"],
                    state["max_drawdown"],
                    state["warmup_books_seen"],
                    state["last_capture_id"],
                    state["last_connection_id"],
                    state["last_book_ordinal"],
                    state["last_book_monotonic_ns"],
                    state["last_book_wall_ns"],
                    state["halt_reason"],
                    state["updated_wall_ns"],
                    self.run_id,
                ),
            )
            if self._should_sample_equity(
                connection,
                book,
                force=(
                    fill_id is not None
                    or decision_id is not None
                    or continuity_reason is not None
                ),
            ):
                equity_id = deterministic_shadow_id(
                    "equity",
                    self.run_id,
                    book.capture_id,
                    book.connection_id,
                    book.ordinal,
                    "book",
                )
                self.store.insert_exact(
                    connection,
                    "shadow_equity",
                    "equity_id",
                    {
                        "equity_id": equity_id,
                        "run_id": self.run_id,
                        "capture_id": book.capture_id,
                        "connection_id": book.connection_id,
                        "book_ordinal": book.ordinal,
                        "book_monotonic_ns": book.received_monotonic_ns,
                        "book_wall_ns": book.received_wall_ns,
                        "cash_quote": state["cash_quote"],
                        "base_quantity": state["base_quantity"],
                        "liquidation_bid": liquidation_bid,
                        "liquidation_covered_base": liquidation_covered_base,
                        "equity_quote": equity_quote,
                        "peak_equity_quote": peak,
                        "drawdown": drawdown,
                        "reason": "book",
                        "created_wall_ns": event_wall_ns,
                    },
                )
            if self._should_sample_health(
                connection,
                event_wall_ns,
                force=continuity_reason is not None,
            ):
                self._insert_book_health(
                    connection,
                    book,
                    event_wall_ns,
                    continuity_reason,
                )
            return ShadowStepResult(
                run_id=self.run_id,
                lifecycle_status=str(state["lifecycle_status"]),
                decision_id=decision_id,
                decision_status=decision_status,
                order_id=order_id,
                resolved_order_id=resolved_order_id,
                fill_id=fill_id,
                duplicate_event=False,
                ignored_event=False,
                continuity_reason=continuity_reason,
                cash_quote=float(state["cash_quote"]),
                base_quantity=float(state["base_quantity"]),
                equity_quote=equity_quote,
            )

    def record_health(
        self,
        *,
        component: str,
        status: Literal["ok", "degraded", "critical"],
        observed_wall_ns: int,
        details: Mapping[str, Any],
        event_key: str | None = None,
    ) -> str:
        """Persist a thread-safe heartbeat and enqueue degraded alerts."""

        component = _text(component, "component")
        if status not in ("ok", "degraded", "critical"):
            raise ValueError("health status must be ok, degraded, or critical")
        if (
            isinstance(observed_wall_ns, bool)
            or not isinstance(observed_wall_ns, int)
            or observed_wall_ns <= 0
        ):
            raise ValueError("observed_wall_ns must be a positive integer")
        details_json = canonical_json(dict(details))
        health_id = deterministic_shadow_id(
            "health",
            self.run_id,
            component,
            event_key or observed_wall_ns,
        )
        with self.store.write_transaction() as connection:
            self.store.insert_exact(
                connection,
                "shadow_health",
                "health_id",
                {
                    "health_id": health_id,
                    "run_id": self.run_id,
                    "component": component,
                    "status": status,
                    "observed_wall_ns": observed_wall_ns,
                    "details_json": details_json,
                },
            )
            if status != "ok":
                self.store.enqueue_notification(
                    connection,
                    run_id=self.run_id,
                    alert_key=f"health:{self.run_id}:{health_id}",
                    topic=f"shadow_health_{component}",
                    severity=(
                        "critical" if status == "critical" else "warning"
                    ),
                    payload={
                        "run_id": self.run_id,
                        "component": component,
                        "status": status,
                        "details": dict(details),
                    },
                    created_wall_ns=observed_wall_ns,
                )
        return health_id

    def halt(self, reason: str, *, observed_wall_ns: int) -> int:
        """Durably halt new intents and atomically expire pending orders."""

        reason = _text(reason, "reason")
        if (
            isinstance(observed_wall_ns, bool)
            or not isinstance(observed_wall_ns, int)
            or observed_wall_ns <= 0
        ):
            raise ValueError("observed_wall_ns must be a positive integer")
        halt_reason = f"external_halt:{reason}"
        with self.store.write_transaction() as connection:
            state = connection.execute(
                "SELECT lifecycle_status FROM shadow_state WHERE run_id = ?",
                (self.run_id,),
            ).fetchone()
            if state is None:
                raise ShadowInvariantError("shadow run state disappeared")
            expired = self._expire_pending(
                connection,
                reason=halt_reason,
                completed_wall_ns=observed_wall_ns,
            )
            connection.execute(
                """
                UPDATE shadow_runs
                SET status = 'halted_recovery', halt_reason = ?
                WHERE run_id = ?
                """,
                (halt_reason, self.run_id),
            )
            connection.execute(
                """
                UPDATE shadow_state
                SET lifecycle_status = 'halted_recovery',
                    revision = revision + 1,
                    halt_reason = ?,
                    updated_wall_ns = ?
                WHERE run_id = ?
                """,
                (halt_reason, observed_wall_ns, self.run_id),
            )
            self.store.enqueue_notification(
                connection,
                run_id=self.run_id,
                alert_key=f"halt:{self.run_id}:{halt_reason}",
                topic="shadow_halted",
                severity="critical",
                payload={
                    "run_id": self.run_id,
                    "reason": reason,
                    "expired_pending_orders": expired,
                    "orders_sent": 0,
                },
                created_wall_ns=observed_wall_ns,
            )
            return expired

    def stop(
        self,
        *,
        reason: str = "sigterm",
        ended_wall_ns: int | None = None,
    ) -> int:
        """Gracefully stop and preserve cash/position for restart recovery."""

        selected_time = time.time_ns() if ended_wall_ns is None else ended_wall_ns
        return self.store.stop_run(
            self.run_id,
            reason=reason,
            ended_wall_ns=selected_time,
        )

    def _resolve_pending(
        self,
        connection: sqlite3.Connection,
        state: dict[str, Any],
        book: PublicOrderBook,
        event_wall_ns: int,
    ) -> tuple[str | None, str | None]:
        pending = connection.execute(
            """
            SELECT o.*, d.capture_id, d.connection_id,
                   d.book_ordinal AS decision_book_ordinal,
                   d.book_monotonic_ns AS decision_monotonic_ns
            FROM shadow_orders AS o
            JOIN shadow_decisions AS d ON d.decision_id = o.decision_id
            WHERE o.run_id = ? AND o.status = 'pending'
            """,
            (self.run_id,),
        ).fetchone()
        if pending is None:
            return None, None
        order_id = str(pending["order_id"])
        if (
            pending["capture_id"] != book.capture_id
            or pending["connection_id"] != book.connection_id
        ):
            self._expire_order(
                connection,
                order_id,
                "connection_boundary",
                event_wall_ns,
            )
            return order_id, None
        due_ns = int(pending["due_monotonic_ns"])
        if book.received_monotonic_ns < due_ns:
            return None, None
        if book.received_monotonic_ns - due_ns > self.config.max_book_gap_ns:
            self._expire_order(
                connection,
                order_id,
                "no_fresh_book_after_latency",
                event_wall_ns,
            )
            self.store.enqueue_notification(
                connection,
                run_id=self.run_id,
                alert_key=f"order-expired-stale:{self.run_id}:{order_id}",
                topic="shadow_order_expired",
                severity="warning",
                payload={
                    "run_id": self.run_id,
                    "order_id": order_id,
                    "reason": "no_fresh_book_after_latency",
                    "lateness_ns": book.received_monotonic_ns - due_ns,
                },
                created_wall_ns=event_wall_ns,
            )
            return order_id, None

        order = IndependentTakerOrder(
            order_id=order_id,
            capture_id=book.capture_id,
            connection_id=book.connection_id,
            market=book.market,
            side=str(pending["side"]),  # type: ignore[arg-type]
            decision_book_ordinal=int(pending["decision_book_ordinal"]),
            decision_monotonic_ns=int(pending["decision_monotonic_ns"]),
            base_quantity=pending["requested_base"],
            quote_notional=pending["requested_quote"],
        )
        execution = sweep_visible_depth(
            book,
            order,
            fee_rate=self.config.fee_rate,
        )
        if execution.side == "buy":
            next_cash = float(state["cash_quote"]) + execution.cash_flow_quote
            if next_cash < -self.config.position_epsilon:
                raise ShadowInvariantError(
                    "simulated buy exceeded reserved shadow cash"
                )
            old_base = float(state["base_quantity"])
            old_cost = old_base * float(state["average_cost_quote"])
            added_cost = execution.filled_quote + execution.fee_quote
            next_base = old_base + execution.filled_base
            state["cash_quote"] = max(0.0, next_cash)
            state["base_quantity"] = next_base
            state["average_cost_quote"] = (
                (old_cost + added_cost) / next_base
                if next_base > self.config.position_epsilon
                else 0.0
            )
        else:
            old_base = float(state["base_quantity"])
            next_base = old_base - execution.filled_base
            if next_base < -self.config.position_epsilon:
                raise ShadowInvariantError(
                    "simulated sell exceeded shadow position"
                )
            cost_released = (
                execution.filled_base * float(state["average_cost_quote"])
            )
            state["cash_quote"] = (
                float(state["cash_quote"]) + execution.cash_flow_quote
            )
            state["base_quantity"] = max(0.0, next_base)
            state["realized_pnl_quote"] = (
                float(state["realized_pnl_quote"])
                + execution.cash_flow_quote
                - cost_released
            )
            if state["base_quantity"] <= self.config.position_epsilon:
                state["base_quantity"] = 0.0
                state["average_cost_quote"] = 0.0
        state["cumulative_fees_quote"] = (
            float(state["cumulative_fees_quote"]) + execution.fee_quote
        )

        order_status = (
            "filled" if execution.fully_filled else "partially_filled"
        )
        terminal_reason = (
            "filled"
            if execution.fully_filled
            else "visible_depth_exhausted_remainder_cancelled"
        )
        connection.execute(
            """
            UPDATE shadow_orders
            SET status = ?,
                remaining_base = ?,
                remaining_quote = ?,
                terminal_reason = ?,
                completed_wall_ns = ?
            WHERE order_id = ? AND status = 'pending'
            """,
            (
                order_status,
                execution.unfilled_base,
                execution.unfilled_quote,
                terminal_reason,
                event_wall_ns,
                order_id,
            ),
        )
        fill_id = deterministic_shadow_id(
            "fill",
            self.run_id,
            order_id,
            book.capture_id,
            book.connection_id,
            book.ordinal,
        )
        self.store.insert_exact(
            connection,
            "shadow_fills",
            "fill_id",
            {
                "fill_id": fill_id,
                "order_id": order_id,
                "run_id": self.run_id,
                "capture_id": book.capture_id,
                "connection_id": book.connection_id,
                "book_ordinal": book.ordinal,
                "book_monotonic_ns": book.received_monotonic_ns,
                "book_wall_ns": book.received_wall_ns,
                "side": execution.side,
                "filled_base": execution.filled_base,
                "filled_quote": execution.filled_quote,
                "fee_quote": execution.fee_quote,
                "vwap_price": execution.vwap_price,
                "levels_consumed": execution.levels_consumed,
                "execution_status": execution.status,
                "assumptions_json": canonical_json(execution.assumptions),
                "created_wall_ns": event_wall_ns,
            },
        )
        self.store.enqueue_notification(
            connection,
            run_id=self.run_id,
            alert_key=f"fill:{self.run_id}:{fill_id}",
            topic="shadow_fill",
            severity="info",
            payload={
                "run_id": self.run_id,
                "order_id": order_id,
                "fill_id": fill_id,
                "side": execution.side,
                "status": execution.status,
                "filled_base": execution.filled_base,
                "filled_quote": execution.filled_quote,
                "fee_quote": execution.fee_quote,
                "vwap_price": execution.vwap_price,
                "book_ordinal": book.ordinal,
            },
            created_wall_ns=event_wall_ns,
        )
        return order_id, fill_id

    def _record_intent(
        self,
        connection: sqlite3.Connection,
        state: dict[str, Any],
        book: PublicOrderBook,
        intent: ShadowIntent,
        event_wall_ns: int,
        *,
        allow_recovery: bool = False,
        due_monotonic_ns: int | None = None,
    ) -> tuple[str, str, str | None]:
        decision_id = deterministic_shadow_id(
            "decision",
            self.run_id,
            book.capture_id,
            book.connection_id,
            book.ordinal,
        )
        rejection: str | None = None
        decision_status = "hold" if intent.action == "hold" else "accepted"
        if intent.action != "hold":
            recovery_sell = (
                allow_recovery
                and state["lifecycle_status"] == "halted_recovery"
                and intent.action == "sell"
                and intent.reason == "gap_recovery_liquidation"
            )
            if state["lifecycle_status"] != "running" and not recovery_sell:
                rejection = f"lifecycle_{state['lifecycle_status']}"
            elif connection.execute(
                """
                SELECT 1 FROM shadow_orders
                WHERE run_id = ? AND status = 'pending'
                """,
                (self.run_id,),
            ).fetchone() is not None:
                rejection = "pending_order_exists"
            elif (
                intent.action == "buy"
                and self.config.one_position_only
                and float(state["base_quantity"]) > self.config.position_epsilon
            ):
                rejection = "position_already_open"
            elif (
                intent.action == "buy"
                and float(intent.quote_notional) > self.config.max_order_quote
            ):
                rejection = "max_order_quote"
            elif (
                intent.action == "buy"
                and float(intent.quote_notional) * (1.0 + self.config.fee_rate)
                > float(state["cash_quote"]) + self.config.position_epsilon
            ):
                rejection = "insufficient_shadow_cash"
            elif (
                intent.action == "sell"
                and float(intent.base_quantity)
                > float(state["base_quantity"]) + self.config.position_epsilon
            ):
                rejection = "insufficient_shadow_position"
            elif (
                intent.action == "sell"
                and float(state["base_quantity"]) <= self.config.position_epsilon
            ):
                rejection = "no_shadow_position"
            if rejection is not None:
                decision_status = "rejected"

        self.store.insert_exact(
            connection,
            "shadow_decisions",
            "decision_id",
            {
                "decision_id": decision_id,
                "run_id": self.run_id,
                "capture_id": book.capture_id,
                "connection_id": book.connection_id,
                "book_ordinal": book.ordinal,
                "book_monotonic_ns": book.received_monotonic_ns,
                "book_wall_ns": book.received_wall_ns,
                "action": intent.action,
                "signal": intent.signal,
                "reason": intent.reason,
                "policy_version": intent.policy_version,
                "features_json": canonical_json(dict(intent.features)),
                "status": decision_status,
                "rejection_reason": rejection,
                "created_wall_ns": event_wall_ns,
            },
        )
        if decision_status != "accepted" or intent.action == "hold":
            return decision_id, decision_status, None

        order_id = deterministic_shadow_id("order", decision_id)
        request_kind = (
            "quote_notional"
            if intent.action == "buy"
            else "base_quantity"
        )
        requested_base = (
            None if intent.action == "buy" else intent.base_quantity
        )
        requested_quote = (
            intent.quote_notional if intent.action == "buy" else None
        )
        self.store.insert_exact(
            connection,
            "shadow_orders",
            "order_id",
            {
                "order_id": order_id,
                "decision_id": decision_id,
                "run_id": self.run_id,
                "side": intent.action,
                "request_kind": request_kind,
                "requested_base": requested_base,
                "requested_quote": requested_quote,
                "remaining_base": requested_base or 0.0,
                "remaining_quote": requested_quote or 0.0,
                "due_monotonic_ns": (
                    book.received_monotonic_ns + self.config.latency_ns
                    if due_monotonic_ns is None
                    else due_monotonic_ns
                ),
                "status": "pending",
                "terminal_reason": None,
                "created_wall_ns": event_wall_ns,
                "completed_wall_ns": None,
            },
        )
        return decision_id, decision_status, order_id

    def _liquidation_equity(
        self,
        state: Mapping[str, Any],
        book: PublicOrderBook,
    ) -> tuple[float, float | None, float]:
        cash = float(state["cash_quote"])
        base = float(state["base_quantity"])
        if base <= self.config.position_epsilon:
            return cash, None, 0.0
        mark_order = IndependentTakerOrder(
            order_id=deterministic_shadow_id(
                "mark",
                self.run_id,
                book.capture_id,
                book.connection_id,
                book.ordinal,
            ),
            capture_id=book.capture_id,
            connection_id=book.connection_id,
            market=book.market,
            side="sell",
            decision_book_ordinal=book.ordinal,
            decision_monotonic_ns=book.received_monotonic_ns,
            base_quantity=base,
        )
        execution = sweep_visible_depth(
            book,
            mark_order,
            fee_rate=self.config.fee_rate,
        )
        # Any position beyond displayed bids is conservatively valued at zero.
        return (
            max(0.0, cash + execution.cash_flow_quote),
            book.best_bid.price,
            execution.filled_base,
        )

    def _insert_book_health(
        self,
        connection: sqlite3.Connection,
        book: PublicOrderBook,
        event_wall_ns: int,
        continuity_reason: str | None,
    ) -> None:
        health_id = deterministic_shadow_id(
            "health",
            self.run_id,
            "market_feed",
            book.capture_id,
            book.connection_id,
            book.ordinal,
        )
        self.store.insert_exact(
            connection,
            "shadow_health",
            "health_id",
            {
                "health_id": health_id,
                "run_id": self.run_id,
                "component": "market_feed",
                "status": (
                    "degraded" if continuity_reason is not None else "ok"
                ),
                "observed_wall_ns": event_wall_ns,
                "details_json": canonical_json(
                    {
                        "capture_id": book.capture_id,
                        "connection_id": book.connection_id,
                        "ordinal": book.ordinal,
                        "received_monotonic_ns": book.received_monotonic_ns,
                        "continuity_reason": continuity_reason,
                    }
                ),
            },
        )

    def _should_sample_equity(
        self,
        connection: sqlite3.Connection,
        book: PublicOrderBook,
        *,
        force: bool,
    ) -> bool:
        if force:
            return True
        row = connection.execute(
            """
            SELECT MAX(book_monotonic_ns) AS sampled_ns
            FROM shadow_equity WHERE run_id = ?
            """,
            (self.run_id,),
        ).fetchone()
        sampled = row["sampled_ns"]
        return (
            sampled is None
            or book.received_monotonic_ns - int(sampled)
            >= self.config.equity_sample_interval_ns
        )

    def _should_sample_health(
        self,
        connection: sqlite3.Connection,
        event_wall_ns: int,
        *,
        force: bool,
    ) -> bool:
        if force:
            return True
        row = connection.execute(
            """
            SELECT MAX(observed_wall_ns) AS sampled_ns
            FROM shadow_health
            WHERE run_id = ? AND component = 'market_feed'
            """,
            (self.run_id,),
        ).fetchone()
        sampled = row["sampled_ns"]
        return (
            sampled is None
            or event_wall_ns - int(sampled)
            >= self.config.health_sample_interval_ns
        )

    def _expire_pending(
        self,
        connection: sqlite3.Connection,
        *,
        reason: str,
        completed_wall_ns: int,
    ) -> int:
        cursor = connection.execute(
            """
            UPDATE shadow_orders
            SET status = 'expired',
                terminal_reason = ?,
                completed_wall_ns = ?
            WHERE run_id = ? AND status = 'pending'
            """,
            (reason, completed_wall_ns, self.run_id),
        )
        return int(cursor.rowcount)

    @staticmethod
    def _expire_order(
        connection: sqlite3.Connection,
        order_id: str,
        reason: str,
        completed_wall_ns: int,
    ) -> None:
        cursor = connection.execute(
            """
            UPDATE shadow_orders
            SET status = 'expired',
                terminal_reason = ?,
                completed_wall_ns = ?
            WHERE order_id = ? AND status = 'pending'
            """,
            (reason, completed_wall_ns, order_id),
        )
        if cursor.rowcount != 1:
            raise ShadowInvariantError("pending order changed concurrently")

    @staticmethod
    def _event_wall_ns(book: PublicOrderBook) -> int:
        if book.received_wall_ns is not None:
            return book.received_wall_ns
        if book.exchange_timestamp_ms is not None:
            return book.exchange_timestamp_ms * 1_000_000
        # Synthetic tests may have no wall clock.  A causal monotonic value keeps
        # deterministic insert payloads without pretending it is epoch time.
        return book.received_monotonic_ns

    @staticmethod
    def _is_exact_duplicate(
        state: Mapping[str, Any],
        book: PublicOrderBook,
    ) -> bool:
        return (
            state["last_capture_id"] == book.capture_id
            and state["last_connection_id"] == book.connection_id
            and state["last_book_ordinal"] == book.ordinal
            and state["last_book_monotonic_ns"]
            == book.received_monotonic_ns
            and state["last_book_wall_ns"] == book.received_wall_ns
        )

    @staticmethod
    def _reject_regressed_or_colliding_book(
        state: Mapping[str, Any],
        book: PublicOrderBook,
    ) -> None:
        if (
            state["last_capture_id"] == book.capture_id
            and state["last_connection_id"] == book.connection_id
            and state["last_book_ordinal"] is not None
            and book.ordinal <= int(state["last_book_ordinal"])
        ):
            raise ShadowInvariantError(
                "book ordinal regressed or reused with different data"
            )
        if (
            state["last_capture_id"] == book.capture_id
            and state["last_connection_id"] == book.connection_id
            and state["last_book_monotonic_ns"] is not None
            and book.received_monotonic_ns
            <= int(state["last_book_monotonic_ns"])
        ):
            raise ShadowInvariantError(
                "book receive-monotonic time regressed"
            )

    @staticmethod
    def _continuity_reason(
        state: Mapping[str, Any],
        book: PublicOrderBook,
    ) -> str | None:
        if book.monotonic_regression:
            return "monotonic_regression_marker"
        if book.gap_before:
            return f"gap:{book.gap_reason}"
        if state["last_capture_id"] is None:
            return None
        if state["last_capture_id"] != book.capture_id:
            return "capture_changed"
        if state["last_connection_id"] != book.connection_id:
            return "connection_changed"
        return None

    @staticmethod
    def _is_automatic_recovery_halt(state: Mapping[str, Any]) -> bool:
        reason = state.get("halt_reason")
        return (
            reason == "restart_with_open_position"
            or (
                isinstance(reason, str)
                and reason.startswith("continuity_with_open_position:")
            )
        )

    def _duplicate_result(
        self,
        connection: sqlite3.Connection,
        state: Mapping[str, Any],
        book: PublicOrderBook,
        intent: ShadowIntent | None,
    ) -> ShadowStepResult:
        decision_id = deterministic_shadow_id(
            "decision",
            self.run_id,
            book.capture_id,
            book.connection_id,
            book.ordinal,
        )
        decision = connection.execute(
            """
            SELECT action, signal, reason, policy_version, features_json, status
            FROM shadow_decisions WHERE decision_id = ?
            """,
            (decision_id,),
        ).fetchone()
        if intent is not None:
            if decision is None:
                raise ShadowInvariantError(
                    "cannot attach a new intent to an already processed book"
                )
            immutable_actual = (
                decision["action"],
                decision["signal"],
                decision["reason"],
                decision["policy_version"],
                decision["features_json"],
            )
            immutable_expected = (
                intent.action,
                intent.signal,
                intent.reason,
                intent.policy_version,
                canonical_json(dict(intent.features)),
            )
            if immutable_actual != immutable_expected:
                raise ShadowInvariantError(
                    "duplicate book carried a different policy intent"
                )
        order = connection.execute(
            """
            SELECT o.order_id, f.fill_id
            FROM shadow_orders AS o
            LEFT JOIN shadow_fills AS f ON f.order_id = o.order_id
            WHERE o.decision_id = ?
            """,
            (decision_id,),
        ).fetchone()
        return ShadowStepResult(
            run_id=self.run_id,
            lifecycle_status=str(state["lifecycle_status"]),
            decision_id=decision_id if decision is not None else None,
            decision_status=(
                str(decision["status"]) if decision is not None else None
            ),
            order_id=(str(order["order_id"]) if order is not None else None),
            resolved_order_id=None,
            fill_id=(
                str(order["fill_id"])
                if order is not None and order["fill_id"] is not None
                else None
            ),
            duplicate_event=True,
            ignored_event=False,
            continuity_reason=None,
            cash_quote=float(state["cash_quote"]),
            base_quantity=float(state["base_quantity"]),
            equity_quote=float(state["last_equity_quote"]),
        )

    def _result_from_state(
        self,
        state: Mapping[str, Any],
        *,
        ignored_event: bool,
    ) -> ShadowStepResult:
        return ShadowStepResult(
            run_id=self.run_id,
            lifecycle_status=str(state["lifecycle_status"]),
            decision_id=None,
            decision_status=None,
            order_id=None,
            resolved_order_id=None,
            fill_id=None,
            duplicate_event=False,
            ignored_event=ignored_event,
            continuity_reason=None,
            cash_quote=float(state["cash_quote"]),
            base_quantity=float(state["base_quantity"]),
            equity_quote=float(state["last_equity_quote"]),
        )
