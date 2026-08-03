#!/usr/bin/env python3
"""Bounded diagnostic shadow runner kept outside the frozen C2 source tree.

This process only patches the public-feed simulated shadow runtime in its own
interpreter. It never imports an exchange credential client or exposes a live
order path. The installed C2 package and its T0 evidence remain untouched.
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import math
import os
import signal
import sqlite3
import tempfile
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from coinpilot.config import AppConfig, load_config
from coinpilot.hft_shadow import (
    LIVE_ORDER_ROUTING_SUPPORTED,
    ShadowEngine as CoreShadowEngine,
    ShadowIntent,
)
from coinpilot.hft_depth import IndependentTakerOrder, sweep_visible_depth
from coinpilot.hft_shadow_signal import DiagnosticSnapshot
from coinpilot.hft_shadow_store import ShadowStore
import coinpilot.shadow_service as shadow_service
from coinpilot.shadow_service import DiagnosticPolicy as CoreDiagnosticPolicy


KST = ZoneInfo("Asia/Seoul")


@dataclass(frozen=True, slots=True)
class BoundedSettings:
    cooldown_seconds: float
    max_round_trips_per_day: int
    reserve_full_order_loss: bool
    execution_spread_recheck: bool
    audit_dir: Path

    def validate(self) -> BoundedSettings:
        if (
            not math.isfinite(self.cooldown_seconds)
            or self.cooldown_seconds < 0
        ):
            raise ValueError("cooldown_seconds must be non-negative and finite")
        if (
            isinstance(self.max_round_trips_per_day, bool)
            or self.max_round_trips_per_day < 1
        ):
            raise ValueError("max_round_trips_per_day must be positive")
        if not isinstance(self.reserve_full_order_loss, bool):
            raise ValueError("reserve_full_order_loss must be boolean")
        if not isinstance(self.execution_spread_recheck, bool):
            raise ValueError("execution_spread_recheck must be boolean")
        return self


_SETTINGS: BoundedSettings | None = None
_MAX_EXECUTION_SPREAD_BPS: float | None = None


def _settings() -> BoundedSettings:
    if _SETTINGS is None:
        raise RuntimeError("bounded settings were not initialized")
    return _SETTINGS


def _event_wall_ns(snapshot: DiagnosticSnapshot) -> int:
    value = snapshot.book.received_wall_ns
    return time.time_ns() if value is None else int(value)


def _kst_day_start_ns(wall_ns: int) -> int:
    moment = datetime.fromtimestamp(wall_ns / 1e9, tz=KST)
    return int(
        moment.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
        * 1e9
    )


def _activity_since(
    store: ShadowStore,
    *,
    market: str,
    wall_ns: int,
) -> tuple[int, int | None, int, int | None]:
    """Read durable daily caps and cross-midnight retry timestamps."""

    connection = sqlite3.connect(store.path, timeout=30.0)
    connection.row_factory = sqlite3.Row
    try:
        row = connection.execute(
            """
            SELECT (
                       SELECT COUNT(*)
                       FROM shadow_fills AS f
                       JOIN shadow_runs AS r ON r.run_id = f.run_id
                       WHERE r.market = ?
                         AND f.side = 'sell'
                         AND f.created_wall_ns >= ?
                   ) AS completed_sells,
                   (
                       SELECT MAX(f.created_wall_ns)
                       FROM shadow_fills AS f
                       JOIN shadow_runs AS r ON r.run_id = f.run_id
                       WHERE r.market = ? AND f.side = 'sell'
                   ) AS latest_sell_wall_ns,
                   (
                       SELECT COUNT(*)
                       FROM shadow_orders AS o
                       JOIN shadow_runs AS r ON r.run_id = o.run_id
                       WHERE r.market = ?
                         AND o.side = 'buy'
                         AND o.created_wall_ns >= ?
                   ) AS entry_attempts,
                   (
                       SELECT MAX(o.created_wall_ns)
                       FROM shadow_orders AS o
                       JOIN shadow_runs AS r ON r.run_id = o.run_id
                       WHERE r.market = ? AND o.side = 'buy'
                   ) AS latest_buy_wall_ns
            """,
            (
                market,
                _kst_day_start_ns(wall_ns),
                market,
                market,
                _kst_day_start_ns(wall_ns),
                market,
            ),
        ).fetchone()
    finally:
        connection.close()
    if row is None:
        return 0, None, 0, None
    latest_sell = row["latest_sell_wall_ns"]
    latest_buy = row["latest_buy_wall_ns"]
    return (
        int(row["completed_sells"]),
        None if latest_sell is None else int(latest_sell),
        int(row["entry_attempts"]),
        None if latest_buy is None else int(latest_buy),
    )


def _fill_summary(store: ShadowStore, run_id: str) -> dict[str, float | int]:
    """Aggregate the complete restart lineage carried into ``run_id``."""

    connection = sqlite3.connect(store.path, timeout=30.0)
    connection.row_factory = sqlite3.Row
    try:
        row = connection.execute(
            """
            WITH RECURSIVE lineage(run_id, restart_of_run_id) AS (
                SELECT run_id, restart_of_run_id
                FROM shadow_runs
                WHERE run_id = ?
                UNION ALL
                SELECT r.run_id, r.restart_of_run_id
                FROM shadow_runs AS r
                JOIN lineage AS child
                  ON r.run_id = child.restart_of_run_id
            )
            SELECT COUNT(*) AS fills,
                   COALESCE(SUM(
                       CASE WHEN side = 'buy' THEN 1 ELSE 0 END
                   ), 0) AS buys,
                   COALESCE(SUM(
                       CASE WHEN side = 'sell' THEN 1 ELSE 0 END
                   ), 0) AS sells,
                   COALESCE(SUM(fee_quote), 0.0) AS fees_quote,
                   COALESCE(SUM(
                       CASE WHEN side = 'buy' THEN filled_quote ELSE 0 END
                   ), 0.0) AS bought_quote,
                   COALESCE(SUM(
                       CASE WHEN side = 'sell' THEN filled_quote ELSE 0 END
                   ), 0.0) AS sold_quote
            FROM shadow_fills AS f
            JOIN lineage AS l ON l.run_id = f.run_id
            """,
            (run_id,),
        ).fetchone()
    finally:
        connection.close()
    if row is None:
        return {
            "fills": 0,
            "buys": 0,
            "sells": 0,
            "fees_quote": 0.0,
            "bought_quote": 0.0,
            "sold_quote": 0.0,
        }
    return {
        "fills": int(row["fills"]),
        "buys": int(row["buys"]),
        "sells": int(row["sells"]),
        "fees_quote": float(row["fees_quote"]),
        "bought_quote": float(row["bought_quote"]),
        "sold_quote": float(row["sold_quote"]),
    }


def _write_json_owner_only(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.is_symlink() or path.parent.is_symlink():
        raise RuntimeError("refusing a symlinked halt diagnostic path")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(
                payload,
                handle,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
                allow_nan=False,
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
        directory_descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        temporary.unlink(missing_ok=True)


def _write_or_verify_audit(path: Path, payload: dict[str, Any]) -> None:
    if path.exists():
        if path.is_symlink() or not path.is_file():
            raise RuntimeError("refusing an unsafe existing halt audit path")
        existing = json.loads(path.read_text(encoding="utf-8"))
        if existing != payload:
            raise RuntimeError("existing halt audit payload does not match ledger")
        return
    _write_json_owner_only(path, payload)


def _lineage_day_start_equity(
    store: ShadowStore,
    *,
    run_id: str,
    wall_ns: int,
) -> float | None:
    connection = sqlite3.connect(store.path, timeout=30.0)
    connection.row_factory = sqlite3.Row
    try:
        row = connection.execute(
            """
            WITH RECURSIVE lineage(run_id, restart_of_run_id) AS (
                SELECT run_id, restart_of_run_id
                FROM shadow_runs
                WHERE run_id = ?
                UNION ALL
                SELECT r.run_id, r.restart_of_run_id
                FROM shadow_runs AS r
                JOIN lineage AS child
                  ON r.run_id = child.restart_of_run_id
            )
            SELECT e.equity_quote
            FROM shadow_equity AS e
            JOIN lineage AS l ON l.run_id = e.run_id
            WHERE e.book_wall_ns IS NOT NULL
              AND e.book_wall_ns >= ?
            ORDER BY e.book_wall_ns ASC, e.rowid ASC
            LIMIT 1
            """,
            (run_id, _kst_day_start_ns(wall_ns)),
        ).fetchone()
    finally:
        connection.close()
    return None if row is None else float(row["equity_quote"])


def _run_identity(store: ShadowStore, run_id: str) -> dict[str, Any]:
    connection = sqlite3.connect(store.path, timeout=30.0)
    connection.row_factory = sqlite3.Row
    try:
        row = connection.execute(
            """
            SELECT config_hash, code_version, started_wall_ns,
                   ended_wall_ns, restart_of_run_id
            FROM shadow_runs
            WHERE run_id = ?
            """,
            (run_id,),
        ).fetchone()
    finally:
        connection.close()
    if row is None:
        raise RuntimeError(f"halted run disappeared: {run_id}")
    return dict(row)


def _settings_hash(settings: BoundedSettings) -> str:
    payload = {
        **dataclasses.asdict(settings),
        "audit_dir": "owner-only-instance-state",
    }
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _halt_audit_payload(
    config: AppConfig,
    settings: BoundedSettings,
    store: ShadowStore,
    *,
    run_id: str,
    status: dict[str, Any],
    trigger: str,
) -> dict[str, Any]:
    observed_wall_ns = int(status.get("updated_wall_ns") or time.time_ns())
    fills = _fill_summary(store, run_id)
    identity = _run_identity(store, run_id)
    initial_cash = float(config.shadow.initial_cash)
    equity = float(status["last_equity_quote"])
    peak = float(status["peak_equity_quote"])
    persisted_day_start = _lineage_day_start_equity(
        store,
        run_id=run_id,
        wall_ns=observed_wall_ns,
    )
    day_start = (
        initial_cash if persisted_day_start is None else persisted_day_start
    )
    account_pnl = equity - initial_cash
    account_loss = max(0.0, -account_pnl)
    daily_loss = max(0.0, day_start - equity)
    drawdown_loss = max(0.0, peak - equity)
    normalized_trigger = trigger.removeprefix("external_halt:")
    if "drawdown" in normalized_trigger:
        trigger_loss = drawdown_loss
        trigger_loss_pct = drawdown_loss / max(peak, 1e-12)
        trigger_basis = "peak_equity"
    else:
        trigger_loss = daily_loss
        trigger_loss_pct = daily_loss / max(day_start, 1e-12)
        trigger_basis = "kst_day_start_equity"
    cumulative_fees = float(status["cumulative_fees_quote"])
    fee_share = (
        None if account_loss <= 0 else cumulative_fees / account_loss
    )
    return {
        "schema_version": 2,
        "mode": "bounded_shadow_loss_halt_diagnostic",
        "status": "quarantined",
        "trigger": normalized_trigger,
        "trigger_basis": trigger_basis,
        "trigger_observed_wall_ns": observed_wall_ns,
        "run_id": run_id,
        "restart_of_run_id": identity["restart_of_run_id"],
        "market": config.data.market,
        "model_version": config.shadow.model_version,
        "config_hash": identity["config_hash"],
        "code_version": identity["code_version"],
        "bounded_settings_sha256": _settings_hash(settings),
        "thresholds": {
            "max_daily_loss_pct": config.shadow.max_daily_loss_pct,
            "max_drawdown_pct": config.shadow.max_drawdown_pct,
        },
        "metrics": {
            "initial_cash_quote": initial_cash,
            "equity_quote": equity,
            "account_pnl_quote": account_pnl,
            "account_loss_quote": account_loss,
            "account_loss_pct": account_loss / initial_cash,
            "kst_day_start_equity_quote": day_start,
            "daily_loss_quote": daily_loss,
            "daily_loss_pct": daily_loss / max(day_start, 1e-12),
            "peak_equity_quote": peak,
            "drawdown_loss_quote": drawdown_loss,
            "drawdown_loss_pct": drawdown_loss / max(peak, 1e-12),
            "trigger_loss_quote": trigger_loss,
            "trigger_loss_pct": trigger_loss_pct,
            "realized_pnl_quote": float(status["realized_pnl_quote"]),
            "max_drawdown": float(status["max_drawdown"]),
            "fills": fills["fills"],
            "completed_round_trips": fills["sells"],
            "cumulative_fees_quote": cumulative_fees,
            "lineage_fill_fees_quote": fills["fees_quote"],
            "fee_share_of_account_loss": fee_share,
            "turnover_quote": (
                float(fills["bought_quote"])
                + float(fills["sold_quote"])
            ),
        },
        "diagnosis": {
            "cost_dominated_account_loss_heuristic": (
                fee_share is not None and fee_share >= 0.5
            ),
            "threshold_crossed": trigger_loss_pct >= 0.10,
            "new_entries_blocked": True,
            "halt_is_latched": True,
            "diagnostic_is_validated_alpha": False,
        },
        "improvement_policy": {
            "automatic_code_mutation": False,
            "reuse_current_ledger": False,
            "required_before_restart": [
                "freeze_and_validate_the_halted_ledger",
                "replay_candidate_causally_with_all_costs",
                "pass_time_split_and_fee_stress_promotion_gates",
                "start_a_new_model_version_in_a_new_ledger",
            ],
        },
        "simulated": True,
        "live_order_routing": False,
        "orders_sent": 0,
    }


def _materialize_missing_halt_audits(
    config: AppConfig,
    settings: BoundedSettings,
) -> int:
    database = Path(config.shadow.database_path).expanduser()
    if not database.exists():
        return 0
    store = ShadowStore(database)
    connection = sqlite3.connect(store.path, timeout=30.0)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            SELECT r.run_id, r.halt_reason, s.*
            FROM shadow_runs AS r
            JOIN shadow_state AS s ON s.run_id = r.run_id
            WHERE r.market = ?
              AND r.halt_reason LIKE 'external_halt:%'
            ORDER BY r.started_wall_ns ASC
            """,
            (config.data.market,),
        ).fetchall()
    finally:
        connection.close()
    written = 0
    for row in rows:
        run_id = str(row["run_id"])
        path = settings.audit_dir / f"{run_id}.halt-diagnostic.json"
        existed = path.exists()
        payload = _halt_audit_payload(
            config,
            settings,
            store,
            run_id=run_id,
            status=dict(row),
            trigger=str(row["halt_reason"]),
        )
        _write_or_verify_audit(path, payload)
        if not existed:
            written += 1
    return written


class BoundedShadowEngine(CoreShadowEngine):
    """Recheck every simulated entry against actual visible fill depth."""

    def _resolve_pending(self, connection, state, book, event_wall_ns):
        pending = connection.execute(
            """
            SELECT o.*, d.capture_id, d.connection_id,
                   d.book_ordinal AS decision_book_ordinal,
                   d.book_monotonic_ns AS decision_monotonic_ns,
                   d.reason
            FROM shadow_orders AS o
            JOIN shadow_decisions AS d ON d.decision_id = o.decision_id
            WHERE o.run_id = ? AND o.status = 'pending'
            """,
            (self.run_id,),
        ).fetchone()
        spread_limit = _MAX_EXECUTION_SPREAD_BPS
        if (
            _settings().execution_spread_recheck
            and spread_limit is not None
            and pending is not None
            and pending["capture_id"] == book.capture_id
            and pending["connection_id"] == book.connection_id
            and book.received_monotonic_ns
            >= int(pending["due_monotonic_ns"])
            and book.received_monotonic_ns
            - int(pending["due_monotonic_ns"])
            <= self.config.max_book_gap_ns
            and str(pending["side"]) == "buy"
        ):
            spread_bps = (
                (book.best_ask.price - book.best_bid.price)
                / book.mid_price
                * 10_000.0
            )
            preview_order = IndependentTakerOrder(
                order_id=str(pending["order_id"]),
                capture_id=book.capture_id,
                connection_id=book.connection_id,
                market=book.market,
                side="buy",
                decision_book_ordinal=int(
                    pending["decision_book_ordinal"]
                ),
                decision_monotonic_ns=int(
                    pending["decision_monotonic_ns"]
                ),
                base_quantity=pending["requested_base"],
                quote_notional=pending["requested_quote"],
            )
            preview = sweep_visible_depth(
                book,
                preview_order,
                fee_rate=self.config.fee_rate,
            )
            projected_one_way_bps = float(preview.slippage_bps_vs_mid)
            if (
                spread_bps > spread_limit
                or projected_one_way_bps > spread_limit / 2.0
            ):
                order_id = str(pending["order_id"])
                self._expire_order(
                    connection,
                    order_id,
                    "execution_spread_too_wide",
                    event_wall_ns,
                )
                self.store.enqueue_notification(
                    connection,
                    run_id=self.run_id,
                    alert_key=(
                        f"bounded-entry-expired:{self.run_id}:{order_id}"
                    ),
                    topic="shadow_order_expired",
                    severity="warning",
                    payload={
                        "run_id": self.run_id,
                        "order_id": order_id,
                        "side": "buy",
                        "reason": "execution_spread_too_wide",
                        "execution_spread_bps": spread_bps,
                        "projected_one_way_bps_vs_mid": (
                            projected_one_way_bps
                        ),
                        "max_spread_bps": spread_limit,
                        "levels_consumed": preview.levels_consumed,
                        "orders_sent": 0,
                    },
                    created_wall_ns=event_wall_ns,
                )
                return order_id, None
        return super()._resolve_pending(
            connection,
            state,
            book,
            event_wall_ns,
        )


class BoundedDiagnosticPolicy(CoreDiagnosticPolicy):
    """Apply persistent churn limits around the diagnostic-only heuristic."""

    def intent(
        self,
        snapshot: DiagnosticSnapshot,
        status: dict[str, Any],
    ) -> ShadowIntent | None:
        intent = super().intent(snapshot, status)
        if intent is None or intent.action != "buy":
            return intent

        settings = _settings()
        now_ns = _event_wall_ns(snapshot)
        (
            round_trips,
            latest_sell_ns,
            entry_attempts,
            latest_buy_ns,
        ) = _activity_since(
            self.store,
            market=self.config.data.market,
            wall_ns=now_ns,
        )
        if (
            round_trips >= settings.max_round_trips_per_day
            or entry_attempts >= settings.max_round_trips_per_day
        ):
            return None
        latest_activity_ns = max(
            value
            for value in (latest_sell_ns, latest_buy_ns, 0)
            if value is not None
        )
        if latest_activity_ns > 0:
            elapsed = max(0.0, (now_ns - latest_activity_ns) / 1e9)
            if elapsed < settings.cooldown_seconds:
                return None

        daily_loss = float(intent.features.get("daily_loss", 0.0))
        if settings.reserve_full_order_loss:
            day_start_equity = max(float(self._day_start_equity or 0.0), 1e-12)
            worst_case = self.config.shadow.order_quote * (
                1.0 + self.config.shadow.fee_rate
            )
            if (
                daily_loss + worst_case / day_start_equity
                >= self.config.shadow.max_daily_loss_pct
            ):
                self._risk_halt_reason = "daily_loss_entry_reserve"
                return None
            equity = max(float(status["last_equity_quote"]), 0.0)
            peak = max(float(status["peak_equity_quote"]), 1e-12)
            current_drawdown = max(0.0, 1.0 - equity / peak)
            if (
                current_drawdown + worst_case / peak
                >= self.config.shadow.max_drawdown_pct
            ):
                self._risk_halt_reason = "max_drawdown_entry_reserve"
                return None

        return dataclasses.replace(
            intent,
            features={
                **dict(intent.features),
                "bounded_policy": True,
                "round_trips_today": round_trips,
                "entry_attempts_today": entry_attempts,
                "max_round_trips_per_day": settings.max_round_trips_per_day,
                "cooldown_seconds": settings.cooldown_seconds,
                "execution_spread_recheck": (
                    settings.execution_spread_recheck
                ),
            },
        )

    def halt_if_flat(self, engine: CoreShadowEngine) -> bool:
        trigger = self._risk_halt_reason
        halted = super().halt_if_flat(engine)
        if not halted:
            return False
        status = engine.status()
        audit_path = _settings().audit_dir / (
            f"{engine.run_id}.halt-diagnostic.json"
        )
        _write_or_verify_audit(
            audit_path,
            _halt_audit_payload(
                self.config,
                _settings(),
                self.store,
                run_id=engine.run_id,
                status=status,
                trigger=trigger or "unknown_risk_halt",
            ),
        )
        return True


def _strict_bool(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise argparse.ArgumentTypeError("expected a boolean value")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--cooldown-seconds", type=float, default=3_600.0)
    parser.add_argument("--max-round-trips-per-day", type=int, default=24)
    parser.add_argument("--reserve-full-order-loss", type=_strict_bool, default=True)
    parser.add_argument("--execution-spread-recheck", type=_strict_bool, default=True)
    parser.add_argument("--audit-dir", type=Path, required=True)
    parser.add_argument("--seconds", type=float)
    parser.add_argument("--max-events", type=int)
    parser.add_argument("--capture-id")
    parser.add_argument("--validate-only", action="store_true")
    return parser


def _validate_ten_percent_policy(config: AppConfig) -> None:
    if config.shadow.mode != "diagnostic":
        raise ValueError("bounded shadow requires shadow.mode='diagnostic'")
    for name, value in (
        ("max_daily_loss_pct", config.shadow.max_daily_loss_pct),
        ("max_drawdown_pct", config.shadow.max_drawdown_pct),
    ):
        if not math.isclose(float(value), 0.10, abs_tol=1e-12):
            raise ValueError(f"shadow.{name} must be exactly 0.10")
    if config.shadow.model_version != "diagnostic-bounded-v1":
        raise ValueError(
            "shadow.model_version must be 'diagnostic-bounded-v1'"
        )
    if not math.isclose(
        float(config.shadow.initial_cash),
        5_000_000.0,
        abs_tol=1e-9,
    ):
        raise ValueError("shadow.initial_cash must be exactly 5000000")
    if not math.isclose(
        float(config.shadow.order_quote),
        25_000.0,
        abs_tol=1e-9,
    ):
        raise ValueError("shadow.order_quote must be exactly 25000")
    if float(config.shadow.fee_rate) > 0.0005:
        raise ValueError("shadow.fee_rate must not exceed 0.0005")
    if float(config.shadow.max_spread_bps) > 10.0:
        raise ValueError("shadow.max_spread_bps must not exceed 10")
    if config.data.market not in {
        "KRW-BTC",
        "KRW-ETH",
        "KRW-XRP",
        "KRW-SOL",
    }:
        raise ValueError("bounded shadow market is outside the approved set")
    if LIVE_ORDER_ROUTING_SUPPORTED is not False:
        raise RuntimeError("live order routing unexpectedly became available")


def _validate_safety_profile(settings: BoundedSettings) -> None:
    if settings.cooldown_seconds < 3_600.0:
        raise ValueError("bounded cooldown must be at least 3600 seconds")
    if settings.max_round_trips_per_day > 24:
        raise ValueError("bounded daily entry/trip cap must not exceed 24")
    if not settings.reserve_full_order_loss:
        raise ValueError("full-order loss reserve must remain enabled")
    if not settings.execution_spread_recheck:
        raise ValueError("execution spread recheck must remain enabled")


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    config = load_config(args.config)
    _validate_ten_percent_policy(config)
    settings = BoundedSettings(
        cooldown_seconds=args.cooldown_seconds,
        max_round_trips_per_day=args.max_round_trips_per_day,
        reserve_full_order_loss=args.reserve_full_order_loss,
        execution_spread_recheck=args.execution_spread_recheck,
        audit_dir=args.audit_dir.expanduser().resolve(strict=False),
    ).validate()
    _validate_safety_profile(settings)
    if args.validate_only:
        print(
            json.dumps(
                {
                    "valid": True,
                    "market": config.data.market,
                    "model_version": config.shadow.model_version,
                    "max_daily_loss_pct": (
                        config.shadow.max_daily_loss_pct
                    ),
                    "max_drawdown_pct": config.shadow.max_drawdown_pct,
                    "bounded_settings_sha256": _settings_hash(settings),
                    "simulated": True,
                    "live_order_routing": False,
                    "orders_sent": 0,
                },
                sort_keys=True,
                allow_nan=False,
            )
        )
        return 0
    settings.audit_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    _materialize_missing_halt_audits(config, settings)

    global _SETTINGS, _MAX_EXECUTION_SPREAD_BPS
    _SETTINGS = settings
    _MAX_EXECUTION_SPREAD_BPS = config.shadow.max_spread_bps
    shadow_service.DiagnosticPolicy = BoundedDiagnosticPolicy
    shadow_service.ShadowEngine = BoundedShadowEngine

    stop = threading.Event()
    previous = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, lambda _signum, _frame: stop.set())
    os.environ["COINPILOT_LIVE_TRADING"] = "0"
    try:
        result = shadow_service.run_shadow_service(
            config,
            duration_seconds=args.seconds,
            max_events=args.max_events,
            capture_id=args.capture_id,
            external_stop_requested=stop.is_set,
        )
    finally:
        signal.signal(signal.SIGTERM, previous)
    print(
        json.dumps(
            {
                **result.to_dict(),
                "bounded_policy": dataclasses.asdict(settings),
                "orders_sent": 0,
                "live_order_routing": False,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
            default=str,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
