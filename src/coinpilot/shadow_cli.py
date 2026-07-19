"""CLI handlers for the isolated shadow runtime and operations processes."""

from __future__ import annotations

import json
import signal
import threading
from argparse import Namespace
from pathlib import Path
from typing import Any

from coinpilot.config import AppConfig
from coinpilot.hft_shadow_store import ShadowStore
from coinpilot.shadow_ops import (
    SlackWebhookClient,
    read_slack_webhook_from_keychain,
    run_notifier,
    run_watchdog,
    serve_status,
)
from coinpilot.shadow_service import run_shadow_service


def _print(value: Any) -> None:
    print(
        json.dumps(
            value,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
    )


def _sigterm_event() -> tuple[threading.Event, Any, bool]:
    requested = threading.Event()
    try:
        previous = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, lambda _signum, _frame: requested.set())
    except (AttributeError, ValueError):
        return requested, None, False
    return requested, previous, True


def _restore_sigterm(previous: Any, installed: bool) -> None:
    if installed:
        signal.signal(signal.SIGTERM, previous)


def cmd_shadow_run(args: Namespace, config: AppConfig) -> int:
    stop, previous, installed = _sigterm_event()
    try:
        result = run_shadow_service(
            config,
            duration_seconds=args.seconds,
            max_events=args.max_events,
            capture_id=args.capture_id,
            external_stop_requested=stop.is_set,
        )
    finally:
        _restore_sigterm(previous, installed)
    _print(result.to_dict())
    return 0


def cmd_shadow_notify(args: Namespace, config: AppConfig) -> int:
    webhook = read_slack_webhook_from_keychain(
        service=config.operations.keychain_service,
        account=config.operations.keychain_account,
    )
    store = ShadowStore(config.shadow.database_path)
    client = SlackWebhookClient(webhook)
    stop, previous, installed = _sigterm_event()
    try:
        result = run_notifier(
            store,
            client,
            poll_seconds=config.operations.notifier_poll_seconds,
            once=args.once,
            stop_requested=stop.is_set,
        )
    finally:
        _restore_sigterm(previous, installed)
    _print({"mode": "shadow_notifier", **result, "orders_sent": 0})
    return 0


def cmd_shadow_web(args: Namespace, config: AppConfig) -> int:
    host = config.operations.bind_host if args.host is None else args.host
    port = config.operations.port if args.port is None else args.port
    store = ShadowStore(config.shadow.database_path)
    serve_status(
        store,
        host=host,
        port=port,
        stale_after_seconds=config.operations.stale_after_seconds,
    )
    return 0


def cmd_shadow_watchdog(args: Namespace, config: AppConfig) -> int:
    store = ShadowStore(config.shadow.database_path)
    result = run_watchdog(
        store,
        stale_after_seconds=config.operations.stale_after_seconds,
    )
    _print({"mode": "shadow_watchdog", **result, "orders_sent": 0})
    return 0


def cmd_shadow_status(args: Namespace, config: AppConfig) -> int:
    database = Path(config.shadow.database_path)
    if not database.exists():
        _print(
            {
                "mode": "shadow_status",
                "status": "not_started",
                "database_path": str(database),
                "orders_sent": 0,
            }
        )
        return 0
    store = ShadowStore(database)
    run_id = store.latest_run_id(config.data.market)
    if run_id is None:
        _print(
            {
                "mode": "shadow_status",
                "status": "no_run",
                "orders_sent": 0,
            }
        )
        return 0
    _print(
        {
            "mode": "shadow_status",
            "status": store.read_status(run_id),
            "recent_fills": store.recent_fills(run_id, limit=args.fills),
            "equity": store.read_equity(run_id, limit=args.equity),
            "orders_sent": 0,
        }
    )
    return 0
