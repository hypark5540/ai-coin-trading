"""Operational helpers for the isolated HFT shadow runtime.

Slack delivery and localhost reads are deliberately separate from the market
feed writer.  Neither component contains exchange credentials or order-routing
code.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections.abc import Callable, Mapping
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from coinpilot.hft_shadow_store import ShadowStore
from coinpilot.shadow_dashboard import dashboard_html as _dashboard_html


class ShadowOperationsError(RuntimeError):
    """Raised when a shadow operations component fails safely."""


class SlackDeliveryError(ShadowOperationsError):
    def __init__(
        self,
        message: str,
        *,
        retry_after_seconds: float | None = None,
    ) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


def read_slack_webhook_from_keychain(
    *,
    service: str,
    account: str,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> str:
    """Read a Slack webhook without placing it in argv, config, or environment."""

    if not service.strip() or not account.strip():
        raise ShadowOperationsError("Keychain service and account are required")
    try:
        result = runner(
            [
                "/usr/bin/security",
                "find-generic-password",
                "-a",
                account,
                "-s",
                service,
                "-w",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ShadowOperationsError(
            "Slack webhook is unavailable in macOS Keychain"
        ) from exc
    secret = result.stdout.strip()
    validate_slack_webhook_url(secret)
    return secret


def validate_slack_webhook_url(url: str) -> None:
    try:
        parsed = urllib.parse.urlsplit(url)
    except ValueError as exc:
        raise ShadowOperationsError("Slack webhook URL is malformed") from exc
    path_parts = [part for part in parsed.path.split("/") if part]
    if (
        parsed.scheme != "https"
        or parsed.hostname != "hooks.slack.com"
        or parsed.port is not None
        or parsed.username is not None
        or parsed.password is not None
        or len(path_parts) != 4
        or path_parts[0] != "services"
        or parsed.query
        or parsed.fragment
    ):
        raise ShadowOperationsError(
            "Slack webhook must be an exact hooks.slack.com/services URL"
        )


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class SlackWebhookClient:
    def __init__(
        self,
        webhook_url: str,
        *,
        timeout_seconds: float = 10.0,
        opener: Any | None = None,
    ) -> None:
        validate_slack_webhook_url(webhook_url)
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self._url = webhook_url
        self._timeout = timeout_seconds
        self._opener = opener or urllib.request.build_opener(_NoRedirect())

    def send(self, message: Mapping[str, Any]) -> None:
        body = json.dumps(
            dict(message),
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        request = urllib.request.Request(
            self._url,
            data=body,
            headers={
                "Content-Type": "application/json; charset=utf-8",
                "User-Agent": "coinpilot-shadow-notifier/1",
            },
            method="POST",
        )
        try:
            with self._opener.open(request, timeout=self._timeout) as response:
                if int(response.status) != 200:
                    raise SlackDeliveryError(
                        f"Slack returned HTTP {int(response.status)}"
                    )
                response.read(32)
        except urllib.error.HTTPError as exc:
            retry_after: float | None = None
            if exc.code == 429:
                try:
                    retry_after = max(
                        1.0,
                        float(exc.headers.get("Retry-After", "1")),
                    )
                except (TypeError, ValueError):
                    retry_after = 1.0
            raise SlackDeliveryError(
                f"Slack returned HTTP {exc.code}",
                retry_after_seconds=retry_after,
            ) from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise SlackDeliveryError("Slack delivery failed") from exc


def slack_message(notification: Mapping[str, Any]) -> dict[str, str]:
    severity = str(notification.get("severity", "info")).upper()
    topic = str(notification.get("topic", "shadow_event"))
    market = str(notification.get("market", "")).strip()
    payload = notification.get("payload")
    safe_payload = dict(payload) if isinstance(payload, Mapping) else {}
    safe_payload.pop("config", None)
    safe_payload.pop("config_hash", None)
    for field in ("run_id", "market"):
        value = notification.get(field)
        if value is not None and str(value).strip():
            safe_payload.setdefault(field, value)
    safe_payload["simulated"] = True
    safe_payload["orders_sent"] = 0
    encoded = json.dumps(
        safe_payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    market_tag = f"[{market}]" if market else ""
    text = (
        f"[COINPILOT SHADOW]{market_tag}[{severity}] {topic}\n"
        f"`{encoded[:2600]}`"
    )
    return {"text": text}


def run_notifier(
    store: ShadowStore,
    client: SlackWebhookClient,
    *,
    poll_seconds: float = 2.0,
    once: bool = False,
    stop_requested: Callable[[], bool] | None = None,
    sleep: Callable[[float], None] = time.sleep,
    wall_time_ns: Callable[[], int] = time.time_ns,
) -> dict[str, int]:
    if poll_seconds <= 0:
        raise ValueError("poll_seconds must be positive")
    worker = f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:8]}"
    delivered = 0
    failed = 0
    while True:
        if stop_requested is not None and stop_requested():
            break
        rows = store.claim_notifications(
            worker_id=worker,
            now_wall_ns=wall_time_ns(),
            lease_ns=60_000_000_000,
            limit=20,
        )
        for row in rows:
            notification_id = str(row["notification_id"])
            try:
                outbound = dict(row)
                run_id = outbound.get("run_id")
                if run_id:
                    run_status = store.read_status(str(run_id))
                    market = run_status.get("market")
                    if market is not None:
                        outbound["market"] = market
                client.send(slack_message(outbound))
            except SlackDeliveryError as exc:
                failed += 1
                attempts = int(row.get("attempt_count", 0)) + 1
                retry_seconds = (
                    exc.retry_after_seconds
                    if exc.retry_after_seconds is not None
                    else min(300.0, 2.0 ** min(attempts, 8))
                )
                now = wall_time_ns()
                store.record_notification_failure(
                    notification_id,
                    worker_id=worker,
                    now_wall_ns=now,
                    retry_at_wall_ns=now + int(retry_seconds * 1e9),
                    error=type(exc).__name__,
                )
            else:
                delivered += 1
                store.mark_notification_delivered(
                    notification_id,
                    worker_id=worker,
                    delivered_wall_ns=wall_time_ns(),
                )
            # Slack incoming webhooks are intentionally paced globally.
            if not once:
                sleep(1.0)
        if once:
            break
        if not rows:
            sleep(poll_seconds)
    return {"delivered": delivered, "failed": failed}


def _public_status(status: Mapping[str, Any], *, now_wall_ns: int) -> dict[str, Any]:
    allowed = {
        "run_id",
        "market",
        "status",
        "lifecycle_status",
        "started_wall_ns",
        "cash_quote",
        "base_quantity",
        "realized_pnl_quote",
        "cumulative_fees_quote",
        "last_equity_quote",
        "peak_equity_quote",
        "max_drawdown",
        "warmup_books_seen",
        "last_book_wall_ns",
        "halt_reason",
        "decisions",
        "pending_orders",
        "fills",
        "alerts_pending",
        "health",
        "live_order_routing",
    }
    result = {key: status.get(key) for key in allowed if key in status}
    last_book = status.get("last_book_wall_ns")
    result["feed_age_seconds"] = (
        None
        if last_book is None
        else max(0.0, (now_wall_ns - int(last_book)) / 1e9)
    )
    result["simulated"] = True
    result["orders_sent"] = 0
    return result


def _with_readiness(
    status: Mapping[str, Any],
    *,
    stale_after_seconds: int,
) -> dict[str, Any]:
    """Add orthogonal feed-freshness and strategy-readiness fields."""

    result = dict(status)
    feed_age = result.get("feed_age_seconds")
    feed_fresh = (
        feed_age is not None
        and float(feed_age) <= stale_after_seconds
    )
    lifecycle = str(result.get("lifecycle_status") or "unknown")
    ready = lifecycle == "running" and feed_fresh
    if ready:
        reason = "ready"
    elif lifecycle in {"halted_recovery", "stopped"}:
        reason = lifecycle
    elif feed_age is None:
        reason = "feed_unavailable"
    elif not feed_fresh:
        reason = "feed_stale"
    elif lifecycle == "warmup":
        reason = lifecycle
    else:
        reason = "lifecycle_not_running"
    result.update(
        {
            "ready": ready,
            "feed_fresh": feed_fresh,
            "readiness_reason": reason,
        }
    )
    return result


def make_status_handler(
    store: ShadowStore,
    *,
    market: str,
    stale_after_seconds: int,
    wall_time_ns: Callable[[], int] = time.time_ns,
) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "CoinPilotShadow/1"
        sys_version = ""

        def _host_allowed(self) -> bool:
            host = self.headers.get("Host", "").split(":", 1)[0].lower()
            return host in {"127.0.0.1", "localhost"}

        def _send(
            self,
            status_code: int,
            body: bytes,
            content_type: str = "application/json; charset=utf-8",
        ) -> None:
            self.send_response(status_code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Referrer-Policy", "no-referrer")
            self.send_header(
                "Content-Security-Policy",
                "default-src 'self'; style-src 'unsafe-inline'; "
                "script-src 'unsafe-inline'; connect-src 'self'; "
                "base-uri 'none'; object-src 'none'; form-action 'none'; "
                "frame-ancestors 'none'",
            )
            self.end_headers()
            self.wfile.write(body)

        def _json(self, status_code: int, value: Mapping[str, Any]) -> None:
            body = json.dumps(
                dict(value),
                ensure_ascii=False,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
            self._send(status_code, body)

        def do_GET(self) -> None:
            if not self._host_allowed():
                self._json(400, {"error": "invalid_host"})
                return
            path = urllib.parse.urlsplit(self.path).path
            if path in {"/livez", "/health/live"}:
                self._json(200, {"live": True, "orders_sent": 0})
                return
            if path == "/":
                self._send(
                    200,
                    _dashboard_html(market),
                    "text/html; charset=utf-8",
                )
                return
            run_id = store.latest_run_id(market)
            if run_id is None:
                self._json(
                    503,
                    {
                        "error": "no_shadow_run",
                        "ready": False,
                        "feed_fresh": False,
                        "readiness_reason": "no_shadow_run",
                        "orders_sent": 0,
                    },
                )
                return
            now = wall_time_ns()
            status = store.read_status(run_id)
            public = _with_readiness(
                _public_status(status, now_wall_ns=now),
                stale_after_seconds=stale_after_seconds,
            )
            if path in {"/readyz", "/health/ready"}:
                self._json(200 if public["ready"] else 503, public)
                return
            if path in {"/healthz", "/api/status"}:
                self._json(200, public)
                return
            if path == "/api/fills":
                self._json(
                    200,
                    {"run_id": run_id, "fills": store.recent_fills(run_id, limit=100)},
                )
                return
            if path == "/api/equity":
                self._json(
                    200,
                    {"run_id": run_id, "equity": store.read_equity(run_id, limit=500)},
                )
                return
            self._json(404, {"error": "not_found"})

        def do_POST(self) -> None:
            self._json(405, {"error": "read_only"})

        def do_PUT(self) -> None:
            self.do_POST()

        def do_DELETE(self) -> None:
            self.do_POST()

        def log_message(self, format: str, *args: Any) -> None:
            return

    return Handler


def serve_status(
    store: ShadowStore,
    *,
    host: str,
    port: int,
    market: str,
    stale_after_seconds: int,
) -> None:
    if host != "127.0.0.1":
        raise ShadowOperationsError("status server may bind only to 127.0.0.1")
    if isinstance(port, bool) or not isinstance(port, int) or not 1024 <= port <= 65535:
        raise ValueError("port must be between 1024 and 65535")
    handler = make_status_handler(
        store,
        market=market,
        stale_after_seconds=stale_after_seconds,
    )
    server = ThreadingHTTPServer((host, port), handler)
    server.daemon_threads = True
    try:
        server.serve_forever(poll_interval=0.5)
    finally:
        server.server_close()


def run_watchdog(
    store: ShadowStore,
    *,
    market: str,
    stale_after_seconds: int,
    now_wall_ns: int | None = None,
) -> dict[str, Any]:
    """Insert one idempotent outbox alert when the latest feed is stale."""

    now = time.time_ns() if now_wall_ns is None else now_wall_ns
    run_id = store.latest_run_id(market)
    if run_id is None:
        return {"status": "no_run", "alert_enqueued": False}
    status = store.read_status(run_id)
    anchor = status.get("last_book_wall_ns") or status.get("started_wall_ns")
    if anchor is None:
        return {"status": "no_time_anchor", "alert_enqueued": False}
    age_ns = max(0, now - int(anchor))
    if age_ns <= stale_after_seconds * 1_000_000_000:
        return {
            "status": "ok",
            "feed_age_seconds": age_ns / 1e9,
            "alert_enqueued": False,
        }
    alert_key = f"watchdog-stale:{run_id}:{int(anchor)}"
    stable_created = int(anchor) + stale_after_seconds * 1_000_000_000
    with store.write_transaction() as connection:
        store.enqueue_notification(
            connection,
            run_id=run_id,
            alert_key=alert_key,
            topic="shadow_feed_stale",
            severity="critical",
            payload={
                "run_id": run_id,
                "last_feed_wall_ns": int(anchor),
                "stale_after_seconds": stale_after_seconds,
                "orders_sent": 0,
            },
            created_wall_ns=stable_created,
        )
    return {
        "status": "critical",
        "feed_age_seconds": age_ns / 1e9,
        "alert_enqueued": True,
    }
