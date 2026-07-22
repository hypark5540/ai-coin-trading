from __future__ import annotations

import json
import threading
import urllib.error
import urllib.parse
import urllib.request
from html.parser import HTMLParser
from http.server import ThreadingHTTPServer

import pytest

from coinpilot.shadow_ops import (
    ShadowOperationsError,
    SlackDeliveryError,
    SlackWebhookClient,
    make_status_handler,
    run_notifier,
    run_watchdog,
    slack_message,
    validate_slack_webhook_url,
)
from coinpilot.hft_shadow import ShadowConfig, ShadowEngine
from coinpilot.hft_shadow_store import ShadowStore
from coinpilot.shadow_dashboard import dashboard_html


class _Response:
    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def read(self, maximum: int) -> bytes:
        return b"ok"


class _Opener:
    def __init__(self) -> None:
        self.requests = []

    def open(self, request, *, timeout):
        self.requests.append((request, timeout))
        return _Response()


VALID_TEST_WEBHOOK = "/".join(
    ("https://hooks.slack.com", "services", "T", "B", "X")
)


@pytest.mark.parametrize(
    "url",
    [
        "http://hooks.slack.com/services/T/B/X",
        "https://example.com/services/T/B/X",
        "https://hooks.slack.com/other/T/B/X",
        "https://hooks.slack.com/services/T/B",
        f"{VALID_TEST_WEBHOOK}?leak=1",
    ],
)
def test_slack_webhook_validation_is_fail_closed(url: str) -> None:
    with pytest.raises(ShadowOperationsError):
        validate_slack_webhook_url(url)


def test_slack_client_posts_bounded_json_without_redirects() -> None:
    opener = _Opener()
    client = SlackWebhookClient(
        VALID_TEST_WEBHOOK,
        opener=opener,
    )
    client.send({"text": "shadow only"})

    request, timeout = opener.requests[0]
    assert request.full_url == VALID_TEST_WEBHOOK
    assert json.loads(request.data) == {"text": "shadow only"}
    assert timeout == 10.0


def test_slack_message_forces_shadow_provenance() -> None:
    message = slack_message(
        {
            "severity": "critical",
            "topic": "test",
            "market": "KRW-ETH",
            "run_id": "eth-run",
            "payload": {
                "orders_sent": 999,
                "config": {"secret": "must-not-appear"},
            },
        }
    )
    assert "orders_sent\":0" in message["text"]
    assert "simulated\":true" in message["text"]
    assert "[KRW-ETH]" in message["text"]
    assert "eth-run" in message["text"]
    assert "secret" not in message["text"]


class _NoRunStore:
    def __init__(self) -> None:
        self.markets: list[str] = []

    def latest_run_id(self, market):
        self.markets.append(market)
        return None


class _DashboardContractParser(HTMLParser):
    """Collect structural and resource-loading contracts without pinning layout."""

    def __init__(self) -> None:
        super().__init__()
        self.tags: list[str] = []
        self.url_attributes: list[tuple[str, str, str]] = []
        self.has_live_region = False
        self.html_language: str | None = None

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        self.tags.append(tag)
        attributes = dict(attrs)
        if tag == "html":
            self.html_language = attributes.get("lang")
        if (
            attributes.get("aria-live") in {"polite", "assertive"}
            or attributes.get("role") in {"alert", "status"}
        ):
            self.has_live_region = True
        for name in ("src", "href", "action", "poster"):
            value = attributes.get(name)
            if value:
                self.url_attributes.append((tag, name, value))


def test_dashboard_root_is_a_self_contained_read_only_interface() -> None:
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0),
        make_status_handler(
            _NoRunStore(),
            market="KRW-BTC",
            stale_after_seconds=60,
        ),
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"
    try:
        with urllib.request.urlopen(f"{base}/", timeout=2) as response:
            assert response.status == 200
            assert response.headers.get_content_type() == "text/html"
            assert response.headers.get_content_charset() == "utf-8"
            assert response.headers["Cache-Control"] == "no-store"
            content_security_policy = response.headers[
                "Content-Security-Policy"
            ]
            assert "default-src 'self'" in content_security_policy
            assert "connect-src 'self'" in content_security_policy
            dashboard = response.read().decode("utf-8")
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    parser = _DashboardContractParser()
    parser.feed(dashboard)
    assert parser.html_language
    assert {"header", "main"} <= set(parser.tags)
    assert parser.tags.count("section") + parser.tags.count("article") >= 3
    assert parser.has_live_region

    for endpoint in ("/api/status", "/api/equity", "/api/fills"):
        assert endpoint in dashboard

    # The dashboard is one inline document: no CDN, analytics, remote fonts,
    # images, forms, or other resource-loading escape hatch.
    non_resource_svg_namespace = "http://www.w3.org/2000/svg"
    dashboard_without_svg_namespace = dashboard.replace(
        non_resource_svg_namespace,
        "",
    )
    assert "http://" not in dashboard_without_svg_namespace
    assert "https://" not in dashboard_without_svg_namespace
    external_url_attributes = [
        item
        for item in parser.url_attributes
        if (
            urllib.parse.urlsplit(item[2]).scheme not in {"", "data"}
            or urllib.parse.urlsplit(item[2]).netloc
        )
    ]
    assert external_url_attributes == []
    assert "form" not in parser.tags

    # Safety state is rendered from the server's forced shadow-only fields.
    normalized = dashboard.casefold()
    assert (
        "read only" in normalized
        or "read-only" in normalized
        or "읽기 전용" in dashboard
    )
    for safety_field in ("simulated", "orders_sent", "live_order_routing"):
        assert safety_field in dashboard


def test_dashboard_uses_configured_market_units_before_first_status() -> None:
    dashboard = dashboard_html("KRW-ETH").decode("utf-8")

    assert "KRW—ETH" in dashboard
    assert "0 ETH held" in dashboard
    assert 'const configuredMarket = "KRW-ETH";' in dashboard
    assert "BTC held" not in dashboard
    assert "formatKrw" not in dashboard


class _MarketStore:
    def __init__(self) -> None:
        self.latest_calls: list[str] = []
        self.runs = {
            "KRW-BTC": "btc-run",
            "KRW-ETH": "eth-run",
        }

    def latest_run_id(self, market):
        self.latest_calls.append(market)
        return self.runs.get(market)

    def read_status(self, run_id):
        market = "KRW-BTC" if run_id == "btc-run" else "KRW-ETH"
        return {
            "run_id": run_id,
            "market": market,
            "status": "running",
            "lifecycle_status": "running",
            "started_wall_ns": 1,
            "last_book_wall_ns": 900,
            "live_order_routing": False,
        }

    def recent_fills(self, run_id, *, limit):
        return [{"run_id": run_id, "limit": limit}]

    def read_equity(self, run_id, *, limit):
        return [{"run_id": run_id, "limit": limit}]


@pytest.mark.parametrize(
    ("market", "expected_run"),
    [("KRW-BTC", "btc-run"), ("KRW-ETH", "eth-run")],
)
def test_status_server_never_crosses_configured_market(
    market: str,
    expected_run: str,
) -> None:
    store = _MarketStore()
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0),
        make_status_handler(
            store,
            market=market,
            stale_after_seconds=60,
            wall_time_ns=lambda: 1_000,
        ),
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"
    try:
        for endpoint in ("/api/status", "/api/fills", "/api/equity"):
            with urllib.request.urlopen(f"{base}{endpoint}", timeout=2) as response:
                payload = json.loads(response.read())
            assert payload["run_id"] == expected_run
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    assert store.latest_calls == [market, market, market]


def test_status_and_watchdog_do_not_fallback_to_another_market() -> None:
    store = _NoRunStore()
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0),
        make_status_handler(
            store,
            market="KRW-ETH",
            stale_after_seconds=60,
        ),
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with pytest.raises(urllib.error.HTTPError) as raised:
            urllib.request.urlopen(
                f"http://127.0.0.1:{server.server_port}/api/status",
                timeout=2,
            )
        assert raised.value.code == 503
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    result = run_watchdog(
        store,
        market="KRW-ETH",
        stale_after_seconds=60,
        now_wall_ns=1_000,
    )
    assert result == {"status": "no_run", "alert_enqueued": False}
    assert store.markets == ["KRW-ETH", "KRW-ETH"]


def test_local_status_server_has_no_mutation_endpoint() -> None:
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0),
        make_status_handler(
            _NoRunStore(),
            market="KRW-BTC",
            stale_after_seconds=60,
        ),
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"
    try:
        with urllib.request.urlopen(f"{base}/livez", timeout=2) as response:
            assert response.status == 200
            assert json.loads(response.read())["orders_sent"] == 0
            assert response.headers["Cache-Control"] == "no-store"
        request = urllib.request.Request(
            f"{base}/api/status",
            data=b"{}",
            method="POST",
        )
        with pytest.raises(urllib.error.HTTPError) as raised:
            urllib.request.urlopen(request, timeout=2)
        assert raised.value.code == 405
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_notifier_delivers_and_commits_the_outbox_lease(tmp_path) -> None:
    store = ShadowStore(tmp_path / "shadow.db")
    engine, _ = ShadowEngine.start(
        store,
        ShadowConfig(),
        run_id="notify-run",
        started_wall_ns=1_000_000_000,
    )

    class Client:
        def __init__(self) -> None:
            self.messages = []

        def send(self, message) -> None:
            self.messages.append(message)

    client = Client()
    result = run_notifier(
        store,
        client,
        once=True,
        wall_time_ns=lambda: 2_000_000_000,
    )

    assert result == {"delivered": 1, "failed": 0}
    assert len(client.messages) == 1
    assert "orders_sent\":0" in client.messages[0]["text"]
    assert store.pending_notifications(run_id=engine.run_id) == []
