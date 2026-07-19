from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

import pytest

from coinpilot.shadow_ops import (
    ShadowOperationsError,
    SlackDeliveryError,
    SlackWebhookClient,
    make_status_handler,
    run_notifier,
    slack_message,
    validate_slack_webhook_url,
)
from coinpilot.hft_shadow import ShadowConfig, ShadowEngine
from coinpilot.hft_shadow_store import ShadowStore


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
            "payload": {
                "orders_sent": 999,
                "config": {"secret": "must-not-appear"},
            },
        }
    )
    assert "orders_sent\":0" in message["text"]
    assert "simulated\":true" in message["text"]
    assert "secret" not in message["text"]


class _NoRunStore:
    def latest_run_id(self):
        return None


def test_local_status_server_has_no_mutation_endpoint() -> None:
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0),
        make_status_handler(_NoRunStore(), stale_after_seconds=60),
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
