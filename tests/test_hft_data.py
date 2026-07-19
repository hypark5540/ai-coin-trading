from __future__ import annotations

import json
from pathlib import Path

import pytest

from coinpilot.hft_data import (
    HFTCaptureError,
    HFTDataValidationError,
    MAX_CAPTURE_SECONDS,
    UPBIT_PUBLIC_WEBSOCKET_URL,
    capture_upbit_public,
    normalize_upbit_message,
    profile_hft_events,
    profile_hft_jsonl,
    read_hft_jsonl,
    validate_hft_event,
)


def _orderbook_message(
    *,
    timestamp: int = 1_000,
    best_ask: float = 101.0,
    best_bid: float = 100.0,
) -> dict[str, object]:
    return {
        "type": "orderbook",
        "code": "KRW-BTC",
        "timestamp": timestamp,
        "total_ask_size": 3.0,
        "total_bid_size": 4.0,
        "orderbook_units": [
            {
                "ask_price": best_ask,
                "ask_size": 1.0,
                "bid_price": best_bid,
                "bid_size": 1.5,
            },
            {
                "ask_price": best_ask + 1,
                "ask_size": 2.0,
                "bid_price": best_bid - 1,
                "bid_size": 2.5,
            },
        ],
    }


def _trade_message(
    *,
    timestamp: int = 1_000,
    sequence_id: int = 7,
    ask_bid: str = "BID",
    best_ask: float = 101.0,
    best_bid: float = 100.0,
) -> dict[str, object]:
    return {
        "type": "trade",
        "code": "KRW-BTC",
        "trade_timestamp": timestamp,
        "sequential_id": sequence_id,
        "trade_price": 100.5,
        "trade_volume": 0.01,
        "ask_bid": ask_bid,
        "best_ask_price": best_ask,
        "best_ask_size": 1.0,
        "best_bid_price": best_bid,
        "best_bid_size": 1.5,
    }


def test_normalize_orderbook_preserves_clocks_best_quote_and_depth() -> None:
    event = normalize_upbit_message(
        json.dumps(_orderbook_message()).encode(),
        received_at_ns=1_100_000_000,
        max_depth=1,
        expected_market="KRW-BTC",
    )

    assert event["event_type"] == "orderbook"
    assert event["exchange_timestamp_ms"] == 1_000
    assert event["received_at_ns"] == "1100000000"
    assert event["best_ask_price"] == 101.0
    assert event["best_bid_price"] == 100.0
    assert event["depth"] == 1
    assert event["levels"] == [
        {
            "level": 1,
            "ask_price": 101.0,
            "ask_size": 1.0,
            "bid_price": 100.0,
            "bid_size": 1.5,
        }
    ]
    assert validate_hft_event(event) == event


def test_normalize_trade_maps_aggressor_and_keeps_sequence_id() -> None:
    event = normalize_upbit_message(
        _trade_message(ask_bid="ASK"),
        received_at_ns=1_200_000_000,
    )

    assert event["event_type"] == "trade"
    assert event["sequence_id"] == "7"
    assert event["aggressor_side"] == "sell"
    assert event["trade_price"] == 100.5
    assert validate_hft_event(event) == event


@pytest.mark.parametrize(
    ("message", "match"),
    [
        ({"type": "ticker", "code": "KRW-BTC"}, "unsupported"),
        (_orderbook_message(best_ask=float("nan")), "finite"),
        (
            {**_trade_message(), "sequential_id": None},
            "sequential_id",
        ),
        (
            {**_trade_message(), "code": "KRW-ETH"},
            "expected",
        ),
    ],
)
def test_normalize_rejects_bad_public_messages(
    message: dict[str, object],
    match: str,
) -> None:
    with pytest.raises(HFTDataValidationError, match=match):
        normalize_upbit_message(
            message,
            received_at_ns=1_200_000_000,
            expected_market="KRW-BTC",
        )


def test_orderbook_levels_must_be_price_sorted() -> None:
    message = _orderbook_message()
    units = list(message["orderbook_units"])
    units[1] = {**units[1], "ask_price": 99.0}
    message["orderbook_units"] = units

    with pytest.raises(HFTDataValidationError, match="non-decreasing"):
        normalize_upbit_message(message, received_at_ns=1_100_000_000)


def test_quality_profile_counts_errors_spreads_duplicates_and_regressions() -> None:
    orderbook_1 = normalize_upbit_message(
        _orderbook_message(timestamp=1_000),
        received_at_ns=1_100_000_000,
    )
    orderbook_2 = normalize_upbit_message(
        _orderbook_message(
            timestamp=900,
            best_ask=100.0,
            best_bid=100.0,
        ),
        received_at_ns=1_200_000_000,
    )
    trade_1 = normalize_upbit_message(
        _trade_message(timestamp=1_000, sequence_id=7),
        received_at_ns=1_300_000_000,
    )
    trade_2 = normalize_upbit_message(
        _trade_message(timestamp=1_100, sequence_id=7),
        received_at_ns=1_250_000_000,
    )
    bad = {**trade_2, "trade_volume": -1}

    profile = profile_hft_events(
        [orderbook_1, orderbook_2, trade_1, trade_2, bad]
    )

    assert profile.total_records == 5
    assert profile.valid_events == 4
    assert profile.field_error_count == 1
    assert profile.orderbook_events == 2
    assert profile.trade_events == 2
    assert profile.invalid_or_nonpositive_spread_count == 1
    assert profile.duplicate_trade_sequence_id_count == 1
    assert profile.receive_timestamp_regression_count == 1
    assert profile.exchange_timestamp_regression_count == 1
    assert profile.coverage_seconds == pytest.approx(0.2)
    assert profile.overall_event_rate_hz == pytest.approx(20.0)
    assert profile.receive_minus_exchange_lag_ms_p50 == pytest.approx(225.0)
    assert profile.receive_minus_exchange_lag_ms_p95 == pytest.approx(300.0)
    assert profile.receive_minus_exchange_lag_ms_p99 == pytest.approx(300.0)


def test_jsonl_profile_records_parse_and_schema_errors(tmp_path: Path) -> None:
    good = normalize_upbit_message(
        _trade_message(),
        received_at_ns=1_100_000_000,
    )
    path = tmp_path / "events.jsonl"
    path.write_text(
        json.dumps(good) + "\n{not-json}\n" + json.dumps({"foo": "bar"}) + "\n",
        encoding="utf-8",
    )

    profile = profile_hft_jsonl(path)

    assert profile.total_records == 3
    assert profile.valid_events == 1
    assert profile.field_error_count == 2
    assert len(profile.field_error_examples) == 2
    with pytest.raises(HFTDataValidationError, match="line 2"):
        read_hft_jsonl(path)
    assert read_hft_jsonl(path, strict=False) == [good]


class _FakeConnection:
    def __init__(self, messages: list[object]) -> None:
        self.messages = list(messages)
        self.sent: list[str] = []
        self.timeouts: list[float] = []
        self.closed = False

    def send(self, message: str) -> None:
        self.sent.append(message)

    def settimeout(self, timeout: float) -> None:
        self.timeouts.append(timeout)

    def recv(self) -> object:
        if self.messages:
            return self.messages.pop(0)
        raise TimeoutError

    def close(self) -> None:
        self.closed = True


def test_capture_uses_only_public_channels_and_writes_atomic_jsonl(
    tmp_path: Path,
) -> None:
    connection = _FakeConnection(
        [
            json.dumps(_orderbook_message()).encode(),
            json.dumps(_trade_message()),
            json.dumps({"type": "ticker", "code": "KRW-BTC"}),
        ]
    )
    factory_calls: list[tuple[str, float]] = []

    def factory(url: str, *, timeout: float) -> _FakeConnection:
        factory_calls.append((url, timeout))
        return connection

    current = -0.001

    def monotonic() -> float:
        nonlocal current
        current += 0.001
        return current

    output = tmp_path / "nested" / "capture.jsonl"
    result = capture_upbit_public(
        market="KRW-BTC",
        duration_seconds=0.01,
        output_path=output,
        connection_factory=factory,
        monotonic=monotonic,
        now_ns=iter([1_100_000_000, 1_200_000_000, 1_300_000_000]).__next__,
    )

    assert factory_calls == [(UPBIT_PUBLIC_WEBSOCKET_URL, 0.01)]
    subscription = json.loads(connection.sent[0])
    assert {item.get("type") for item in subscription if "type" in item} == {
        "orderbook",
        "trade",
    }
    assert all("authorization" not in item for item in subscription)
    assert connection.closed
    assert result.raw_messages == 3
    assert result.written_events == 2
    assert result.rejected_messages == 1
    assert result.quality.valid_events == 2
    assert output.exists()
    assert len(read_hft_jsonl(output)) == 2
    assert not list(output.parent.glob("*.tmp"))


@pytest.mark.parametrize(
    "duration",
    [0, -1, float("nan"), MAX_CAPTURE_SECONDS + 1],
)
def test_capture_rejects_unbounded_or_invalid_duration(
    tmp_path: Path,
    duration: float,
) -> None:
    with pytest.raises(HFTCaptureError, match="duration_seconds"):
        capture_upbit_public(
            market="KRW-BTC",
            duration_seconds=duration,
            output_path=tmp_path / "events.jsonl",
            connection_factory=lambda *_args, **_kwargs: None,
        )
