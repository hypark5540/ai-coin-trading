from __future__ import annotations

import pandas as pd
import pytest

from coinpilot.data import (
    UpbitAPIError,
    UpbitCandleClient,
    candle_gaps,
    closed_candles,
    synthetic_candles,
    validate_candles,
)


def test_synthetic_candles_respect_ohlcv_invariants() -> None:
    candles = synthetic_candles(200)
    validated = validate_candles(candles)
    assert len(validated) == 200
    assert validated["timestamp"].is_monotonic_increasing
    assert (validated["low"] <= validated["open"]).all()
    assert (validated["high"] >= validated["close"]).all()


def test_incomplete_last_candle_is_removed() -> None:
    candles = synthetic_candles(100, interval_minutes=60)
    now = candles.iloc[-1]["timestamp"] + pd.Timedelta(minutes=30)
    filtered = closed_candles(candles, 60, now=now)
    assert len(filtered) == len(candles) - 1
    assert filtered.iloc[-1]["timestamp"] == candles.iloc[-2]["timestamp"]


def test_gap_audit_does_not_fill_missing_candles() -> None:
    candles = synthetic_candles(120).drop(index=60).reset_index(drop=True)
    gaps = candle_gaps(candles, 60)
    assert len(gaps) == 1
    assert gaps.iloc[0]["elapsed_minutes"] == 120
    assert len(candles) == 119


class _FakeResponse:
    def __init__(self, payload, *, headers=None):
        self.status_code = 200
        self.headers = {
            "Remaining-Req": "group=candles; min=1800; sec=9",
            **(headers or {}),
        }
        self.text = ""
        self._payload = payload

    def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, pages):
        self.pages = list(pages)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return _FakeResponse(self.pages.pop(0))


def _upbit_payload(timestamp: pd.Timestamp, price: float) -> dict[str, object]:
    return {
        "market": "KRW-BTC",
        "candle_date_time_utc": timestamp.strftime("%Y-%m-%dT%H:%M:%S"),
        "opening_price": price,
        "high_price": price + 2,
        "low_price": price - 2,
        "trade_price": price + 1,
        "candle_acc_trade_volume": 3.0,
        "candle_acc_trade_price": price * 3,
    }


def test_upbit_pagination_uses_exclusive_oldest_cursor() -> None:
    end = pd.Timestamp("2026-01-10T00:00:00Z")
    first = [
        _upbit_payload(end - pd.Timedelta(hours=index), 1000 - index)
        for index in range(200)
    ]
    second = [_upbit_payload(end - pd.Timedelta(hours=200), 800)]
    session = _FakeSession([first, second])
    client = UpbitCandleClient(
        session=session,
        sleep=lambda _: None,
        monotonic=lambda: 0.0,
    )

    candles = client.fetch_candles(
        market="KRW-BTC", interval_minutes=60, count=201
    )

    assert len(candles) == 201
    assert candles["timestamp"].is_monotonic_increasing
    assert session.calls[1][1]["params"]["to"] == (
        end - pd.Timedelta(hours=199)
    ).isoformat().replace("+00:00", "Z")
    assert "Origin" not in session.calls[0][1]["headers"]


def _ticker_payload(
    *,
    market: str = "KRW-BTC",
    price: float = 50_000_000.0,
    trade_timestamp: object = 1_767_225_600_000,
    timestamp: object = 1_767_225_660_000,
) -> dict[str, object]:
    return {
        "market": market,
        "trade_price": price,
        "trade_timestamp": trade_timestamp,
        "timestamp": timestamp,
    }


def test_upbit_fetch_ticker_returns_exchange_and_observation_times() -> None:
    requested_at = pd.Timestamp("2026-01-01T00:00:00Z")
    observed_at = pd.Timestamp("2026-01-01T00:00:00.250Z")
    clock = iter([requested_at, observed_at])
    session = _FakeSession([[_ticker_payload()]])
    client = UpbitCandleClient(
        session=session,
        sleep=lambda _: None,
        monotonic=lambda: 0.0,
        now=lambda: next(clock),
    )

    ticker = client.fetch_ticker(market="KRW-BTC")

    assert ticker.market == "KRW-BTC"
    assert ticker.price == 50_000_000.0
    assert ticker.exchange_timestamp == pd.Timestamp("2026-01-01T00:00:00Z")
    assert ticker.observed_at == observed_at
    assert session.calls[0][0].endswith("/v1/ticker")
    assert session.calls[0][1]["params"] == {"markets": "KRW-BTC"}


def test_upbit_fetch_ticker_rejects_wrong_market() -> None:
    session = _FakeSession([[_ticker_payload(market="KRW-ETH")]])
    client = UpbitCandleClient(
        session=session,
        sleep=lambda _: None,
        monotonic=lambda: 0.0,
    )

    with pytest.raises(UpbitAPIError, match="market"):
        client.fetch_ticker(market="KRW-BTC")


@pytest.mark.parametrize("price", [0.0, float("nan")])
def test_upbit_fetch_ticker_rejects_invalid_price(price: float) -> None:
    session = _FakeSession([[_ticker_payload(price=price)]])
    client = UpbitCandleClient(
        session=session,
        sleep=lambda _: None,
        monotonic=lambda: 0.0,
    )

    with pytest.raises(UpbitAPIError, match="price"):
        client.fetch_ticker(market="KRW-BTC")


@pytest.mark.parametrize(
    "payload",
    [
        [],
        [_ticker_payload(), _ticker_payload()],
        [
            {
                "market": "KRW-BTC",
                "trade_price": 50_000_000.0,
                "timestamp": 1_767_225_600_000,
            }
        ],
        [_ticker_payload(trade_timestamp="not-a-timestamp")],
    ],
)
def test_upbit_fetch_ticker_rejects_invalid_schema(payload: object) -> None:
    session = _FakeSession([payload])
    client = UpbitCandleClient(
        session=session,
        sleep=lambda _: None,
        monotonic=lambda: 0.0,
    )

    with pytest.raises(UpbitAPIError):
        client.fetch_ticker(market="KRW-BTC")


def test_upbit_fetch_ticker_uses_last_trade_not_last_changed_time() -> None:
    session = _FakeSession(
        [
            [
                _ticker_payload(
                    trade_timestamp=1_767_225_600_000,
                    timestamp=1_767_290_400_000,
                )
            ]
        ]
    )
    client = UpbitCandleClient(
        session=session,
        sleep=lambda _: None,
        monotonic=lambda: 0.0,
    )

    ticker = client.fetch_ticker(market="KRW-BTC")

    assert ticker.exchange_timestamp == pd.Timestamp("2026-01-01T00:00:00Z")
    assert ticker.exchange_timestamp != pd.Timestamp("2026-01-01T18:00:00Z")


def test_fetch_candles_captures_conservative_request_time_cutoff() -> None:
    current_start = pd.Timestamp("2026-01-01T00:00:00Z")
    requested_at = pd.Timestamp("2026-01-01T01:00:04Z")
    observed_at = pd.Timestamp("2026-01-01T01:00:06Z")
    clock = iter([requested_at, observed_at])
    session = _FakeSession(
        [
            [
                _upbit_payload(current_start, 1000),
                _upbit_payload(current_start - pd.Timedelta(hours=1), 900),
            ]
        ]
    )
    client = UpbitCandleClient(
        session=session,
        sleep=lambda _: None,
        monotonic=lambda: 0.0,
        now=lambda: next(clock),
    )

    candles = client.fetch_candles(
        market="KRW-BTC", interval_minutes=60, count=2
    )
    finalized = closed_candles(
        candles,
        60,
        now=candles.attrs["finalization_cutoff"],
    )

    assert candles.attrs["finalization_cutoff"] == requested_at - pd.Timedelta(
        minutes=1
    )
    assert candles.attrs["observed_at"] == observed_at
    assert len(finalized) == 1
    assert finalized.iloc[-1]["timestamp"] == current_start - pd.Timedelta(hours=1)


def test_fetch_candles_prefers_earlier_http_server_clock() -> None:
    class _ServerClockSession:
        def get(self, *_args, **_kwargs):
            return _FakeResponse(
                [
                    _upbit_payload(
                        pd.Timestamp("2026-01-01T01:00:00Z"), 1000
                    ),
                    _upbit_payload(
                        pd.Timestamp("2026-01-01T00:00:00Z"), 900
                    ),
                ],
                headers={"Date": "Thu, 01 Jan 2026 01:59:57 GMT"},
            )

    clock = iter(
        [
            pd.Timestamp("2026-01-01T02:00:07Z"),
            pd.Timestamp("2026-01-01T02:00:08Z"),
        ]
    )
    client = UpbitCandleClient(
        session=_ServerClockSession(),
        sleep=lambda _: None,
        monotonic=lambda: 0.0,
        now=lambda: next(clock),
    )

    candles = client.fetch_candles(
        market="KRW-BTC", interval_minutes=60, count=2
    )

    assert candles.attrs["finalization_cutoff"] == pd.Timestamp(
        "2026-01-01T01:59:57Z"
    )
