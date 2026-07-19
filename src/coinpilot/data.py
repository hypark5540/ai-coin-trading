from __future__ import annotations

import hashlib
import math
import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Any

import numpy as np
import pandas as pd
import requests


CANDLE_COLUMNS = (
    "timestamp",
    "market",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
)


class CandleValidationError(ValueError):
    """Raised when OHLCV data violates an invariant."""


class UpbitAPIError(RuntimeError):
    """Raised when the Upbit public API cannot provide valid data."""


class UpbitRateLimitError(UpbitAPIError):
    """Raised when Upbit temporarily blocks this client."""


@dataclass(frozen=True, slots=True)
class MarketTicker:
    market: str
    price: float
    exchange_timestamp: pd.Timestamp
    observed_at: pd.Timestamp


def candle_data_hash(frame: pd.DataFrame) -> str:
    """Return a stable hash of the validated source rows used by a model."""
    candles = validate_candles(frame)
    values = pd.util.hash_pandas_object(
        candles.loc[:, CANDLE_COLUMNS],
        index=False,
    ).to_numpy()
    return hashlib.sha256(values.tobytes()).hexdigest()


def validate_candles(frame: pd.DataFrame) -> pd.DataFrame:
    missing = set(CANDLE_COLUMNS) - set(frame.columns)
    if missing:
        raise CandleValidationError(
            "Missing candle columns: " + ", ".join(sorted(missing))
        )

    result = frame.loc[:, CANDLE_COLUMNS].copy()
    result["timestamp"] = pd.to_datetime(result["timestamp"], utc=True, errors="raise")
    numeric_columns = ("open", "high", "low", "close", "volume", "quote_volume")
    for column in numeric_columns:
        result[column] = pd.to_numeric(result[column], errors="raise").astype(float)

    if result.empty:
        raise CandleValidationError("Candle data is empty")
    if result["timestamp"].duplicated().any():
        raise CandleValidationError("Candle timestamps must be unique")
    if not result["timestamp"].is_monotonic_increasing:
        raise CandleValidationError("Candle timestamps must increase monotonically")
    if result["market"].isna().any() or result["market"].nunique() != 1:
        raise CandleValidationError("A candle frame must contain exactly one market")

    prices = result.loc[:, ("open", "high", "low", "close")]
    if (~np.isfinite(prices.to_numpy())).any() or (prices <= 0).any().any():
        raise CandleValidationError("OHLC prices must be finite and positive")
    if (result["high"] < result["low"]).any():
        raise CandleValidationError("Candle high cannot be below low")
    if (
        (result["open"] > result["high"])
        | (result["open"] < result["low"])
        | (result["close"] > result["high"])
        | (result["close"] < result["low"])
    ).any():
        raise CandleValidationError("Open and close must be within low/high")
    if (
        (~np.isfinite(result[["volume", "quote_volume"]].to_numpy())).any()
        or (result[["volume", "quote_volume"]] < 0).any().any()
    ):
        raise CandleValidationError("Volumes must be finite and non-negative")

    return result.reset_index(drop=True)


def closed_candles(
    frame: pd.DataFrame,
    interval_minutes: int,
    *,
    now: datetime | pd.Timestamp | None = None,
    close_grace_seconds: float = 5.0,
) -> pd.DataFrame:
    validated = validate_candles(frame)
    current_time = pd.Timestamp.now(tz="UTC") if now is None else pd.Timestamp(now)
    if current_time.tzinfo is None:
        current_time = current_time.tz_localize("UTC")
    else:
        current_time = current_time.tz_convert("UTC")
    closes_at = (
        validated["timestamp"]
        + pd.Timedelta(minutes=interval_minutes)
        + pd.Timedelta(seconds=close_grace_seconds)
    )
    filtered = validated.loc[closes_at <= current_time]
    if filtered.empty:
        raise CandleValidationError("No fully closed candles are available")
    return filtered.reset_index(drop=True)


def candle_gaps(frame: pd.DataFrame, interval_minutes: int) -> pd.DataFrame:
    validated = validate_candles(frame)
    delta = validated["timestamp"].diff()
    expected = pd.Timedelta(minutes=interval_minutes)
    mask = delta.notna() & (delta != expected)
    if not mask.any():
        return pd.DataFrame(
            columns=["previous_timestamp", "timestamp", "elapsed_minutes"]
        )
    indexes = validated.index[mask]
    return pd.DataFrame(
        {
            "previous_timestamp": validated.loc[indexes - 1, "timestamp"].to_numpy(),
            "timestamp": validated.loc[indexes, "timestamp"].to_numpy(),
            "elapsed_minutes": (
                delta.loc[indexes].dt.total_seconds().to_numpy() / 60
            ),
        }
    ).reset_index(drop=True)


class UpbitCandleClient:
    """Small, rate-limit-aware client for Upbit's public minute candles."""

    _remaining_pattern = re.compile(r"(?:^|;\s*)sec=(\d+)")

    def __init__(
        self,
        *,
        base_url: str = "https://api.upbit.com",
        timeout_seconds: float = 10.0,
        session: Any | None = None,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        now: Callable[[], datetime | pd.Timestamp] | None = None,
        max_retries: int = 4,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.session = session or requests.Session()
        self.sleep = sleep
        self.monotonic = monotonic
        self.now = now or (lambda: pd.Timestamp.now(tz="UTC"))
        self.max_retries = max_retries
        self._last_request_at: float | None = None

    def _pace(self) -> None:
        if self._last_request_at is not None:
            elapsed = self.monotonic() - self._last_request_at
            wait_for = 0.11 - elapsed
            if wait_for > 0:
                self.sleep(wait_for)
        self._last_request_at = self.monotonic()

    def _now_utc(self) -> pd.Timestamp:
        value = pd.Timestamp(self.now())
        if value.tzinfo is None:
            return value.tz_localize("UTC")
        return value.tz_convert("UTC")

    def _request_json(
        self, url: str, params: dict[str, str | int]
    ) -> tuple[Any, pd.Timestamp, pd.Timestamp, pd.Timestamp | None]:
        for attempt in range(self.max_retries + 1):
            self._pace()
            requested_at = self._now_utc()
            try:
                response = self.session.get(
                    url,
                    params=params,
                    headers={
                        "Accept": "application/json",
                        "User-Agent": "coinpilot/0.1",
                    },
                    timeout=self.timeout_seconds,
                )
            except requests.RequestException as exc:
                if attempt >= self.max_retries:
                    raise UpbitAPIError(f"Upbit request failed: {exc}") from exc
                self.sleep(min(0.5 * (2**attempt), 4.0))
                continue

            status = int(response.status_code)
            if status == 418:
                raise UpbitRateLimitError(
                    "Upbit temporarily blocked this IP after repeated rate-limit violations"
                )
            if status == 429:
                if attempt >= self.max_retries:
                    raise UpbitRateLimitError("Upbit candle rate limit was exhausted")
                self.sleep(min(1.0 + 0.25 * attempt, 2.0))
                continue
            if status >= 500:
                if attempt >= self.max_retries:
                    raise UpbitAPIError(f"Upbit server error: HTTP {status}")
                self.sleep(min(0.5 * (2**attempt), 4.0))
                continue
            if status != 200:
                body = getattr(response, "text", "")
                raise UpbitAPIError(f"Upbit returned HTTP {status}: {body[:300]}")

            remaining = response.headers.get("Remaining-Req", "")
            match = self._remaining_pattern.search(remaining)
            if match and int(match.group(1)) == 0:
                self.sleep(1.05)

            try:
                payload = response.json()
            except ValueError as exc:
                raise UpbitAPIError("Upbit returned invalid JSON") from exc
            observed_at = self._now_utc()
            server_time: pd.Timestamp | None = None
            date_header = response.headers.get("Date")
            if date_header:
                try:
                    server_time = pd.Timestamp(parsedate_to_datetime(date_header))
                    if server_time.tzinfo is None:
                        server_time = server_time.tz_localize("UTC")
                    else:
                        server_time = server_time.tz_convert("UTC")
                except (TypeError, ValueError, OverflowError):
                    server_time = None
            return payload, requested_at, observed_at, server_time

        raise AssertionError("unreachable")

    def _request_page(
        self, market: str, interval_minutes: int, count: int, to: str | None
    ) -> tuple[
        list[dict[str, Any]],
        pd.Timestamp,
        pd.Timestamp,
        pd.Timestamp | None,
    ]:
        url = f"{self.base_url}/v1/candles/minutes/{interval_minutes}"
        params: dict[str, str | int] = {"market": market, "count": count}
        if to is not None:
            params["to"] = to
        payload, requested_at, observed_at, server_time = self._request_json(
            url, params
        )
        if not isinstance(payload, list):
            raise UpbitAPIError("Upbit candle response was not a list")
        return payload, requested_at, observed_at, server_time

    def fetch_ticker(self, *, market: str) -> MarketTicker:
        payload, _, observed_at, _ = self._request_json(
            f"{self.base_url}/v1/ticker", {"markets": market}
        )
        if not isinstance(payload, list) or len(payload) != 1:
            raise UpbitAPIError("Upbit ticker response was not a one-item list")
        item = payload[0]
        try:
            response_market = str(item["market"])
            price = float(item["trade_price"])
            exchange_timestamp = pd.to_datetime(
                int(item["trade_timestamp"]), unit="ms", utc=True
            )
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise UpbitAPIError("Upbit ticker payload has an invalid schema") from exc
        if response_market != market:
            raise UpbitAPIError("Upbit ticker market did not match the request")
        if not math.isfinite(price) or price <= 0:
            raise UpbitAPIError("Upbit ticker price must be finite and positive")
        return MarketTicker(
            market=response_market,
            price=price,
            exchange_timestamp=pd.Timestamp(exchange_timestamp),
            observed_at=observed_at,
        )

    def fetch_candles(
        self,
        *,
        market: str,
        interval_minutes: int,
        count: int,
        end_time: datetime | pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        if interval_minutes not in (1, 3, 5, 10, 15, 30, 60, 240):
            raise ValueError("Unsupported Upbit minute interval")
        if count < 1:
            raise ValueError("count must be positive")

        cursor: str | None = None
        if end_time is not None:
            cursor_timestamp = pd.Timestamp(end_time)
            if cursor_timestamp.tzinfo is None:
                cursor_timestamp = cursor_timestamp.tz_localize("UTC")
            else:
                cursor_timestamp = cursor_timestamp.tz_convert("UTC")
            cursor = cursor_timestamp.isoformat().replace("+00:00", "Z")

        pages: list[dict[str, Any]] = []
        previous_cursor: str | None = None
        finalization_cutoff: pd.Timestamp | None = None
        newest_page_observed_at: pd.Timestamp | None = None
        while len(pages) < count:
            page_size = min(200, count - len(pages))
            page, requested_at, observed_at, server_time = self._request_page(
                market=market,
                interval_minutes=interval_minutes,
                count=page_size,
                to=cursor,
            )
            if finalization_cutoff is None:
                # HTTP Date is the exchange-facing server clock. Use the earlier
                # of it and the local request clock so a fast local clock cannot
                # finalize a still-open candle. If Date is absent, keep a
                # conservative one-minute clock-skew reserve.
                finalization_cutoff = (
                    min(requested_at, server_time)
                    if server_time is not None
                    else requested_at - pd.Timedelta(minutes=1)
                )
                newest_page_observed_at = observed_at
            if not page:
                break
            pages.extend(page)

            try:
                timestamps = [
                    pd.Timestamp(item["candle_date_time_utc"], tz="UTC")
                    for item in page
                ]
            except (KeyError, TypeError, ValueError) as exc:
                raise UpbitAPIError(
                    "Upbit candle payload has an invalid timestamp"
                ) from exc
            oldest = min(timestamps)
            cursor = oldest.isoformat().replace("+00:00", "Z")
            if cursor == previous_cursor:
                break
            previous_cursor = cursor
            if len(page) < page_size:
                break

        if not pages:
            raise UpbitAPIError("Upbit returned no candle data")

        records = []
        for item in pages:
            try:
                records.append(
                    {
                        "timestamp": pd.Timestamp(
                            item["candle_date_time_utc"], tz="UTC"
                        ),
                        "market": str(item["market"]),
                        "open": float(item["opening_price"]),
                        "high": float(item["high_price"]),
                        "low": float(item["low_price"]),
                        "close": float(item["trade_price"]),
                        "volume": float(item["candle_acc_trade_volume"]),
                        "quote_volume": float(item["candle_acc_trade_price"]),
                    }
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise UpbitAPIError("Upbit candle payload has an invalid schema") from exc

        frame = pd.DataFrame.from_records(records)
        frame = (
            frame.sort_values("timestamp")
            .drop_duplicates("timestamp", keep="last")
            .tail(count)
            .reset_index(drop=True)
        )
        if set(frame["market"]) != {market}:
            raise UpbitAPIError("Upbit candle market did not match the request")
        validated = validate_candles(frame)
        validated.attrs["finalization_cutoff"] = finalization_cutoff
        validated.attrs["observed_at"] = newest_page_observed_at
        return validated


def synthetic_candles(
    count: int = 1800,
    *,
    market: str = "KRW-BTC",
    interval_minutes: int = 60,
    seed: int = 7,
) -> pd.DataFrame:
    if count < 100:
        raise ValueError("Synthetic series needs at least 100 candles")
    generator = np.random.default_rng(seed)

    noise = generator.normal(0.0, 0.006, count)
    returns = np.zeros(count)
    regimes = np.sin(np.arange(count) / 120.0) * 0.0007
    for index in range(1, count):
        returns[index] = (
            regimes[index] + 0.14 * returns[index - 1] + noise[index]
        )

    opening = np.empty(count)
    closing = np.empty(count)
    opening[0] = 50_000_000.0
    closing[0] = opening[0] * math.exp(returns[0])
    for index in range(1, count):
        opening[index] = closing[index - 1]
        closing[index] = opening[index] * math.exp(returns[index])

    intrabar = np.abs(generator.normal(0.004, 0.002, count))
    high = np.maximum(opening, closing) * (1 + intrabar)
    low = np.minimum(opening, closing) * np.maximum(1 - intrabar, 0.01)
    volume = generator.lognormal(mean=2.0, sigma=0.45, size=count) * (
        1 + np.abs(returns) * 20
    )

    end = pd.Timestamp("2026-01-01T00:00:00Z")
    timestamps = pd.date_range(
        end=end,
        periods=count,
        freq=pd.Timedelta(minutes=interval_minutes),
        tz="UTC",
    )
    return validate_candles(
        pd.DataFrame(
            {
                "timestamp": timestamps,
                "market": market,
                "open": opening,
                "high": high,
                "low": low,
                "close": closing,
                "volume": volume,
                "quote_volume": volume * closing,
            }
        )
    )
