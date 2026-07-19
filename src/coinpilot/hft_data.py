from __future__ import annotations

import json
import math
import os
import time
import uuid
from collections.abc import Callable, Iterable, Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


HFT_EVENT_SCHEMA_VERSION = 1
UPBIT_PUBLIC_WEBSOCKET_URL = "wss://api.upbit.com/websocket/v1"
MAX_ORDERBOOK_DEPTH = 30
MAX_CAPTURE_SECONDS = 3_600.0


class HFTDataValidationError(ValueError):
    """Raised when an HFT market-data event violates the normalized contract."""


class HFTCaptureError(RuntimeError):
    """Raised when the bounded public market-data capture cannot run."""


@dataclass(frozen=True, slots=True)
class HFTDataQualityProfile:
    total_records: int
    valid_events: int
    field_error_count: int
    field_error_examples: tuple[str, ...]
    orderbook_events: int
    trade_events: int
    markets: tuple[str, ...]
    coverage_seconds: float
    overall_event_rate_hz: float | None
    orderbook_event_rate_hz: float | None
    trade_event_rate_hz: float | None
    invalid_or_nonpositive_spread_count: int
    duplicate_trade_sequence_id_count: int
    receive_timestamp_regression_count: int
    exchange_timestamp_regression_count: int
    lag_sample_count: int
    receive_minus_exchange_lag_ms_p50: float | None
    receive_minus_exchange_lag_ms_p95: float | None
    receive_minus_exchange_lag_ms_p99: float | None
    first_received_at_ns: int | None
    last_received_at_ns: int | None
    first_exchange_timestamp_ms: int | None
    last_exchange_timestamp_ms: int | None

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        # Nanosecond epoch values exceed JavaScript's safe integer range.
        # Serialize them losslessly for browser/report consumers while keeping
        # integers inside Python for arithmetic.
        for field in ("first_received_at_ns", "last_received_at_ns"):
            if result[field] is not None:
                result[field] = str(result[field])
        return result


@dataclass(frozen=True, slots=True)
class HFTCaptureResult:
    output_path: Path
    requested_duration_seconds: float
    elapsed_seconds: float
    raw_messages: int
    written_events: int
    rejected_messages: int
    quality: HFTDataQualityProfile

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["output_path"] = str(self.output_path)
        result["quality"] = self.quality.to_dict()
        return result


def _required_text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise HFTDataValidationError(f"{field} must be a non-empty string")
    return value.strip()


def _required_int(value: Any, field: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool):
        raise HFTDataValidationError(f"{field} must be an integer")
    if isinstance(value, float) and not value.is_integer():
        raise HFTDataValidationError(f"{field} must be an integer")
    try:
        result = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise HFTDataValidationError(f"{field} must be an integer") from exc
    if result < minimum:
        raise HFTDataValidationError(f"{field} must be >= {minimum}")
    return result


def _required_float(
    value: Any,
    field: str,
    *,
    positive: bool = False,
    nonnegative: bool = False,
) -> float:
    if isinstance(value, bool):
        raise HFTDataValidationError(f"{field} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise HFTDataValidationError(f"{field} must be numeric") from exc
    if not math.isfinite(result):
        raise HFTDataValidationError(f"{field} must be finite")
    if positive and result <= 0:
        raise HFTDataValidationError(f"{field} must be positive")
    if nonnegative and result < 0:
        raise HFTDataValidationError(f"{field} must be non-negative")
    return result


def _optional_float(
    value: Any,
    field: str,
    *,
    positive: bool = False,
    nonnegative: bool = False,
) -> float | None:
    if value is None:
        return None
    return _required_float(
        value,
        field,
        positive=positive,
        nonnegative=nonnegative,
    )


def _validate_depth(max_depth: int) -> int:
    depth = _required_int(max_depth, "max_depth", minimum=1)
    if depth > MAX_ORDERBOOK_DEPTH:
        raise HFTDataValidationError(
            f"max_depth cannot exceed {MAX_ORDERBOOK_DEPTH}"
        )
    return depth


def _decode_message(message: Mapping[str, Any] | str | bytes) -> Mapping[str, Any]:
    if isinstance(message, Mapping):
        return message
    if isinstance(message, bytes):
        try:
            message = message.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise HFTDataValidationError(
                "Upbit message was not valid UTF-8"
            ) from exc
    if not isinstance(message, str):
        raise HFTDataValidationError(
            "Upbit message must be a mapping, JSON string, or bytes"
        )
    try:
        payload = json.loads(message)
    except json.JSONDecodeError as exc:
        raise HFTDataValidationError("Upbit message was not valid JSON") from exc
    if not isinstance(payload, Mapping):
        raise HFTDataValidationError("Upbit message must decode to an object")
    return payload


def _validate_market(market: Any, expected_market: str | None) -> str:
    result = _required_text(market, "market").upper()
    parts = result.split("-")
    if len(parts) != 2 or any(not part.isalnum() for part in parts):
        raise HFTDataValidationError(
            "market must use Upbit's QUOTE-BASE form, for example KRW-BTC"
        )
    if expected_market is not None and result != expected_market.upper():
        raise HFTDataValidationError(
            f"message market {result} does not match expected {expected_market.upper()}"
        )
    return result


def _normalize_orderbook(
    payload: Mapping[str, Any],
    *,
    received_at_ns: int,
    max_depth: int,
    expected_market: str | None,
) -> dict[str, Any]:
    market = _validate_market(payload.get("code"), expected_market)
    exchange_timestamp_ms = _required_int(
        payload.get("timestamp"),
        "timestamp",
        minimum=1,
    )
    raw_units = payload.get("orderbook_units")
    if not isinstance(raw_units, list) or not raw_units:
        raise HFTDataValidationError(
            "orderbook_units must be a non-empty list"
        )

    units: list[dict[str, float | int]] = []
    for index, raw_unit in enumerate(raw_units[:max_depth], start=1):
        if not isinstance(raw_unit, Mapping):
            raise HFTDataValidationError(
                f"orderbook_units[{index - 1}] must be an object"
            )
        units.append(
            {
                "level": index,
                "ask_price": _required_float(
                    raw_unit.get("ask_price"),
                    f"orderbook_units[{index - 1}].ask_price",
                    positive=True,
                ),
                "ask_size": _required_float(
                    raw_unit.get("ask_size"),
                    f"orderbook_units[{index - 1}].ask_size",
                    nonnegative=True,
                ),
                "bid_price": _required_float(
                    raw_unit.get("bid_price"),
                    f"orderbook_units[{index - 1}].bid_price",
                    positive=True,
                ),
                "bid_size": _required_float(
                    raw_unit.get("bid_size"),
                    f"orderbook_units[{index - 1}].bid_size",
                    nonnegative=True,
                ),
            }
        )

    ask_prices = [float(unit["ask_price"]) for unit in units]
    bid_prices = [float(unit["bid_price"]) for unit in units]
    if any(current < previous for previous, current in zip(ask_prices, ask_prices[1:])):
        raise HFTDataValidationError(
            "orderbook ask prices must be non-decreasing by level"
        )
    if any(current > previous for previous, current in zip(bid_prices, bid_prices[1:])):
        raise HFTDataValidationError(
            "orderbook bid prices must be non-increasing by level"
        )

    best = units[0]
    return {
        "schema_version": HFT_EVENT_SCHEMA_VERSION,
        "event_type": "orderbook",
        "market": market,
        "exchange_timestamp_ms": exchange_timestamp_ms,
        "received_at_ns": str(received_at_ns),
        "sequence_id": None,
        "best_ask_price": best["ask_price"],
        "best_ask_size": best["ask_size"],
        "best_bid_price": best["bid_price"],
        "best_bid_size": best["bid_size"],
        "total_ask_size": _required_float(
            payload.get("total_ask_size"),
            "total_ask_size",
            nonnegative=True,
        ),
        "total_bid_size": _required_float(
            payload.get("total_bid_size"),
            "total_bid_size",
            nonnegative=True,
        ),
        "depth": len(units),
        "levels": units,
    }


def _normalize_trade(
    payload: Mapping[str, Any],
    *,
    received_at_ns: int,
    expected_market: str | None,
) -> dict[str, Any]:
    market = _validate_market(payload.get("code"), expected_market)
    raw_side = _required_text(payload.get("ask_bid"), "ask_bid").upper()
    if raw_side not in {"BID", "ASK"}:
        raise HFTDataValidationError("ask_bid must be BID or ASK")

    return {
        "schema_version": HFT_EVENT_SCHEMA_VERSION,
        "event_type": "trade",
        "market": market,
        "exchange_timestamp_ms": _required_int(
            payload.get("trade_timestamp"),
            "trade_timestamp",
            minimum=1,
        ),
        "received_at_ns": str(received_at_ns),
        # Upbit trade IDs currently exceed IEEE-754 safe integer precision.
        "sequence_id": str(
            _required_int(
                payload.get("sequential_id"),
                "sequential_id",
                minimum=0,
            )
        ),
        "trade_price": _required_float(
            payload.get("trade_price"),
            "trade_price",
            positive=True,
        ),
        "trade_volume": _required_float(
            payload.get("trade_volume"),
            "trade_volume",
            positive=True,
        ),
        "aggressor_side": "buy" if raw_side == "BID" else "sell",
        "best_ask_price": _optional_float(
            payload.get("best_ask_price"),
            "best_ask_price",
            positive=True,
        ),
        "best_ask_size": _optional_float(
            payload.get("best_ask_size"),
            "best_ask_size",
            nonnegative=True,
        ),
        "best_bid_price": _optional_float(
            payload.get("best_bid_price"),
            "best_bid_price",
            positive=True,
        ),
        "best_bid_size": _optional_float(
            payload.get("best_bid_size"),
            "best_bid_size",
            nonnegative=True,
        ),
    }


def normalize_upbit_message(
    message: Mapping[str, Any] | str | bytes,
    *,
    received_at_ns: int | None = None,
    max_depth: int = MAX_ORDERBOOK_DEPTH,
    expected_market: str | None = None,
) -> dict[str, Any]:
    """Normalize one Upbit public orderbook or trade WebSocket message.

    The returned dictionary is directly serializable as one JSONL record.  The
    exchange timestamp is retained in milliseconds while the local receive
    timestamp uses nanoseconds so downstream replay can distinguish the clocks.
    """

    payload = _decode_message(message)
    receive_ns = _required_int(
        time.time_ns() if received_at_ns is None else received_at_ns,
        "received_at_ns",
        minimum=1,
    )
    depth = _validate_depth(max_depth)
    event_type = _required_text(payload.get("type"), "type").lower()
    if event_type == "orderbook":
        return _normalize_orderbook(
            payload,
            received_at_ns=receive_ns,
            max_depth=depth,
            expected_market=expected_market,
        )
    if event_type == "trade":
        return _normalize_trade(
            payload,
            received_at_ns=receive_ns,
            expected_market=expected_market,
        )
    raise HFTDataValidationError(
        f"unsupported public WebSocket message type: {event_type}"
    )


def validate_hft_event(event: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and canonicalize one already-normalized event."""

    if not isinstance(event, Mapping):
        raise HFTDataValidationError("normalized event must be an object")
    schema_version = _required_int(
        event.get("schema_version"),
        "schema_version",
        minimum=1,
    )
    if schema_version != HFT_EVENT_SCHEMA_VERSION:
        raise HFTDataValidationError(
            f"unsupported schema_version {schema_version}"
        )

    event_type = _required_text(event.get("event_type"), "event_type").lower()
    market = _validate_market(event.get("market"), None)
    exchange_timestamp_ms = _required_int(
        event.get("exchange_timestamp_ms"),
        "exchange_timestamp_ms",
        minimum=1,
    )
    received_at_ns = _required_int(
        event.get("received_at_ns"),
        "received_at_ns",
        minimum=1,
    )

    if event_type == "orderbook":
        raw_levels = event.get("levels")
        if not isinstance(raw_levels, list) or not raw_levels:
            raise HFTDataValidationError("levels must be a non-empty list")
        depth = _required_int(event.get("depth"), "depth", minimum=1)
        if depth > MAX_ORDERBOOK_DEPTH or depth != len(raw_levels):
            raise HFTDataValidationError(
                "depth must equal levels length and cannot exceed 30"
            )
        levels: list[dict[str, float | int]] = []
        for index, raw_level in enumerate(raw_levels, start=1):
            if not isinstance(raw_level, Mapping):
                raise HFTDataValidationError(
                    f"levels[{index - 1}] must be an object"
                )
            level_number = _required_int(
                raw_level.get("level"),
                f"levels[{index - 1}].level",
                minimum=1,
            )
            if level_number != index:
                raise HFTDataValidationError(
                    "level numbers must be contiguous and one-based"
                )
            levels.append(
                {
                    "level": level_number,
                    "ask_price": _required_float(
                        raw_level.get("ask_price"),
                        f"levels[{index - 1}].ask_price",
                        positive=True,
                    ),
                    "ask_size": _required_float(
                        raw_level.get("ask_size"),
                        f"levels[{index - 1}].ask_size",
                        nonnegative=True,
                    ),
                    "bid_price": _required_float(
                        raw_level.get("bid_price"),
                        f"levels[{index - 1}].bid_price",
                        positive=True,
                    ),
                    "bid_size": _required_float(
                        raw_level.get("bid_size"),
                        f"levels[{index - 1}].bid_size",
                        nonnegative=True,
                    ),
                }
            )
        ask_prices = [float(level["ask_price"]) for level in levels]
        bid_prices = [float(level["bid_price"]) for level in levels]
        if any(
            current < previous
            for previous, current in zip(ask_prices, ask_prices[1:])
        ):
            raise HFTDataValidationError(
                "orderbook ask prices must be non-decreasing by level"
            )
        if any(
            current > previous
            for previous, current in zip(bid_prices, bid_prices[1:])
        ):
            raise HFTDataValidationError(
                "orderbook bid prices must be non-increasing by level"
            )

        best = levels[0]
        declared_best = {
            "best_ask_price": _required_float(
                event.get("best_ask_price"),
                "best_ask_price",
                positive=True,
            ),
            "best_ask_size": _required_float(
                event.get("best_ask_size"),
                "best_ask_size",
                nonnegative=True,
            ),
            "best_bid_price": _required_float(
                event.get("best_bid_price"),
                "best_bid_price",
                positive=True,
            ),
            "best_bid_size": _required_float(
                event.get("best_bid_size"),
                "best_bid_size",
                nonnegative=True,
            ),
        }
        for name, value in declared_best.items():
            level_name = name.removeprefix("best_")
            if value != best[level_name]:
                raise HFTDataValidationError(
                    f"{name} must match the first orderbook level"
                )

        return {
            "schema_version": schema_version,
            "event_type": event_type,
            "market": market,
            "exchange_timestamp_ms": exchange_timestamp_ms,
            "received_at_ns": str(received_at_ns),
            "sequence_id": None,
            **declared_best,
            "total_ask_size": _required_float(
                event.get("total_ask_size"),
                "total_ask_size",
                nonnegative=True,
            ),
            "total_bid_size": _required_float(
                event.get("total_bid_size"),
                "total_bid_size",
                nonnegative=True,
            ),
            "depth": depth,
            "levels": levels,
        }

    if event_type == "trade":
        sequence_id = _required_int(
            event.get("sequence_id"),
            "sequence_id",
            minimum=0,
        )
        aggressor_side = _required_text(
            event.get("aggressor_side"),
            "aggressor_side",
        ).lower()
        if aggressor_side not in {"buy", "sell"}:
            raise HFTDataValidationError(
                "aggressor_side must be buy or sell"
            )
        return {
            "schema_version": schema_version,
            "event_type": event_type,
            "market": market,
            "exchange_timestamp_ms": exchange_timestamp_ms,
            "received_at_ns": str(received_at_ns),
            "sequence_id": str(sequence_id),
            "trade_price": _required_float(
                event.get("trade_price"),
                "trade_price",
                positive=True,
            ),
            "trade_volume": _required_float(
                event.get("trade_volume"),
                "trade_volume",
                positive=True,
            ),
            "aggressor_side": aggressor_side,
            "best_ask_price": _optional_float(
                event.get("best_ask_price"),
                "best_ask_price",
                positive=True,
            ),
            "best_ask_size": _optional_float(
                event.get("best_ask_size"),
                "best_ask_size",
                nonnegative=True,
            ),
            "best_bid_price": _optional_float(
                event.get("best_bid_price"),
                "best_bid_price",
                positive=True,
            ),
            "best_bid_size": _optional_float(
                event.get("best_bid_size"),
                "best_bid_size",
                nonnegative=True,
            ),
        }

    raise HFTDataValidationError(
        f"normalized event_type must be orderbook or trade, got {event_type}"
    )


def read_hft_jsonl(
    path: str | Path,
    *,
    strict: bool = True,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    with Path(path).open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                decoded = json.loads(line)
                events.append(validate_hft_event(decoded))
            except (json.JSONDecodeError, HFTDataValidationError, TypeError) as exc:
                if strict:
                    raise HFTDataValidationError(
                        f"invalid HFT JSONL record at line {line_number}: {exc}"
                    ) from exc
    return events


def _percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return float(ordered[lower])
    fraction = position - lower
    return float(
        ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction
    )


def _event_rate(count: int, coverage_seconds: float) -> float | None:
    if coverage_seconds <= 0:
        return None
    return count / coverage_seconds


def _profile_validated_events(
    events: Iterable[Mapping[str, Any]],
    *,
    total_records: int | None = None,
    initial_errors: Iterable[str] = (),
) -> HFTDataQualityProfile:
    validated: list[dict[str, Any]] = []
    errors = list(initial_errors)
    records_seen = 0
    for index, event in enumerate(events, start=1):
        records_seen += 1
        try:
            validated.append(validate_hft_event(event))
        except (HFTDataValidationError, TypeError) as exc:
            if len(errors) < 10:
                errors.append(f"record {index}: {exc}")

    if total_records is None:
        total_records = records_seen + len(tuple(initial_errors))
    receive_times = [int(event["received_at_ns"]) for event in validated]
    exchange_times = [event["exchange_timestamp_ms"] for event in validated]
    first_receive = min(receive_times) if receive_times else None
    last_receive = max(receive_times) if receive_times else None
    coverage_seconds = (
        (last_receive - first_receive) / 1_000_000_000
        if first_receive is not None
        and last_receive is not None
        and last_receive > first_receive
        else 0.0
    )

    orderbook_count = sum(
        event["event_type"] == "orderbook" for event in validated
    )
    trade_count = sum(event["event_type"] == "trade" for event in validated)
    invalid_spreads = 0
    duplicate_sequences = 0
    seen_trade_sequences: set[tuple[str, int]] = set()
    receive_regressions = 0
    exchange_regressions = 0
    previous_receive: int | None = None
    previous_exchange: dict[tuple[str, str], int] = {}
    lags_ms: list[float] = []

    for event in validated:
        ask = event.get("best_ask_price")
        bid = event.get("best_bid_price")
        if ask is not None and bid is not None and float(ask) - float(bid) <= 0:
            invalid_spreads += 1

        if event["event_type"] == "trade":
            sequence_key = (event["market"], str(event["sequence_id"]))
            if sequence_key in seen_trade_sequences:
                duplicate_sequences += 1
            else:
                seen_trade_sequences.add(sequence_key)

        received_at_ns = int(event["received_at_ns"])
        if previous_receive is not None and received_at_ns < previous_receive:
            receive_regressions += 1
        previous_receive = received_at_ns

        exchange_key = (event["market"], event["event_type"])
        exchange_timestamp_ms = event["exchange_timestamp_ms"]
        if (
            exchange_key in previous_exchange
            and exchange_timestamp_ms < previous_exchange[exchange_key]
        ):
            exchange_regressions += 1
        previous_exchange[exchange_key] = exchange_timestamp_ms
        lags_ms.append(received_at_ns / 1_000_000 - exchange_timestamp_ms)

    return HFTDataQualityProfile(
        total_records=total_records,
        valid_events=len(validated),
        field_error_count=total_records - len(validated),
        field_error_examples=tuple(errors[:10]),
        orderbook_events=orderbook_count,
        trade_events=trade_count,
        markets=tuple(sorted({event["market"] for event in validated})),
        coverage_seconds=coverage_seconds,
        overall_event_rate_hz=_event_rate(len(validated), coverage_seconds),
        orderbook_event_rate_hz=_event_rate(
            orderbook_count, coverage_seconds
        ),
        trade_event_rate_hz=_event_rate(trade_count, coverage_seconds),
        invalid_or_nonpositive_spread_count=invalid_spreads,
        duplicate_trade_sequence_id_count=duplicate_sequences,
        receive_timestamp_regression_count=receive_regressions,
        exchange_timestamp_regression_count=exchange_regressions,
        lag_sample_count=len(lags_ms),
        receive_minus_exchange_lag_ms_p50=_percentile(lags_ms, 0.50),
        receive_minus_exchange_lag_ms_p95=_percentile(lags_ms, 0.95),
        receive_minus_exchange_lag_ms_p99=_percentile(lags_ms, 0.99),
        first_received_at_ns=first_receive,
        last_received_at_ns=last_receive,
        first_exchange_timestamp_ms=min(exchange_times)
        if exchange_times
        else None,
        last_exchange_timestamp_ms=max(exchange_times)
        if exchange_times
        else None,
    )


def profile_hft_events(
    events: Iterable[Mapping[str, Any]],
) -> HFTDataQualityProfile:
    """Profile normalized events without failing the whole batch on bad rows."""

    materialized = list(events)
    return _profile_validated_events(
        materialized,
        total_records=len(materialized),
    )


def profile_hft_jsonl(path: str | Path) -> HFTDataQualityProfile:
    events: list[Mapping[str, Any]] = []
    errors: list[str] = []
    total_records = 0
    with Path(path).open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            total_records += 1
            try:
                decoded = json.loads(line)
                if not isinstance(decoded, Mapping):
                    raise HFTDataValidationError(
                        "record must decode to an object"
                    )
                events.append(decoded)
            except (json.JSONDecodeError, HFTDataValidationError) as exc:
                if len(errors) < 10:
                    errors.append(f"line {line_number}: {exc}")
    return _profile_validated_events(
        events,
        total_records=total_records,
        initial_errors=errors,
    )


def _load_websocket_factory() -> tuple[Callable[..., Any], tuple[type[BaseException], ...]]:
    try:
        import websocket  # type: ignore[import-not-found]
    except ImportError as exc:
        raise HFTCaptureError(
            "Public WebSocket capture requires the optional "
            "'websocket-client' package"
        ) from exc
    return websocket.create_connection, (websocket.WebSocketTimeoutException,)


def capture_upbit_public(
    *,
    market: str,
    duration_seconds: float,
    output_path: str | Path,
    max_depth: int = MAX_ORDERBOOK_DEPTH,
    connection_factory: Callable[..., Any] | None = None,
    monotonic: Callable[[], float] = time.monotonic,
    now_ns: Callable[[], int] = time.time_ns,
) -> HFTCaptureResult:
    """Capture a bounded stream from Upbit's public orderbook/trade endpoint.

    This function subscribes only to unauthenticated public channels and has no
    order or account code path.  It writes to a temporary sibling file, flushes
    valid normalized JSONL records, and atomically replaces the destination only
    after a normal bounded capture.
    """

    normalized_market = _validate_market(market, None)
    try:
        duration = float(duration_seconds)
    except (TypeError, ValueError, OverflowError) as exc:
        raise HFTCaptureError("duration_seconds must be numeric") from exc
    if not math.isfinite(duration) or duration <= 0:
        raise HFTCaptureError("duration_seconds must be finite and positive")
    if duration > MAX_CAPTURE_SECONDS:
        raise HFTCaptureError(
            f"duration_seconds cannot exceed {MAX_CAPTURE_SECONDS:g}"
        )
    depth = _validate_depth(max_depth)

    timeout_errors: tuple[type[BaseException], ...] = (TimeoutError,)
    if connection_factory is None:
        connection_factory, websocket_timeout_errors = _load_websocket_factory()
        timeout_errors = (*timeout_errors, *websocket_timeout_errors)

    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(
        f".{destination.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    )
    subscription = [
        {"ticket": f"coinpilot-{uuid.uuid4()}"},
        {
            "type": "orderbook",
            "codes": [normalized_market],
            "is_only_realtime": True,
        },
        {
            "type": "trade",
            "codes": [normalized_market],
            "is_only_realtime": True,
        },
        {"format": "DEFAULT"},
    ]

    connection: Any | None = None
    start = monotonic()
    raw_messages = 0
    written_events = 0
    rejected_messages = 0
    completed = False
    try:
        connection = connection_factory(
            UPBIT_PUBLIC_WEBSOCKET_URL,
            timeout=min(5.0, duration),
        )
        connection.send(
            json.dumps(subscription, separators=(",", ":"), ensure_ascii=False)
        )
        with temporary.open("w", encoding="utf-8") as handle:
            deadline = start + duration
            while True:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    break
                if hasattr(connection, "settimeout"):
                    connection.settimeout(max(0.001, min(1.0, remaining)))
                try:
                    message = connection.recv()
                except timeout_errors:
                    continue
                if message is None:
                    break
                raw_messages += 1
                try:
                    event = normalize_upbit_message(
                        message,
                        received_at_ns=now_ns(),
                        max_depth=depth,
                        expected_market=normalized_market,
                    )
                except HFTDataValidationError:
                    rejected_messages += 1
                    continue
                handle.write(
                    json.dumps(
                        event,
                        separators=(",", ":"),
                        sort_keys=True,
                        ensure_ascii=False,
                    )
                    + "\n"
                )
                written_events += 1
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
        completed = True
    except HFTCaptureError:
        raise
    except Exception as exc:
        raise HFTCaptureError(f"Upbit public WebSocket capture failed: {exc}") from exc
    finally:
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass
        if not completed:
            temporary.unlink(missing_ok=True)

    elapsed = max(0.0, monotonic() - start)
    return HFTCaptureResult(
        output_path=destination,
        requested_duration_seconds=duration,
        elapsed_seconds=elapsed,
        raw_messages=raw_messages,
        written_events=written_events,
        rejected_messages=rejected_messages,
        quality=profile_hft_jsonl(destination),
    )
