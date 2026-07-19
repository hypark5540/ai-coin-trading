"""Deterministic visible-depth taker execution for public-book research.

This module intentionally models independent orders, not a portfolio strategy.
It sweeps aggregated public depth at the first eligible order book after a
receive-monotonic latency deadline.

Explicit assumptions:

* displayed aggregated depth is the only liquidity available;
* every execution is a marketable taker order;
* there is no hidden liquidity, maker queue priority, replenishment, or
  market-impact recovery;
* visible-depth exhaustion produces a partial fill;
* replayed orders are independent and do not deplete one another's books.

The archive converter consumes the envelope written by the HFT capture layer.
``connection_id`` is the continuity boundary.  Storage-file segment names are
deliberately ignored because file rotation is not a market-data discontinuity.
"""

from __future__ import annotations

import bisect
import math
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal


OrderSide = Literal["buy", "sell"]
RequestKind = Literal["base_quantity", "quote_notional"]
ExecutionStatus = Literal["filled", "partial"]
ReplayStatus = Literal["filled", "partial", "invalid", "unavailable"]

VISIBLE_TAKER_ASSUMPTIONS = (
    "aggregated visible public depth only",
    "marketable taker execution",
    "no hidden liquidity or maker queue priority",
    "no replenishment or market-impact recovery",
    "orders replay independently without shared depth depletion",
)


class DepthBookValidationError(ValueError):
    """Raised when an archive record, book, order, or config is invalid."""


def _positive_float(value: Any, field: str) -> float:
    if isinstance(value, bool):
        raise DepthBookValidationError(f"{field} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise DepthBookValidationError(f"{field} must be numeric") from exc
    if not math.isfinite(result) or result <= 0:
        raise DepthBookValidationError(f"{field} must be positive and finite")
    return result


def _integer(value: Any, field: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool):
        raise DepthBookValidationError(f"{field} must be an integer")
    if isinstance(value, float) and not value.is_integer():
        raise DepthBookValidationError(f"{field} must be an integer")
    try:
        result = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise DepthBookValidationError(f"{field} must be an integer") from exc
    if result < minimum:
        raise DepthBookValidationError(f"{field} must be >= {minimum}")
    return result


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DepthBookValidationError(f"{field} must be a non-empty string")
    return value.strip()


def _strict_bool(value: Any, field: str) -> bool:
    if not isinstance(value, bool):
        raise DepthBookValidationError(f"{field} must be boolean")
    return value


@dataclass(frozen=True, slots=True)
class DepthLevel:
    """One displayed price level measured in base-asset quantity."""

    price: float
    base_size: float

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "price",
            _positive_float(self.price, "level.price"),
        )
        object.__setattr__(
            self,
            "base_size",
            _positive_float(self.base_size, "level.base_size"),
        )


def _level(
    value: DepthLevel | tuple[float, float] | list[float],
    field: str,
) -> DepthLevel:
    if isinstance(value, DepthLevel):
        return value
    if not isinstance(value, (tuple, list)) or len(value) != 2:
        raise DepthBookValidationError(
            f"{field} must contain DepthLevel or (price, base_size) pairs"
        )
    return DepthLevel(value[0], value[1])


def _levels(
    values: Iterable[DepthLevel | tuple[float, float] | list[float]],
    *,
    side: Literal["ask", "bid"],
) -> tuple[DepthLevel, ...]:
    materialized = tuple(_level(value, f"{side}s") for value in values)
    if not materialized:
        raise DepthBookValidationError(f"{side}s cannot be empty")
    ordered = tuple(
        sorted(
            materialized,
            key=lambda item: item.price,
            reverse=side == "bid",
        )
    )
    prices = [item.price for item in ordered]
    if len(prices) != len(set(prices)):
        raise DepthBookValidationError(
            f"{side}s cannot contain duplicate prices"
        )
    return ordered


@dataclass(frozen=True, slots=True)
class _ArchiveEnvelope:
    source_index: int
    capture_id: str
    connection_id: str
    ordinal: int
    received_wall_ns: int
    received_monotonic_ns: int
    monotonic_regression: bool
    gap_before: bool
    gap_reason: str | None
    event: Mapping[str, Any]


def _parse_archive_envelope(
    record: Mapping[str, Any],
    source_index: int,
) -> _ArchiveEnvelope:
    if not isinstance(record, Mapping):
        raise DepthBookValidationError("archive record must be an object")
    if _integer(record.get("schema_version"), "schema_version") != 1:
        raise DepthBookValidationError("unsupported archive schema_version")
    event = record.get("event")
    if not isinstance(event, Mapping):
        raise DepthBookValidationError("archive event must be an object")
    gap_before = _strict_bool(record.get("gap_before"), "gap_before")
    gap_reason_value = record.get("gap_reason")
    gap_reason = (
        None
        if gap_reason_value is None
        else _text(gap_reason_value, "gap_reason")
    )
    if gap_before and gap_reason is None:
        raise DepthBookValidationError(
            "gap_reason is required when gap_before is true"
        )
    if not gap_before and gap_reason is not None:
        raise DepthBookValidationError(
            "gap_reason must be null when gap_before is false"
        )
    return _ArchiveEnvelope(
        source_index=source_index,
        capture_id=_text(record.get("capture_id"), "capture_id"),
        connection_id=_text(record.get("connection_id"), "connection_id"),
        ordinal=_integer(record.get("ordinal"), "ordinal", minimum=1),
        received_wall_ns=_integer(
            record.get("received_wall_ns"),
            "received_wall_ns",
        ),
        received_monotonic_ns=_integer(
            record.get("received_monotonic_ns"),
            "received_monotonic_ns",
        ),
        monotonic_regression=_strict_bool(
            record.get("monotonic_regression"),
            "monotonic_regression",
        ),
        gap_before=gap_before,
        gap_reason=gap_reason,
        event=event,
    )


@dataclass(frozen=True, slots=True)
class PublicOrderBook:
    """One public order book with archive continuity metadata."""

    capture_id: str
    connection_id: str
    ordinal: int
    market: str
    received_monotonic_ns: int
    received_wall_ns: int | None
    exchange_timestamp_ms: int | None
    asks: tuple[DepthLevel, ...]
    bids: tuple[DepthLevel, ...]
    gap_before: bool = False
    gap_reason: str | None = None
    monotonic_regression: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "capture_id",
            _text(self.capture_id, "capture_id"),
        )
        object.__setattr__(
            self,
            "connection_id",
            _text(self.connection_id, "connection_id"),
        )
        object.__setattr__(
            self,
            "ordinal",
            _integer(self.ordinal, "ordinal", minimum=1),
        )
        object.__setattr__(
            self,
            "market",
            _text(self.market, "market").upper(),
        )
        object.__setattr__(
            self,
            "received_monotonic_ns",
            _integer(
                self.received_monotonic_ns,
                "received_monotonic_ns",
            ),
        )
        if self.received_wall_ns is not None:
            object.__setattr__(
                self,
                "received_wall_ns",
                _integer(self.received_wall_ns, "received_wall_ns"),
            )
        if self.exchange_timestamp_ms is not None:
            object.__setattr__(
                self,
                "exchange_timestamp_ms",
                _integer(
                    self.exchange_timestamp_ms,
                    "exchange_timestamp_ms",
                ),
            )
        object.__setattr__(self, "asks", _levels(self.asks, side="ask"))
        object.__setattr__(self, "bids", _levels(self.bids, side="bid"))
        if self.best_ask.price <= self.best_bid.price:
            raise DepthBookValidationError(
                "order book must be uncrossed with best ask above best bid"
            )
        if not isinstance(self.gap_before, bool):
            raise DepthBookValidationError("gap_before must be boolean")
        if not isinstance(self.monotonic_regression, bool):
            raise DepthBookValidationError(
                "monotonic_regression must be boolean"
            )
        if self.gap_before:
            object.__setattr__(
                self,
                "gap_reason",
                _text(self.gap_reason, "gap_reason"),
            )
        elif self.gap_reason is not None:
            raise DepthBookValidationError(
                "gap_reason must be null when gap_before is false"
            )

    @property
    def best_ask(self) -> DepthLevel:
        return self.asks[0]

    @property
    def best_bid(self) -> DepthLevel:
        return self.bids[0]

    @property
    def mid_price(self) -> float:
        return (self.best_ask.price + self.best_bid.price) / 2.0

    @classmethod
    def from_normalized_event(
        cls,
        event: Mapping[str, Any],
        *,
        capture_id: str,
        connection_id: str,
        ordinal: int,
        received_monotonic_ns: int,
        received_wall_ns: int | None = None,
        gap_before: bool = False,
        gap_reason: str | None = None,
        monotonic_regression: bool = False,
    ) -> PublicOrderBook:
        """Build from one normalized ``coinpilot.hft_data`` order-book event."""

        if event.get("event_type") != "orderbook":
            raise DepthBookValidationError(
                "normalized event must have event_type='orderbook'"
            )
        raw_levels = event.get("levels")
        if not isinstance(raw_levels, Sequence) or isinstance(
            raw_levels,
            (str, bytes),
        ):
            raise DepthBookValidationError("levels must be a sequence")
        asks: list[DepthLevel] = []
        bids: list[DepthLevel] = []
        for index, raw in enumerate(raw_levels):
            if not isinstance(raw, Mapping):
                raise DepthBookValidationError(
                    f"levels[{index}] must be an object"
                )
            asks.append(
                DepthLevel(
                    _positive_float(
                        raw.get("ask_price"),
                        f"levels[{index}].ask_price",
                    ),
                    _positive_float(
                        raw.get("ask_size"),
                        f"levels[{index}].ask_size",
                    ),
                )
            )
            bids.append(
                DepthLevel(
                    _positive_float(
                        raw.get("bid_price"),
                        f"levels[{index}].bid_price",
                    ),
                    _positive_float(
                        raw.get("bid_size"),
                        f"levels[{index}].bid_size",
                    ),
                )
            )
        event_wall = (
            None
            if event.get("received_at_ns") is None
            else _integer(event.get("received_at_ns"), "event.received_at_ns")
        )
        selected_wall = event_wall if received_wall_ns is None else received_wall_ns
        if (
            event_wall is not None
            and selected_wall is not None
            and event_wall != _integer(selected_wall, "received_wall_ns")
        ):
            raise DepthBookValidationError(
                "archive wall time disagrees with normalized event"
            )
        book = cls(
            capture_id=capture_id,
            connection_id=connection_id,
            ordinal=ordinal,
            market=_text(event.get("market"), "market"),
            received_monotonic_ns=received_monotonic_ns,
            received_wall_ns=selected_wall,
            exchange_timestamp_ms=(
                None
                if event.get("exchange_timestamp_ms") is None
                else _integer(
                    event.get("exchange_timestamp_ms"),
                    "exchange_timestamp_ms",
                )
            ),
            asks=tuple(asks),
            bids=tuple(bids),
            gap_before=gap_before,
            gap_reason=gap_reason,
            monotonic_regression=monotonic_regression,
        )
        for field, actual in (
            ("best_ask_price", book.best_ask.price),
            ("best_bid_price", book.best_bid.price),
        ):
            if event.get(field) is not None and not math.isclose(
                _positive_float(event[field], field),
                actual,
                rel_tol=0.0,
                abs_tol=1e-12,
            ):
                raise DepthBookValidationError(
                    f"{field} disagrees with sorted visible levels"
                )
        return book

    @classmethod
    def from_archive_envelope(
        cls,
        record: Mapping[str, Any],
    ) -> PublicOrderBook:
        """Build directly from one schema-v1 capture archive envelope."""

        envelope = _parse_archive_envelope(record, 0)
        return cls.from_normalized_event(
            envelope.event,
            capture_id=envelope.capture_id,
            connection_id=envelope.connection_id,
            ordinal=envelope.ordinal,
            received_monotonic_ns=envelope.received_monotonic_ns,
            received_wall_ns=envelope.received_wall_ns,
            gap_before=envelope.gap_before,
            gap_reason=envelope.gap_reason,
            monotonic_regression=envelope.monotonic_regression,
        )


@dataclass(frozen=True, slots=True)
class ConnectionSegmentMetadata:
    """Archive ordinals covered by one capture connection."""

    capture_id: str
    connection_id: str
    first_orderbook_ordinal: int
    last_orderbook_ordinal: int
    orderbook_count: int


@dataclass(frozen=True, slots=True)
class OrderBookRecordLoadResult:
    """Books plus ingestion/rejection and continuity metadata."""

    books: tuple[PublicOrderBook, ...]
    total_records: int
    skipped_non_orderbook_records: int
    rejected_records: int
    rejection_examples: tuple[str, ...]
    capture_ids: tuple[str, ...]
    connection_segments: tuple[ConnectionSegmentMetadata, ...]
    gap_marker_count: int
    ordinal_discontinuity_count: int
    monotonic_regression_marker_count: int
    propagated_gap_to_orderbook_count: int
    reordered_record_count: int


def public_orderbooks_from_records(
    records: Iterable[Mapping[str, Any]],
) -> OrderBookRecordLoadResult:
    """Load archive envelopes in lossless ordinal order.

    Gap or regression markers on intervening trade records are propagated to
    the next order book so a book-only replay cannot silently cross them.
    Duplicate ``(capture_id, ordinal)`` records are rejected.
    """

    materialized = list(records)
    parsed: list[_ArchiveEnvelope] = []
    errors: list[str] = []
    rejected = 0
    for index, record in enumerate(materialized, start=1):
        try:
            parsed.append(_parse_archive_envelope(record, index))
        except (DepthBookValidationError, TypeError) as exc:
            rejected += 1
            if len(errors) < 10:
                errors.append(f"record {index}: {exc}")

    unique: list[_ArchiveEnvelope] = []
    seen: set[tuple[str, int]] = set()
    for envelope in parsed:
        key = (envelope.capture_id, envelope.ordinal)
        if key in seen:
            rejected += 1
            if len(errors) < 10:
                errors.append(
                    f"record {envelope.source_index}: duplicate archive "
                    f"ordinal {envelope.capture_id}/{envelope.ordinal}"
                )
            continue
        seen.add(key)
        unique.append(envelope)
    original_order = [
        (envelope.capture_id, envelope.ordinal)
        for envelope in unique
    ]
    unique.sort(key=lambda item: (item.capture_id, item.ordinal))
    sorted_order = [
        (envelope.capture_id, envelope.ordinal)
        for envelope in unique
    ]
    reordered = sum(
        left != right
        for left, right in zip(original_order, sorted_order, strict=True)
    )

    pending_gap: dict[str, str | None] = defaultdict(lambda: None)
    pending_regression: dict[str, bool] = defaultdict(bool)
    previous_ordinal: dict[str, int] = {}
    books: list[PublicOrderBook] = []
    skipped = 0
    propagated_gap = 0
    ordinal_discontinuities = 0
    gap_markers = sum(envelope.gap_before for envelope in unique)
    regression_markers = sum(
        envelope.monotonic_regression for envelope in unique
    )
    for envelope in unique:
        prior_ordinal = previous_ordinal.get(envelope.capture_id)
        if (
            prior_ordinal is not None
            and envelope.ordinal != prior_ordinal + 1
        ):
            ordinal_discontinuities += 1
            pending_gap[envelope.capture_id] = "ordinal_discontinuity"
        previous_ordinal[envelope.capture_id] = envelope.ordinal
        if envelope.gap_before:
            pending_gap[envelope.capture_id] = envelope.gap_reason
        if envelope.monotonic_regression:
            pending_regression[envelope.capture_id] = True
        event_type = envelope.event.get("event_type")
        if event_type == "trade":
            skipped += 1
            continue
        if event_type != "orderbook":
            rejected += 1
            if len(errors) < 10:
                errors.append(
                    f"record {envelope.source_index}: unsupported event_type"
                )
            continue
        gap_reason = pending_gap[envelope.capture_id]
        try:
            book = PublicOrderBook.from_normalized_event(
                envelope.event,
                capture_id=envelope.capture_id,
                connection_id=envelope.connection_id,
                ordinal=envelope.ordinal,
                received_monotonic_ns=envelope.received_monotonic_ns,
                received_wall_ns=envelope.received_wall_ns,
                gap_before=gap_reason is not None,
                gap_reason=gap_reason,
                monotonic_regression=(
                    pending_regression[envelope.capture_id]
                ),
            )
        except (DepthBookValidationError, TypeError) as exc:
            rejected += 1
            if len(errors) < 10:
                errors.append(f"record {envelope.source_index}: {exc}")
            continue
        if gap_reason is not None:
            propagated_gap += 1
        books.append(book)
        pending_gap[envelope.capture_id] = None
        pending_regression[envelope.capture_id] = False

    grouped: dict[
        tuple[str, str],
        list[PublicOrderBook],
    ] = defaultdict(list)
    for book in books:
        grouped[(book.capture_id, book.connection_id)].append(book)
    segments = tuple(
        ConnectionSegmentMetadata(
            capture_id=key[0],
            connection_id=key[1],
            first_orderbook_ordinal=min(book.ordinal for book in values),
            last_orderbook_ordinal=max(book.ordinal for book in values),
            orderbook_count=len(values),
        )
        for key, values in sorted(grouped.items())
    )
    return OrderBookRecordLoadResult(
        books=tuple(books),
        total_records=len(materialized),
        skipped_non_orderbook_records=skipped,
        rejected_records=rejected,
        rejection_examples=tuple(errors),
        capture_ids=tuple(sorted({book.capture_id for book in books})),
        connection_segments=segments,
        gap_marker_count=gap_markers,
        ordinal_discontinuity_count=ordinal_discontinuities,
        monotonic_regression_marker_count=regression_markers,
        propagated_gap_to_orderbook_count=propagated_gap,
        reordered_record_count=reordered,
    )


@dataclass(frozen=True, slots=True)
class IndependentTakerOrder:
    """Independent marketable order expressed in exactly one target unit."""

    order_id: str
    capture_id: str
    connection_id: str
    market: str
    side: OrderSide
    decision_book_ordinal: int
    decision_monotonic_ns: int
    base_quantity: float | None = None
    quote_notional: float | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "order_id", _text(self.order_id, "order_id"))
        object.__setattr__(
            self,
            "capture_id",
            _text(self.capture_id, "capture_id"),
        )
        object.__setattr__(
            self,
            "connection_id",
            _text(self.connection_id, "connection_id"),
        )
        object.__setattr__(
            self,
            "market",
            _text(self.market, "market").upper(),
        )
        if self.side not in ("buy", "sell"):
            raise DepthBookValidationError("side must be 'buy' or 'sell'")
        object.__setattr__(
            self,
            "decision_book_ordinal",
            _integer(
                self.decision_book_ordinal,
                "decision_book_ordinal",
                minimum=1,
            ),
        )
        object.__setattr__(
            self,
            "decision_monotonic_ns",
            _integer(
                self.decision_monotonic_ns,
                "decision_monotonic_ns",
            ),
        )
        supplied = (
            self.base_quantity is not None,
            self.quote_notional is not None,
        )
        if sum(supplied) != 1:
            raise DepthBookValidationError(
                "exactly one of base_quantity or quote_notional is required"
            )
        if self.base_quantity is not None:
            object.__setattr__(
                self,
                "base_quantity",
                _positive_float(self.base_quantity, "base_quantity"),
            )
        if self.quote_notional is not None:
            object.__setattr__(
                self,
                "quote_notional",
                _positive_float(self.quote_notional, "quote_notional"),
            )

    @property
    def request_kind(self) -> RequestKind:
        if self.base_quantity is not None:
            return "base_quantity"
        return "quote_notional"


@dataclass(frozen=True, slots=True)
class VisibleDepthExecution:
    """One deterministic taker sweep of a selected visible book."""

    order_id: str
    capture_id: str
    connection_id: str
    market: str
    side: OrderSide
    status: ExecutionStatus
    request_kind: RequestKind
    requested_base: float | None
    requested_quote: float | None
    filled_base: float
    unfilled_base: float
    filled_quote: float
    unfilled_quote: float
    fully_filled: bool
    levels_consumed: int
    best_price: float
    mid_price: float
    vwap_price: float
    slippage_quote_vs_best: float
    slippage_bps_vs_best: float
    slippage_quote_vs_mid: float
    slippage_bps_vs_mid: float
    fee_rate: float
    fee_quote: float
    cash_flow_quote: float
    book_ordinal: int
    book_received_monotonic_ns: int
    book_received_wall_ns: int | None
    book_exchange_timestamp_ms: int | None
    assumptions: tuple[str, ...] = VISIBLE_TAKER_ASSUMPTIONS


@dataclass(frozen=True, slots=True)
class DepthReplayConfig:
    """Receive-monotonic execution assumptions for independent orders."""

    latency_ns: int
    fee_rate: float = 0.0005
    max_book_gap_ns: int | None = 1_000_000_000

    def validate(self) -> DepthReplayConfig:
        latency = _integer(self.latency_ns, "latency_ns")
        if isinstance(self.fee_rate, bool):
            raise DepthBookValidationError("fee_rate must be numeric")
        try:
            fee = float(self.fee_rate)
        except (TypeError, ValueError, OverflowError) as exc:
            raise DepthBookValidationError("fee_rate must be numeric") from exc
        if not math.isfinite(fee) or not 0 <= fee < 1:
            raise DepthBookValidationError("fee_rate must be in [0, 1)")
        gap = self.max_book_gap_ns
        if gap is not None:
            gap = _integer(gap, "max_book_gap_ns", minimum=1)
        return DepthReplayConfig(
            latency_ns=latency,
            fee_rate=fee,
            max_book_gap_ns=gap,
        )


@dataclass(frozen=True, slots=True)
class TakerReplayOutcome:
    """Replay status and optional visible-depth execution."""

    order_id: str
    status: ReplayStatus
    reason: str
    decision_book_ordinal: int
    decision_monotonic_ns: int
    due_monotonic_ns: int
    selected_book_ordinal: int | None
    selected_book_monotonic_ns: int | None
    execution: VisibleDepthExecution | None


def sweep_visible_depth(
    book: PublicOrderBook,
    order: IndependentTakerOrder,
    *,
    fee_rate: float = 0.0005,
) -> VisibleDepthExecution:
    """Sweep visible asks or bids without extrapolating beyond their depth.

    ``quote_notional`` is gross quote currency before fees: buy spend or sell
    proceeds.  The unfilled field in the requested unit is exact.  The
    unrequested unit's unfilled field is zero because no target was expressed
    in that unit.
    """

    if (
        order.capture_id != book.capture_id
        or order.connection_id != book.connection_id
        or order.market != book.market
    ):
        raise DepthBookValidationError(
            "order and book capture, connection, and market must match"
        )
    selected = DepthReplayConfig(
        latency_ns=0,
        fee_rate=fee_rate,
        max_book_gap_ns=None,
    ).validate()
    levels = book.asks if order.side == "buy" else book.bids
    remaining = float(
        order.base_quantity
        if order.base_quantity is not None
        else order.quote_notional
    )
    filled_base = 0.0
    filled_quote = 0.0
    levels_consumed = 0
    for level in levels:
        if remaining <= 0:
            break
        take_base = (
            min(level.base_size, remaining)
            if order.request_kind == "base_quantity"
            else min(level.base_size, remaining / level.price)
        )
        if take_base <= 0:
            continue
        take_quote = take_base * level.price
        filled_base += take_base
        filled_quote += take_quote
        levels_consumed += 1
        remaining -= (
            take_base
            if order.request_kind == "base_quantity"
            else take_quote
        )
        if abs(remaining) <= max(1e-12, abs(filled_quote) * 1e-15):
            remaining = 0.0
    if filled_base <= 0 or filled_quote <= 0:
        raise DepthBookValidationError(
            "validated visible book unexpectedly produced no fill"
        )

    unfilled_base = (
        max(0.0, remaining)
        if order.request_kind == "base_quantity"
        else 0.0
    )
    unfilled_quote = (
        max(0.0, remaining)
        if order.request_kind == "quote_notional"
        else 0.0
    )
    fully_filled = remaining == 0.0
    vwap = filled_quote / filled_base
    best = levels[0].price
    mid = book.mid_price
    direction = 1.0 if order.side == "buy" else -1.0
    best_difference = direction * (vwap - best)
    mid_difference = direction * (vwap - mid)
    fee = filled_quote * selected.fee_rate
    cash_flow = (
        -(filled_quote + fee)
        if order.side == "buy"
        else filled_quote - fee
    )
    return VisibleDepthExecution(
        order_id=order.order_id,
        capture_id=book.capture_id,
        connection_id=book.connection_id,
        market=book.market,
        side=order.side,
        status="filled" if fully_filled else "partial",
        request_kind=order.request_kind,
        requested_base=order.base_quantity,
        requested_quote=order.quote_notional,
        filled_base=filled_base,
        unfilled_base=unfilled_base,
        filled_quote=filled_quote,
        unfilled_quote=unfilled_quote,
        fully_filled=fully_filled,
        levels_consumed=levels_consumed,
        best_price=best,
        mid_price=mid,
        vwap_price=vwap,
        slippage_quote_vs_best=filled_base * best_difference,
        slippage_bps_vs_best=best_difference / best * 10_000.0,
        slippage_quote_vs_mid=filled_base * mid_difference,
        slippage_bps_vs_mid=mid_difference / mid * 10_000.0,
        fee_rate=selected.fee_rate,
        fee_quote=fee,
        cash_flow_quote=cash_flow,
        book_ordinal=book.ordinal,
        book_received_monotonic_ns=book.received_monotonic_ns,
        book_received_wall_ns=book.received_wall_ns,
        book_exchange_timestamp_ms=book.exchange_timestamp_ms,
    )


@dataclass(slots=True)
class _BookRun:
    books: list[PublicOrderBook]
    times: list[int]
    end_reason: str | None = None


@dataclass(frozen=True, slots=True)
class _IndexedBook:
    book: PublicOrderBook
    run_id: int
    index_in_run: int


class _ReplayBookIndex:
    """One-pass run construction plus logarithmic order lookup."""

    def __init__(
        self,
        books: Sequence[PublicOrderBook],
        max_gap_ns: int | None,
    ) -> None:
        ordered = sorted(
            books,
            key=lambda book: (
                book.capture_id,
                book.market,
                book.ordinal,
            ),
        )
        archive_keys = [(book.capture_id, book.ordinal) for book in ordered]
        if len(archive_keys) != len(set(archive_keys)):
            raise DepthBookValidationError(
                "books cannot contain duplicate capture ordinals"
            )
        self.runs: list[_BookRun] = []
        grouped: dict[
            tuple[str, str],
            list[PublicOrderBook],
        ] = defaultdict(list)
        for book in ordered:
            grouped[(book.capture_id, book.market)].append(book)

        decision_books: dict[
            tuple[str, str, str, int],
            _IndexedBook,
        ] = {}
        for market_key, stream in grouped.items():
            current_run: _BookRun | None = None
            previous: PublicOrderBook | None = None
            for book in stream:
                boundary: str | None = None
                if previous is not None:
                    if book.connection_id != previous.connection_id:
                        boundary = "connection_boundary_before_due"
                    elif (
                        book.monotonic_regression
                        or book.received_monotonic_ns
                        < previous.received_monotonic_ns
                    ):
                        boundary = "receive_monotonic_regression"
                    elif book.gap_before:
                        boundary = f"archive_gap_before:{book.gap_reason}"
                    elif (
                        max_gap_ns is not None
                        and book.received_monotonic_ns
                        - previous.received_monotonic_ns
                        > max_gap_ns
                    ):
                        boundary = "book_gap_before_due"
                if current_run is None or boundary is not None:
                    if current_run is not None:
                        current_run.end_reason = boundary
                    current_run = _BookRun(books=[], times=[])
                    self.runs.append(current_run)
                run_id = len(self.runs) - 1
                index_in_run = len(current_run.books)
                current_run.books.append(book)
                current_run.times.append(book.received_monotonic_ns)
                indexed = _IndexedBook(
                    book=book,
                    run_id=run_id,
                    index_in_run=index_in_run,
                )
                decision_key = (
                    book.capture_id,
                    book.market,
                    book.connection_id,
                    book.ordinal,
                )
                if decision_key in decision_books:
                    raise DepthBookValidationError(
                        "books cannot contain duplicate decision-book keys"
                    )
                decision_books[decision_key] = indexed
                previous = book

        self.decision_books = decision_books

    def select(
        self,
        order: IndependentTakerOrder,
        due: int,
    ) -> tuple[PublicOrderBook | None, str | None]:
        key = (
            order.capture_id,
            order.market,
            order.connection_id,
            order.decision_book_ordinal,
        )
        anchor = self.decision_books.get(key)
        if anchor is None:
            return None, "decision_book_not_found_or_connection_mismatch"
        if (
            anchor.book.received_monotonic_ns
            != order.decision_monotonic_ns
        ):
            return None, "decision_time_does_not_match_book"
        run = self.runs[anchor.run_id]
        candidate_index = max(
            anchor.index_in_run,
            bisect.bisect_left(run.times, due),
        )
        if candidate_index < len(run.books):
            candidate = run.books[candidate_index]
            if candidate.received_monotonic_ns < due:
                raise RuntimeError("selected book precedes latency due")
            return candidate, None
        if run.end_reason is not None:
            return None, run.end_reason
        return None, "no_book_at_or_after_due"


def _empty_outcome(
    order: IndependentTakerOrder,
    *,
    due: int,
    reason: str,
) -> TakerReplayOutcome:
    status: Literal["invalid", "unavailable"] = (
        "unavailable"
        if reason == "no_book_at_or_after_due"
        else "invalid"
    )
    return TakerReplayOutcome(
        order_id=order.order_id,
        status=status,
        reason=reason,
        decision_book_ordinal=order.decision_book_ordinal,
        decision_monotonic_ns=order.decision_monotonic_ns,
        due_monotonic_ns=due,
        selected_book_ordinal=None,
        selected_book_monotonic_ns=None,
        execution=None,
    )


def replay_independent_taker_orders(
    books: Sequence[PublicOrderBook],
    orders: Sequence[IndependentTakerOrder],
    config: DepthReplayConfig,
) -> tuple[TakerReplayOutcome, ...]:
    """Replay orders at the first continuity-safe book at/after latency due.

    The index is built once in archive-ordinal order. Each order identifies the
    exact already-observed decision book by ordinal, then uses logarithmic
    monotonic-time lookup only inside that book's continuity-safe run. Wall or
    exchange timestamps never select a fill.
    """

    selected = config.validate()
    index = _ReplayBookIndex(books, selected.max_book_gap_ns)
    outcomes: list[TakerReplayOutcome] = []
    for order in orders:
        due = order.decision_monotonic_ns + selected.latency_ns
        book, reason = index.select(order, due)
        if book is None:
            assert reason is not None
            outcomes.append(_empty_outcome(order, due=due, reason=reason))
            continue
        execution = sweep_visible_depth(
            book,
            order,
            fee_rate=selected.fee_rate,
        )
        outcomes.append(
            TakerReplayOutcome(
                order_id=order.order_id,
                status=execution.status,
                reason=(
                    "filled_at_first_eligible_book"
                    if execution.fully_filled
                    else "visible_depth_exhausted"
                ),
                decision_book_ordinal=order.decision_book_ordinal,
                decision_monotonic_ns=order.decision_monotonic_ns,
                due_monotonic_ns=due,
                selected_book_ordinal=book.ordinal,
                selected_book_monotonic_ns=book.received_monotonic_ns,
                execution=execution,
            )
        )
    return tuple(outcomes)
