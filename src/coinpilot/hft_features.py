from __future__ import annotations

import math
from collections import deque
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any

from coinpilot.hft_data import HFTDataValidationError, validate_hft_event


HFT_FEATURE_SCHEMA_VERSION = 2
_NS_PER_MS = 1_000_000
_EVENT_KEYS = ("event", "normalized_event", "payload")
_MONOTONIC_KEYS = (
    "receive_monotonic_ns",
    "received_monotonic_ns",
    "monotonic_ns",
)
_MARKER_NAMES = {
    "gap",
    "stream_gap",
    "capture_gap",
    "archive_gap",
    "reconnect",
    "reconnected",
    "disconnect",
    "disconnected",
    "connection_open",
    "connection_start",
    "connection_close",
    "connection_end",
}


class HFTFeatureValidationError(ValueError):
    """Raised when causal HFT features cannot be built safely."""


class HFTFeatureResourceLimitError(HFTFeatureValidationError):
    """Raised when a configured bounded-memory safety limit is exhausted."""


@dataclass(frozen=True, slots=True)
class HFTFeatureConfig:
    """Causal feature and future-label contract for an event archive."""

    trailing_window_ms: int = 1_000
    label_horizons_ms: tuple[int, ...] = (100, 1_000, 5_000)
    invalid_event_policy: str = "raise"
    time_regression_policy: str = "segment"
    max_label_overshoot_ms: int = 250
    max_in_memory_trade_ids: int = 250_000

    def __post_init__(self) -> None:
        if (
            isinstance(self.trailing_window_ms, bool)
            or not isinstance(self.trailing_window_ms, int)
            or self.trailing_window_ms < 1
        ):
            raise ValueError("trailing_window_ms must be a positive integer")
        if (
            not isinstance(self.label_horizons_ms, tuple)
            or not self.label_horizons_ms
        ):
            raise ValueError("label_horizons_ms must be a non-empty tuple")
        if any(
            isinstance(value, bool)
            or not isinstance(value, int)
            or value < 1
            for value in self.label_horizons_ms
        ):
            raise ValueError("label horizons must be positive integer milliseconds")
        if len(set(self.label_horizons_ms)) != len(self.label_horizons_ms):
            raise ValueError("label horizons must be unique")
        if (
            isinstance(self.max_label_overshoot_ms, bool)
            or not isinstance(self.max_label_overshoot_ms, int)
            or self.max_label_overshoot_ms < 0
        ):
            raise ValueError(
                "max_label_overshoot_ms must be a non-negative integer"
            )
        if (
            isinstance(self.max_in_memory_trade_ids, bool)
            or not isinstance(self.max_in_memory_trade_ids, int)
            or self.max_in_memory_trade_ids < 1
        ):
            raise ValueError(
                "max_in_memory_trade_ids must be a positive integer"
            )
        if self.invalid_event_policy not in {"raise", "segment"}:
            raise ValueError("invalid_event_policy must be raise or segment")
        if self.time_regression_policy not in {"raise", "segment"}:
            raise ValueError("time_regression_policy must be raise or segment")


@dataclass(frozen=True, slots=True)
class _ArchiveRecord:
    event: Mapping[str, Any] | None
    marker: str | None
    boundary_before: str | None
    capture_id: str | None
    connection_id: str | None
    monotonic_ns: int | None
    source_ordinal: int | None


@dataclass(frozen=True, slots=True)
class _TradeObservation:
    time_ns: int
    sign: int
    volume: float
    notional: float


@dataclass(frozen=True, slots=True)
class _BookObservation:
    arrival_index: int
    time_ns: int
    received_at_ns: str
    receive_monotonic_ns: str | None
    exchange_timestamp_ms: str
    mid_price: float


@dataclass(slots=True)
class _PendingDecision:
    row: dict[str, Any]
    time_ns: int
    mid_price: float
    unresolved_labels: int


@dataclass(slots=True)
class _Segment:
    segment_id: str
    capture_id: str | None
    connection_id: str | None
    market: str
    time_source: str
    previous_time_ns: int
    last_event_time_ns: int
    last_source_ordinal: int | None
    trades: deque[_TradeObservation] = field(default_factory=deque)
    trade_count: int = 0
    signed_trade_count: int = 0
    trade_volume: float = 0.0
    signed_trade_volume: float = 0.0
    trade_notional: float = 0.0
    signed_trade_notional: float = 0.0
    seen_trade_ids: set[tuple[str, str]] = field(default_factory=set)
    pending_rows: deque[_PendingDecision] = field(default_factory=deque)
    label_queues: dict[int, deque[_PendingDecision]] = field(default_factory=dict)


def _integer(value: Any, field_name: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool):
        raise HFTFeatureValidationError(f"{field_name} must be an integer")
    if isinstance(value, float) and not value.is_integer():
        raise HFTFeatureValidationError(f"{field_name} must be an integer")
    try:
        result = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise HFTFeatureValidationError(
            f"{field_name} must be an integer"
        ) from exc
    if result < minimum:
        raise HFTFeatureValidationError(
            f"{field_name} must be >= {minimum}"
        )
    return result


def _identifier(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    result = str(value).strip()
    if not result:
        raise HFTFeatureValidationError(
            f"{field_name} must be a non-empty string when present"
        )
    return result


def _finite_float(value: Any, field_name: str) -> float:
    if isinstance(value, bool):
        raise HFTFeatureValidationError(f"{field_name} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise HFTFeatureValidationError(
            f"{field_name} must be numeric"
        ) from exc
    if not math.isfinite(result):
        raise HFTFeatureValidationError(f"{field_name} must be finite")
    return result


def _marker_name(record: Mapping[str, Any]) -> str | None:
    if record.get("gap") is True:
        return "gap"
    if record.get("reconnect") is True:
        return "reconnect"
    for key in ("record_type", "marker_type", "event_type", "type"):
        raw = record.get(key)
        if isinstance(raw, str):
            normalized = raw.strip().lower().replace("-", "_")
            if normalized in _MARKER_NAMES:
                return normalized
    return None


def _first_metadata(
    outer: Mapping[str, Any],
    inner: Mapping[str, Any],
    keys: tuple[str, ...],
) -> Any:
    for source in (outer, inner):
        for key in keys:
            if source.get(key) is not None:
                return source.get(key)
    return None


def _unwrap_archive_record(record: Mapping[str, Any]) -> _ArchiveRecord:
    if not isinstance(record, Mapping):
        raise HFTFeatureValidationError("archive record must be an object")
    marker = _marker_name(record)
    if marker is not None:
        monotonic_raw = _first_metadata(record, record, _MONOTONIC_KEYS)
        return _ArchiveRecord(
            event=None,
            marker=marker,
            boundary_before=None,
            capture_id=_identifier(record.get("capture_id"), "capture_id"),
            connection_id=_identifier(
                record.get("connection_id"), "connection_id"
            ),
            monotonic_ns=(
                _integer(monotonic_raw, "receive_monotonic_ns")
                if monotonic_raw is not None
                else None
            ),
            source_ordinal=(
                _integer(record["ordinal"], "ordinal", minimum=1)
                if record.get("ordinal") is not None
                else None
            ),
        )

    inner: Mapping[str, Any] = record
    for key in _EVENT_KEYS:
        candidate = record.get(key)
        if isinstance(candidate, Mapping) and (
            "event_type" in candidate or "schema_version" in candidate
        ):
            inner = candidate
            break

    inner_marker = _marker_name(inner)
    if inner_marker is not None:
        ordinal_raw = _first_metadata(record, inner, ("ordinal",))
        return _ArchiveRecord(
            event=None,
            marker=inner_marker,
            boundary_before=None,
            capture_id=_identifier(
                _first_metadata(record, inner, ("capture_id",)),
                "capture_id",
            ),
            connection_id=_identifier(
                _first_metadata(record, inner, ("connection_id", "session_id")),
                "connection_id",
            ),
            monotonic_ns=None,
            source_ordinal=(
                _integer(ordinal_raw, "ordinal", minimum=1)
                if ordinal_raw is not None
                else None
            ),
        )

    monotonic_raw = _first_metadata(record, inner, _MONOTONIC_KEYS)
    ordinal_raw = _first_metadata(record, inner, ("ordinal",))
    gap_before = record.get("gap_before") is True or inner.get("gap_before") is True
    gap_reason_raw = _first_metadata(record, inner, ("gap_reason",))
    gap_reason = (
        _identifier(gap_reason_raw, "gap_reason")
        if gap_reason_raw is not None
        else None
    )
    return _ArchiveRecord(
        event=inner,
        marker=None,
        boundary_before=(
            f"gap_before:{gap_reason}" if gap_before and gap_reason else "gap_before"
        )
        if gap_before
        else None,
        capture_id=_identifier(
            _first_metadata(record, inner, ("capture_id",)),
            "capture_id",
        ),
        connection_id=_identifier(
            _first_metadata(record, inner, ("connection_id", "session_id")),
            "connection_id",
        ),
        monotonic_ns=(
            _integer(monotonic_raw, "receive_monotonic_ns")
            if monotonic_raw is not None
            else None
        ),
        source_ordinal=(
            _integer(ordinal_raw, "ordinal", minimum=1)
            if ordinal_raw is not None
            else None
        ),
    )


def _validated_event(event: Mapping[str, Any]) -> tuple[dict[str, Any], str | None]:
    """Validate through hft_data while allowing an archive-native trade_id."""

    candidate = dict(event)
    raw_type = str(candidate.get("event_type", "")).strip().lower()
    trade_identity: str | None = None
    if raw_type == "trade":
        raw_identity = candidate.get("sequence_id")
        if raw_identity is None:
            raw_identity = candidate.get("trade_id")
            if raw_identity is None:
                raise HFTFeatureValidationError(
                    "trade requires sequence_id or trade_id"
                )
            trade_identity = _identifier(raw_identity, "trade_id")
            # hft_data's native Upbit contract validates numeric sequence IDs.
            # The archive envelope may instead provide an opaque trade ID.
            candidate["sequence_id"] = "0"
        else:
            trade_identity = _identifier(raw_identity, "sequence_id")

    try:
        validated = validate_hft_event(candidate)
    except (HFTDataValidationError, TypeError) as exc:
        raise HFTFeatureValidationError(str(exc)) from exc
    if validated["event_type"] == "trade":
        if trade_identity is None:
            trade_identity = str(validated["sequence_id"])
        validated["sequence_id"] = trade_identity
    return validated, trade_identity


def _segment_boundary_reason(
    current: _Segment,
    *,
    capture_id: str | None,
    connection_id: str | None,
    market: str,
    time_source: str,
    source_ordinal: int | None,
) -> str | None:
    if (
        capture_id is not None
        and current.capture_id is not None
        and capture_id != current.capture_id
    ):
        return "capture_changed"
    if (
        connection_id is not None
        and current.connection_id is not None
        and connection_id != current.connection_id
    ):
        return "connection_changed"
    if market != current.market:
        return "market_changed"
    if time_source != current.time_source:
        return "receive_time_source_changed"
    if (
        capture_id is not None
        and current.capture_id == capture_id
        and source_ordinal is not None
        and current.last_source_ordinal is not None
        and source_ordinal != current.last_source_ordinal + 1
    ):
        return "archive_ordinal_discontinuity"
    return None


def _book_features(event: Mapping[str, Any]) -> dict[str, int | float]:
    levels = event["levels"]
    best = levels[0]
    ask_price = _finite_float(best["ask_price"], "best ask price")
    bid_price = _finite_float(best["bid_price"], "best bid price")
    ask_size = _finite_float(best["ask_size"], "best ask size")
    bid_size = _finite_float(best["bid_size"], "best bid size")
    spread = ask_price - bid_price
    if spread <= 0:
        raise HFTFeatureValidationError(
            "orderbook best ask must be strictly above best bid"
        )
    l1_total = bid_size + ask_size
    if l1_total <= 0:
        raise HFTFeatureValidationError(
            "orderbook L1 bid and ask sizes cannot both be zero"
        )

    top_five = levels[:5]
    l5_bid_size = sum(
        _finite_float(level["bid_size"], "L5 bid size") for level in top_five
    )
    l5_ask_size = sum(
        _finite_float(level["ask_size"], "L5 ask size") for level in top_five
    )
    l5_total = l5_bid_size + l5_ask_size
    if l5_total <= 0:
        raise HFTFeatureValidationError(
            "orderbook top-five bid and ask sizes cannot both be zero"
        )

    mid_price = (ask_price + bid_price) / 2.0
    return {
        "best_ask_price": ask_price,
        "best_ask_size": ask_size,
        "best_bid_price": bid_price,
        "best_bid_size": bid_size,
        "mid_price": mid_price,
        "spread": spread,
        "spread_bps": spread / mid_price * 10_000.0,
        "microprice": (
            ask_price * bid_size + bid_price * ask_size
        )
        / l1_total,
        "l1_imbalance": (bid_size - ask_size) / l1_total,
        "l5_imbalance": (l5_bid_size - l5_ask_size) / l5_total,
        "l5_depth_used": len(top_five),
    }


def _trade_features(segment: _Segment) -> dict[str, int | float]:
    return {
        "trailing_trade_count": segment.trade_count,
        "trailing_signed_trade_count": segment.signed_trade_count,
        "trailing_trade_volume": segment.trade_volume,
        "trailing_signed_trade_volume": segment.signed_trade_volume,
        "trailing_trade_flow_imbalance": (
            segment.signed_trade_volume / segment.trade_volume
            if segment.trade_volume > 0
            else 0.0
        ),
        "trailing_trade_notional": segment.trade_notional,
        "trailing_signed_trade_notional": segment.signed_trade_notional,
    }


def _invalid_label(reason: str) -> dict[str, Any]:
    return {
        "valid": False,
        "future_mid_return": None,
        "future_mid_price": None,
        "label_end_arrival_index": None,
        "label_end_received_at_ns": None,
        "label_end_receive_monotonic_ns": None,
        "label_end_exchange_timestamp_ms": None,
        "invalid_reason": reason,
    }


def _valid_label(
    *,
    decision_mid: float,
    future: _BookObservation,
) -> dict[str, Any]:
    return {
        "valid": True,
        "future_mid_return": future.mid_price / decision_mid - 1.0,
        "future_mid_price": future.mid_price,
        "label_end_arrival_index": future.arrival_index,
        "label_end_received_at_ns": future.received_at_ns,
        "label_end_receive_monotonic_ns": future.receive_monotonic_ns,
        "label_end_exchange_timestamp_ms": future.exchange_timestamp_ms,
        "invalid_reason": None,
    }


TradeIdDeduplicator = Callable[[str, str, str], bool]


class CausalHFTFeatureBuilder:
    """Incrementally build causal decision rows with bounded label lookahead.

    ``feed`` may be called continuously across archive-file partitions. Do not
    call ``finalize`` at an ordinary file boundary; call it only at the logical
    end of the archive. Decision rows are retained only until the largest label
    horizon resolves, while trailing trades are retained only for the configured
    feature window.

    A future book resolves a label only when it is within
    ``max_label_overshoot_ms`` of the requested horizon. Exact trade-ID
    deduplication uses an in-memory set bounded by
    ``max_in_memory_trade_ids`` for the current connection segment by default.
    Very long uninterrupted connections can supply a disk-backed
    ``trade_id_deduplicator`` callback. The callback must atomically return
    ``True`` for an already-seen ``(segment_id, market, trade_id)`` and
    ``False`` while recording a new ID.
    """

    def __init__(
        self,
        config: HFTFeatureConfig | None = None,
        *,
        trade_id_deduplicator: TradeIdDeduplicator | None = None,
    ) -> None:
        self.config = config or HFTFeatureConfig()
        self.trade_id_deduplicator = trade_id_deduplicator
        self._current: _Segment | None = None
        self._segment_count = 0
        self._arrival_index = 0
        self._finalized = False
        self._window_ns = self.config.trailing_window_ms * _NS_PER_MS
        self._max_label_overshoot_ns = (
            self.config.max_label_overshoot_ms * _NS_PER_MS
        )

    @property
    def pending_row_count(self) -> int:
        """Number of rows waiting only for one or more future labels."""

        return (
            len(self._current.pending_rows)
            if self._current is not None
            else 0
        )

    @property
    def retained_trade_count(self) -> int:
        """Number of public trades retained in the causal feature window."""

        return len(self._current.trades) if self._current is not None else 0

    @property
    def retained_trade_id_count(self) -> int:
        """Number of exact trade IDs retained by the in-memory deduplicator."""

        return (
            len(self._current.seen_trade_ids)
            if self._current is not None
            else 0
        )

    def _new_segment(
        self,
        *,
        capture_id: str | None,
        connection_id: str | None,
        market: str,
        time_source: str,
        event_time_ns: int,
        source_ordinal: int | None,
    ) -> _Segment:
        self._segment_count += 1
        segment = _Segment(
            segment_id=f"segment-{self._segment_count:06d}",
            capture_id=capture_id,
            connection_id=connection_id,
            market=market,
            time_source=time_source,
            previous_time_ns=event_time_ns,
            last_event_time_ns=event_time_ns,
            last_source_ordinal=source_ordinal,
            label_queues={
                horizon: deque()
                for horizon in self.config.label_horizons_ms
            },
        )
        self._current = segment
        return segment

    def _emit_completed_prefix(self, segment: _Segment) -> list[dict[str, Any]]:
        emitted: list[dict[str, Any]] = []
        while (
            segment.pending_rows
            and segment.pending_rows[0].unresolved_labels == 0
        ):
            emitted.append(segment.pending_rows.popleft().row)
        return emitted

    def _close_segment(self, reason: str) -> list[dict[str, Any]]:
        segment = self._current
        if segment is None:
            return []
        for pending in segment.pending_rows:
            labels = pending.row["labels"]
            for horizon_ms in self.config.label_horizons_ms:
                key = f"{horizon_ms}ms"
                if key in labels:
                    continue
                target_ns = pending.time_ns + horizon_ms * _NS_PER_MS
                if reason == "end_of_stream":
                    invalid_reason = (
                        "end_of_stream_before_horizon"
                        if segment.last_event_time_ns < target_ns
                        else "no_book_at_or_after_horizon"
                    )
                else:
                    invalid_reason = f"segment_boundary:{reason}"
                labels[key] = _invalid_label(invalid_reason)
                pending.unresolved_labels -= 1
        emitted = list(segment.pending_rows)
        self._current = None
        return [pending.row for pending in emitted]

    def _resolve_labels(
        self,
        segment: _Segment,
        future: _BookObservation,
    ) -> list[dict[str, Any]]:
        for horizon_ms in self.config.label_horizons_ms:
            queue = segment.label_queues[horizon_ms]
            while (
                queue
                and queue[0].time_ns + horizon_ms * _NS_PER_MS
                <= future.time_ns
            ):
                pending = queue.popleft()
                key = f"{horizon_ms}ms"
                if key in pending.row["labels"]:
                    continue
                target_ns = pending.time_ns + horizon_ms * _NS_PER_MS
                if future.time_ns - target_ns > self._max_label_overshoot_ns:
                    pending.row["labels"][key] = _invalid_label(
                        "label_overshoot_exceeded"
                    )
                else:
                    pending.row["labels"][key] = _valid_label(
                        decision_mid=pending.mid_price,
                        future=future,
                    )
                pending.unresolved_labels -= 1
        return self._emit_completed_prefix(segment)

    def _trade_is_duplicate(
        self,
        segment: _Segment,
        market: str,
        trade_identity: str,
    ) -> bool:
        if self.trade_id_deduplicator is not None:
            return bool(
                self.trade_id_deduplicator(
                    segment.segment_id,
                    market,
                    trade_identity,
                )
            )
        key = (market, trade_identity)
        if key in segment.seen_trade_ids:
            return True
        if (
            len(segment.seen_trade_ids)
            >= self.config.max_in_memory_trade_ids
        ):
            raise HFTFeatureResourceLimitError(
                "max_in_memory_trade_ids="
                f"{self.config.max_in_memory_trade_ids} exhausted; "
                "provide a disk-backed trade_id_deduplicator or start a "
                "new explicitly bounded job"
            )
        segment.seen_trade_ids.add(key)
        return False

    def _expire_trades(self, segment: _Segment, event_time_ns: int) -> None:
        cutoff = event_time_ns - self._window_ns
        while segment.trades and segment.trades[0].time_ns < cutoff:
            expired = segment.trades.popleft()
            segment.trade_count -= 1
            segment.signed_trade_count -= expired.sign
            segment.trade_volume -= expired.volume
            segment.signed_trade_volume -= expired.sign * expired.volume
            segment.trade_notional -= expired.notional
            segment.signed_trade_notional -= expired.sign * expired.notional
        if segment.trade_count == 0:
            # Avoid tiny floating residuals after repeated addition/subtraction.
            segment.signed_trade_count = 0
            segment.trade_volume = 0.0
            segment.signed_trade_volume = 0.0
            segment.trade_notional = 0.0
            segment.signed_trade_notional = 0.0

    def feed(self, raw_record: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
        """Consume one archive record and return newly label-complete rows."""

        if self._finalized:
            raise RuntimeError("cannot feed a finalized HFT feature builder")
        arrival_index = self._arrival_index
        self._arrival_index += 1
        emitted: list[dict[str, Any]] = []

        try:
            archive = _unwrap_archive_record(raw_record)
        except HFTFeatureValidationError as exc:
            if self.config.invalid_event_policy == "raise":
                raise HFTFeatureValidationError(
                    f"record {arrival_index}: {exc}"
                ) from exc
            emitted.extend(self._close_segment("invalid_archive_record"))
            return tuple(emitted)

        if archive.marker is not None:
            emitted.extend(
                self._close_segment(f"{archive.marker}_marker")
            )
            return tuple(emitted)
        if archive.boundary_before is not None:
            emitted.extend(self._close_segment(archive.boundary_before))
        if archive.event is None:
            emitted.extend(self._close_segment("missing_event"))
            return tuple(emitted)

        try:
            event, trade_identity = _validated_event(archive.event)
            received_at_ns = _integer(
                event["received_at_ns"],
                "received_at_ns",
                minimum=1,
            )
            if archive.monotonic_ns is not None:
                event_time_ns = archive.monotonic_ns
                time_source = "receive_monotonic_ns"
            else:
                event_time_ns = received_at_ns
                time_source = "received_at_ns"

            effective_capture = archive.capture_id
            effective_connection = archive.connection_id
            current = self._current
            if current is not None:
                if effective_capture is None:
                    effective_capture = current.capture_id
                if effective_connection is None:
                    effective_connection = current.connection_id
                boundary = _segment_boundary_reason(
                    current,
                    capture_id=effective_capture,
                    connection_id=effective_connection,
                    market=event["market"],
                    time_source=time_source,
                    source_ordinal=archive.source_ordinal,
                )
                if boundary is not None:
                    emitted.extend(self._close_segment(boundary))
                    current = None
                elif event_time_ns < current.previous_time_ns:
                    if self.config.time_regression_policy == "raise":
                        raise HFTFeatureValidationError(
                            "receive time regressed within a segment"
                        )
                    emitted.extend(
                        self._close_segment("receive_time_regression")
                    )
                    current = None

            if current is None:
                current = self._new_segment(
                    capture_id=effective_capture,
                    connection_id=effective_connection,
                    market=event["market"],
                    time_source=time_source,
                    event_time_ns=event_time_ns,
                    source_ordinal=archive.source_ordinal,
                )
            else:
                current.previous_time_ns = event_time_ns
                current.last_event_time_ns = event_time_ns
                if archive.source_ordinal is not None:
                    current.last_source_ordinal = archive.source_ordinal

            self._expire_trades(current, event_time_ns)
            if event["event_type"] == "trade":
                if trade_identity is None:
                    raise HFTFeatureValidationError(
                        "validated trade is missing its identity"
                    )
                if not self._trade_is_duplicate(
                    current,
                    event["market"],
                    str(trade_identity),
                ):
                    sign = 1 if event["aggressor_side"] == "buy" else -1
                    volume = _finite_float(
                        event["trade_volume"], "trade_volume"
                    )
                    price = _finite_float(event["trade_price"], "trade_price")
                    observation = _TradeObservation(
                        time_ns=event_time_ns,
                        sign=sign,
                        volume=volume,
                        notional=price * volume,
                    )
                    current.trades.append(observation)
                    current.trade_count += 1
                    current.signed_trade_count += sign
                    current.trade_volume += observation.volume
                    current.signed_trade_volume += sign * observation.volume
                    current.trade_notional += observation.notional
                    current.signed_trade_notional += (
                        sign * observation.notional
                    )
                return tuple(emitted)

            book_values = _book_features(event)
            book = _BookObservation(
                arrival_index=arrival_index,
                time_ns=event_time_ns,
                received_at_ns=str(received_at_ns),
                receive_monotonic_ns=(
                    str(archive.monotonic_ns)
                    if archive.monotonic_ns is not None
                    else None
                ),
                exchange_timestamp_ms=str(event["exchange_timestamp_ms"]),
                mid_price=float(book_values["mid_price"]),
            )
            emitted.extend(self._resolve_labels(current, book))
            row: dict[str, Any] = {
                "feature_schema_version": HFT_FEATURE_SCHEMA_VERSION,
                "source_kind": "captured_public_market_data",
                "decision_source": "public_orderbook",
                "public_trades_are_own_fills": False,
                "arrival_index": arrival_index,
                "segment_id": current.segment_id,
                "capture_id": current.capture_id,
                "connection_id": current.connection_id,
                "source_ordinal": archive.source_ordinal,
                "market": event["market"],
                "decision_event_type": "orderbook",
                "decision_received_at_ns": str(received_at_ns),
                "decision_receive_monotonic_ns": (
                    str(archive.monotonic_ns)
                    if archive.monotonic_ns is not None
                    else None
                ),
                "decision_time_ns": str(event_time_ns),
                "decision_time_source": time_source,
                "decision_exchange_timestamp_ms": str(
                    event["exchange_timestamp_ms"]
                ),
                "trailing_window_ms": self.config.trailing_window_ms,
                "max_label_overshoot_ms": (
                    self.config.max_label_overshoot_ms
                ),
                **book_values,
                **_trade_features(current),
                "labels": {},
            }
            pending = _PendingDecision(
                row=row,
                time_ns=event_time_ns,
                mid_price=book.mid_price,
                unresolved_labels=len(self.config.label_horizons_ms),
            )
            current.pending_rows.append(pending)
            for queue in current.label_queues.values():
                queue.append(pending)
            return tuple(emitted)
        except HFTFeatureResourceLimitError as exc:
            raise HFTFeatureResourceLimitError(
                f"record {arrival_index}: {exc}"
            ) from exc
        except HFTFeatureValidationError as exc:
            if (
                self.config.time_regression_policy == "raise"
                and "receive time regressed" in str(exc)
            ):
                raise HFTFeatureValidationError(
                    f"record {arrival_index}: {exc}"
                ) from exc
            if self.config.invalid_event_policy == "raise":
                raise HFTFeatureValidationError(
                    f"record {arrival_index}: {exc}"
                ) from exc
            emitted.extend(self._close_segment("invalid_event"))
            return tuple(emitted)

    def finalize(self) -> tuple[dict[str, Any], ...]:
        """Close the logical archive and emit rows with invalid tail labels."""

        if self._finalized:
            return ()
        self._finalized = True
        return tuple(self._close_segment("end_of_stream"))


def iter_hft_feature_rows(
    records: Iterable[Mapping[str, Any]],
    config: HFTFeatureConfig | None = None,
    *,
    trade_id_deduplicator: TradeIdDeduplicator | None = None,
) -> Iterator[dict[str, Any]]:
    """Yield causal rows without materializing a multi-file archive."""

    builder = CausalHFTFeatureBuilder(
        config,
        trade_id_deduplicator=trade_id_deduplicator,
    )
    for record in records:
        yield from builder.feed(record)
    yield from builder.finalize()


def build_hft_feature_rows(
    records: Iterable[Mapping[str, Any]],
    config: HFTFeatureConfig | None = None,
) -> list[dict[str, Any]]:
    """Materialize :func:`iter_hft_feature_rows` for bounded inputs and tests."""

    return list(iter_hft_feature_rows(records, config))


def build_causal_hft_features(
    records: Iterable[Mapping[str, Any]],
    config: HFTFeatureConfig | None = None,
) -> list[dict[str, Any]]:
    """Readable alias for :func:`build_hft_feature_rows`."""

    return build_hft_feature_rows(records, config)
