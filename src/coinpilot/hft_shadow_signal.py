"""Immediate, causal diagnostic signal snapshots for HFT shadow plumbing.

This module intentionally contains no future labels and makes no profitability
claim.  It exists so the always-on shadow path can exercise decisions, risk,
execution, and accounting while a real model is researched separately.
"""

from __future__ import annotations

import math
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from coinpilot.hft_continuity import is_confirmed_continuity_gap
from coinpilot.hft_depth import PublicOrderBook


class ShadowSignalError(ValueError):
    """Raised when an archive envelope cannot be consumed causally."""


@dataclass(frozen=True, slots=True)
class DiagnosticSnapshot:
    book: PublicOrderBook
    ready: bool
    warmup_books_seen: int
    book_imbalance_l5: float
    trade_flow: float
    signal: float
    spread_bps: float
    model_version: str


class OnlineDiagnosticSignal:
    """Compute a bounded heuristic from records received up to the current book."""

    def __init__(
        self,
        *,
        warmup_books: int,
        trade_flow_window_ms: int,
        model_version: str,
        book_weight: float = 0.70,
        flow_weight: float = 0.30,
    ) -> None:
        if (
            isinstance(warmup_books, bool)
            or not isinstance(warmup_books, int)
            or warmup_books < 1
        ):
            raise ShadowSignalError("warmup_books must be a positive integer")
        if (
            isinstance(trade_flow_window_ms, bool)
            or not isinstance(trade_flow_window_ms, int)
            or trade_flow_window_ms < 1
        ):
            raise ShadowSignalError(
                "trade_flow_window_ms must be a positive integer"
            )
        if not isinstance(model_version, str) or not model_version.strip():
            raise ShadowSignalError("model_version must be non-empty")
        for name, value in (
            ("book_weight", book_weight),
            ("flow_weight", flow_weight),
        ):
            if not math.isfinite(float(value)) or value < 0:
                raise ShadowSignalError(f"{name} must be finite and non-negative")
        if book_weight + flow_weight <= 0:
            raise ShadowSignalError("at least one signal weight must be positive")

        self.warmup_books = warmup_books
        self.window_ns = trade_flow_window_ms * 1_000_000
        self.model_version = model_version.strip()
        weight_total = book_weight + flow_weight
        self.book_weight = book_weight / weight_total
        self.flow_weight = flow_weight / weight_total
        self._connection_id: str | None = None
        self._capture_id: str | None = None
        self._last_ordinal: int | None = None
        self._books_seen = 0
        self._signed_trades: deque[tuple[int, str, float]] = deque()
        self._trade_ids: set[str] = set()

    def reset(self) -> None:
        self._books_seen = 0
        self._signed_trades.clear()
        self._trade_ids.clear()

    @staticmethod
    def _required_int(record: Mapping[str, Any], key: str) -> int:
        value = record.get(key)
        if isinstance(value, bool):
            raise ShadowSignalError(f"{key} must be an integer")
        try:
            result = int(value)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ShadowSignalError(f"{key} must be an integer") from exc
        if result < 0:
            raise ShadowSignalError(f"{key} cannot be negative")
        return result

    @staticmethod
    def _required_text(record: Mapping[str, Any], key: str) -> str:
        value = record.get(key)
        if not isinstance(value, str) or not value.strip():
            raise ShadowSignalError(f"{key} must be a non-empty string")
        return value.strip()

    def _continuity(self, record: Mapping[str, Any]) -> tuple[int, str, str]:
        ordinal = self._required_int(record, "ordinal")
        if ordinal < 1:
            raise ShadowSignalError("ordinal must be positive")
        capture_id = self._required_text(record, "capture_id")
        connection_id = self._required_text(record, "connection_id")
        boundary = (
            self._capture_id is not None
            and (
                capture_id != self._capture_id
                or connection_id != self._connection_id
                or self._last_ordinal is None
                or ordinal != self._last_ordinal + 1
            )
        )
        if (
            boundary
            or is_confirmed_continuity_gap(
                gap_before=record.get("gap_before") is True,
                gap_reason=record.get("gap_reason"),
            )
            or record.get("monotonic_regression") is True
        ):
            self.reset()
        self._capture_id = capture_id
        self._connection_id = connection_id
        self._last_ordinal = ordinal
        return ordinal, capture_id, connection_id

    def _prune(self, now_ns: int) -> None:
        cutoff = now_ns - self.window_ns
        while self._signed_trades and self._signed_trades[0][0] < cutoff:
            _, trade_id, _ = self._signed_trades.popleft()
            self._trade_ids.discard(trade_id)

    def feed(
        self,
        record: Mapping[str, Any],
    ) -> DiagnosticSnapshot | None:
        """Consume one archive envelope and emit immediately on order books."""

        if not isinstance(record, Mapping):
            raise ShadowSignalError("archive record must be an object")
        if self._required_int(record, "schema_version") != 1:
            raise ShadowSignalError("unsupported archive schema_version")
        _, _, _ = self._continuity(record)
        received_ns = self._required_int(record, "received_monotonic_ns")
        event = record.get("event")
        if not isinstance(event, Mapping):
            raise ShadowSignalError("archive event must be an object")
        event_type = event.get("event_type")

        self._prune(received_ns)
        if event_type == "trade":
            trade_id = str(event.get("sequence_id", "")).strip()
            if not trade_id:
                raise ShadowSignalError("trade sequence_id must be non-empty")
            if trade_id in self._trade_ids:
                return None
            try:
                price = float(event["trade_price"])
                volume = float(event["trade_volume"])
            except (KeyError, TypeError, ValueError, OverflowError) as exc:
                raise ShadowSignalError("trade price/volume must be numeric") from exc
            side = event.get("aggressor_side")
            if (
                not math.isfinite(price)
                or not math.isfinite(volume)
                or price <= 0
                or volume <= 0
                or side not in {"buy", "sell"}
            ):
                raise ShadowSignalError("trade fields are invalid")
            direction = 1.0 if side == "buy" else -1.0
            self._signed_trades.append(
                (received_ns, trade_id, direction * price * volume)
            )
            self._trade_ids.add(trade_id)
            return None
        if event_type != "orderbook":
            raise ShadowSignalError("event_type must be orderbook or trade")

        book = PublicOrderBook.from_archive_envelope(record)
        self._books_seen += 1
        ask_size = sum(level.base_size for level in book.asks[:5])
        bid_size = sum(level.base_size for level in book.bids[:5])
        depth = ask_size + bid_size
        imbalance = (bid_size - ask_size) / depth if depth > 0 else 0.0
        bought = sum(
            value for _, _, value in self._signed_trades if value > 0
        )
        sold = -sum(
            value for _, _, value in self._signed_trades if value < 0
        )
        total_flow = bought + sold
        trade_flow = (bought - sold) / total_flow if total_flow > 0 else 0.0
        signal = self.book_weight * imbalance + self.flow_weight * trade_flow
        spread_bps = (
            (book.best_ask.price - book.best_bid.price)
            / book.mid_price
            * 10_000.0
        )
        return DiagnosticSnapshot(
            book=book,
            ready=self._books_seen >= self.warmup_books,
            warmup_books_seen=self._books_seen,
            book_imbalance_l5=imbalance,
            trade_flow=trade_flow,
            signal=signal,
            spread_bps=spread_bps,
            model_version=self.model_version,
        )
