from __future__ import annotations

import pytest

from coinpilot.hft_depth import (
    DepthBookValidationError,
    DepthReplayConfig,
    IndependentTakerOrder,
    PublicOrderBook,
    public_orderbooks_from_records,
    replay_independent_taker_orders,
    sweep_visible_depth,
)


def _book(
    monotonic_ns: int,
    *,
    ordinal: int = 1,
    connection_id: str = "capture-1-connection-000001",
    asks: tuple[tuple[float, float], ...] = (
        (100.0, 1.0),
        (101.0, 2.0),
    ),
    bids: tuple[tuple[float, float], ...] = (
        (99.0, 1.0),
        (98.0, 2.0),
    ),
    gap_before: bool = False,
    gap_reason: str | None = None,
    monotonic_regression: bool = False,
) -> PublicOrderBook:
    return PublicOrderBook(
        capture_id="capture-1",
        connection_id=connection_id,
        ordinal=ordinal,
        market="KRW-BTC",
        received_monotonic_ns=monotonic_ns,
        received_wall_ns=1_700_000_000_000_000_000 + monotonic_ns,
        exchange_timestamp_ms=1_700_000_000_000 + monotonic_ns // 1_000_000,
        asks=asks,
        bids=bids,
        gap_before=gap_before,
        gap_reason=gap_reason,
        monotonic_regression=monotonic_regression,
    )


def _order(
    *,
    order_id: str = "order-1",
    side: str = "buy",
    decision_ns: int = 100,
    decision_ordinal: int = 1,
    connection_id: str = "capture-1-connection-000001",
    base_quantity: float | None = 1.5,
    quote_notional: float | None = None,
) -> IndependentTakerOrder:
    return IndependentTakerOrder(
        order_id=order_id,
        capture_id="capture-1",
        connection_id=connection_id,
        market="KRW-BTC",
        side=side,  # type: ignore[arg-type]
        decision_book_ordinal=decision_ordinal,
        decision_monotonic_ns=decision_ns,
        base_quantity=base_quantity,
        quote_notional=quote_notional,
    )


def _event(
    monotonic_ns: int,
    *,
    event_type: str = "orderbook",
    ask: float = 100.0,
    bid: float = 99.0,
) -> dict[str, object]:
    wall = 1_700_000_000_000_000_000 + monotonic_ns
    if event_type == "trade":
        return {
            "event_type": "trade",
            "market": "KRW-BTC",
            "received_at_ns": wall,
        }
    return {
        "event_type": "orderbook",
        "market": "KRW-BTC",
        "received_at_ns": wall,
        "exchange_timestamp_ms": wall // 1_000_000,
        "best_ask_price": ask,
        "best_bid_price": bid,
        "levels": [
            {
                "ask_price": ask,
                "ask_size": 1.0,
                "bid_price": bid,
                "bid_size": 2.0,
            }
        ],
    }


def _envelope(
    ordinal: int,
    monotonic_ns: int,
    *,
    event_type: str = "orderbook",
    connection_id: str = "capture-1-connection-000001",
    gap_before: bool = False,
    gap_reason: str | None = None,
    monotonic_regression: bool = False,
    ask: float = 100.0,
    bid: float = 99.0,
) -> dict[str, object]:
    wall = 1_700_000_000_000_000_000 + monotonic_ns
    return {
        "schema_version": 1,
        "capture_id": "capture-1",
        "connection_id": connection_id,
        "ordinal": ordinal,
        "received_wall_ns": str(wall),
        "received_monotonic_ns": str(monotonic_ns),
        "receive_delta_monotonic_ns": None,
        "monotonic_regression": monotonic_regression,
        "gap_before": gap_before,
        "gap_reason": gap_reason,
        "gap_duration_monotonic_ns": None,
        "event": _event(
            monotonic_ns,
            event_type=event_type,
            ask=ask,
            bid=bid,
        ),
    }


def test_exact_multilevel_base_sweep_sorts_and_attributes_slippage() -> None:
    book = _book(
        100,
        asks=((101.0, 2.0), (100.0, 1.0)),
        bids=((98.0, 2.0), (99.0, 1.0)),
    )
    execution = sweep_visible_depth(
        book,
        _order(base_quantity=1.5),
        fee_rate=0.001,
    )

    assert [level.price for level in book.asks] == [100.0, 101.0]
    assert [level.price for level in book.bids] == [99.0, 98.0]
    assert execution.status == "filled"
    assert execution.filled_base == pytest.approx(1.5)
    assert execution.filled_quote == pytest.approx(150.5)
    assert execution.unfilled_base == pytest.approx(0.0)
    assert execution.vwap_price == pytest.approx(150.5 / 1.5)
    assert execution.levels_consumed == 2
    assert execution.best_price == pytest.approx(100.0)
    assert execution.mid_price == pytest.approx(99.5)
    assert execution.slippage_quote_vs_best == pytest.approx(0.5)
    assert execution.slippage_quote_vs_mid == pytest.approx(1.25)
    assert execution.fee_quote == pytest.approx(0.1505)
    assert execution.cash_flow_quote == pytest.approx(-150.6505)


def test_quote_notional_and_partial_visible_depth_are_explicit() -> None:
    book = _book(100)
    quote_execution = sweep_visible_depth(
        book,
        _order(base_quantity=None, quote_notional=250.0),
        fee_rate=0.0,
    )
    assert quote_execution.request_kind == "quote_notional"
    assert quote_execution.filled_quote == pytest.approx(250.0)
    assert quote_execution.filled_base == pytest.approx(1.0 + 150.0 / 101.0)
    assert quote_execution.unfilled_quote == pytest.approx(0.0)
    assert quote_execution.levels_consumed == 2

    partial_quote = sweep_visible_depth(
        book,
        _order(base_quantity=None, quote_notional=400.0),
        fee_rate=0.0,
    )
    assert partial_quote.status == "partial"
    assert partial_quote.filled_base == pytest.approx(3.0)
    assert partial_quote.filled_quote == pytest.approx(302.0)
    assert partial_quote.unfilled_quote == pytest.approx(98.0)

    sell_quote = sweep_visible_depth(
        book,
        _order(
            side="sell",
            base_quantity=None,
            quote_notional=150.0,
        ),
        fee_rate=0.0,
    )
    assert sell_quote.status == "filled"
    assert sell_quote.filled_quote == pytest.approx(150.0)
    assert sell_quote.filled_base == pytest.approx(1.0 + 51.0 / 98.0)

    partial = sweep_visible_depth(
        book,
        _order(side="sell", base_quantity=5.0),
        fee_rate=0.001,
    )
    assert partial.status == "partial"
    assert partial.filled_base == pytest.approx(3.0)
    assert partial.unfilled_base == pytest.approx(2.0)
    assert partial.filled_quote == pytest.approx(295.0)
    assert partial.vwap_price == pytest.approx(295.0 / 3.0)
    assert partial.slippage_quote_vs_best == pytest.approx(2.0)
    assert partial.fee_quote == pytest.approx(0.295)
    assert partial.cash_flow_quote == pytest.approx(294.705)


def test_fee_two_x_changes_only_fee_and_worsens_cash_flow() -> None:
    book = _book(100)
    order = _order(base_quantity=1.5)
    base = sweep_visible_depth(book, order, fee_rate=0.0005)
    stressed = sweep_visible_depth(book, order, fee_rate=0.001)

    assert stressed.filled_base == pytest.approx(base.filled_base)
    assert stressed.filled_quote == pytest.approx(base.filled_quote)
    assert stressed.vwap_price == pytest.approx(base.vwap_price)
    assert stressed.slippage_bps_vs_best == pytest.approx(
        base.slippage_bps_vs_best
    )
    assert stressed.fee_quote == pytest.approx(base.fee_quote * 2.0)
    assert stressed.cash_flow_quote < base.cash_flow_quote


def test_latency_selects_first_monotonic_book_at_due_without_lookahead() -> None:
    books = (
        _book(100, ordinal=1, asks=((100.0, 2.0),), bids=((99.0, 2.0),)),
        _book(200, ordinal=2, asks=((110.0, 2.0),), bids=((109.0, 2.0),)),
        _book(300, ordinal=3, asks=((120.0, 2.0),), bids=((119.0, 2.0),)),
        _book(400, ordinal=4, asks=((130.0, 2.0),), bids=((129.0, 2.0),)),
    )
    order = _order(decision_ns=100, base_quantity=1.0)
    config = DepthReplayConfig(
        latency_ns=150,
        fee_rate=0.0,
        max_book_gap_ns=150,
    )
    result = replay_independent_taker_orders(books, (order,), config)[0]

    assert result.status == "filled"
    assert result.due_monotonic_ns == 250
    assert result.selected_book_monotonic_ns == 300
    assert result.selected_book_ordinal == 3
    assert result.execution is not None
    assert result.execution.vwap_price == pytest.approx(120.0)

    changed_before_due = (
        books[0],
        _book(
            200,
            ordinal=2,
            asks=((1_000.0, 2.0),),
            bids=((999.0, 2.0),),
        ),
        books[2],
        books[3],
    )
    changed = replay_independent_taker_orders(
        changed_before_due,
        (order,),
        config,
    )[0]
    assert changed.selected_book_ordinal == 3
    assert changed.execution is not None
    assert changed.execution.vwap_price == pytest.approx(120.0)


def test_archive_loader_orders_ordinals_and_propagates_trade_gap() -> None:
    records = (
        _envelope(3, 300, ask=120.0, bid=119.0),
        _envelope(
            2,
            200,
            event_type="trade",
            gap_before=True,
            gap_reason="receive_error",
        ),
        _envelope(1, 100, ask=100.0, bid=99.0),
    )
    loaded = public_orderbooks_from_records(records)

    assert [book.ordinal for book in loaded.books] == [1, 3]
    assert loaded.total_records == 3
    assert loaded.skipped_non_orderbook_records == 1
    assert loaded.rejected_records == 0
    assert loaded.reordered_record_count == 2
    assert loaded.gap_marker_count == 1
    assert loaded.propagated_gap_to_orderbook_count == 1
    assert loaded.books[1].gap_before is True
    assert loaded.books[1].gap_reason == "receive_error"
    assert loaded.connection_segments[0].orderbook_count == 2

    outcome = replay_independent_taker_orders(
        loaded.books,
        (_order(decision_ns=100, base_quantity=1.0),),
        DepthReplayConfig(
            latency_ns=150,
            fee_rate=0.0,
            max_book_gap_ns=None,
        ),
    )[0]
    assert outcome.status == "invalid"
    assert outcome.reason == "archive_gap_before:receive_error"


def test_archive_loader_turns_missing_ordinal_into_replay_boundary() -> None:
    loaded = public_orderbooks_from_records(
        (
            _envelope(1, 100),
            _envelope(3, 300),
        )
    )

    assert loaded.ordinal_discontinuity_count == 1
    assert loaded.books[1].gap_before is True
    assert loaded.books[1].gap_reason == "ordinal_discontinuity"
    outcome = replay_independent_taker_orders(
        loaded.books,
        (
            _order(
                decision_ns=100,
                decision_ordinal=1,
                base_quantity=1.0,
            ),
        ),
        DepthReplayConfig(
            latency_ns=150,
            fee_rate=0.0,
            max_book_gap_ns=1_000,
        ),
    )[0]
    assert outcome.status == "invalid"
    assert outcome.reason == (
        "archive_gap_before:ordinal_discontinuity"
    )


def test_invalid_book_gap_connection_and_regression_never_fill() -> None:
    with pytest.raises(DepthBookValidationError, match="uncrossed"):
        _book(
            100,
            asks=((99.0, 1.0),),
            bids=((100.0, 1.0),),
        )

    order = _order(decision_ns=100, base_quantity=1.0)
    gap = replay_independent_taker_orders(
        (
            _book(100, ordinal=1),
            _book(200, ordinal=2),
            _book(400, ordinal=3),
        ),
        (order,),
        DepthReplayConfig(
            latency_ns=250,
            fee_rate=0.0,
            max_book_gap_ns=150,
        ),
    )[0]
    assert gap.status == "invalid"
    assert gap.reason == "book_gap_before_due"
    assert gap.execution is None

    boundary = replay_independent_taker_orders(
        (
            _book(100, ordinal=1),
            _book(
                200,
                ordinal=2,
                connection_id="capture-1-connection-000002",
            ),
            _book(400, ordinal=3),
        ),
        (order,),
        DepthReplayConfig(
            latency_ns=250,
            fee_rate=0.0,
            max_book_gap_ns=None,
        ),
    )[0]
    assert boundary.status == "invalid"
    assert boundary.reason == "connection_boundary_before_due"

    regression = replay_independent_taker_orders(
        (
            _book(100, ordinal=1),
            _book(300, ordinal=2),
            _book(
                250,
                ordinal=3,
                monotonic_regression=True,
            ),
            _book(400, ordinal=4),
        ),
        (order,),
        DepthReplayConfig(
            latency_ns=250,
            fee_rate=0.0,
            max_book_gap_ns=None,
        ),
    )[0]
    assert regression.status == "invalid"
    assert regression.reason == "receive_monotonic_regression"
    assert regression.execution is None


def test_regressed_duplicate_time_cannot_reanchor_an_earlier_decision() -> None:
    books = (
        _book(
            100,
            ordinal=1,
            asks=((100.0, 2.0),),
            bids=((99.0, 2.0),),
        ),
        _book(
            200,
            ordinal=2,
            asks=((110.0, 2.0),),
            bids=((109.0, 2.0),),
        ),
        _book(
            100,
            ordinal=3,
            asks=((120.0, 2.0),),
            bids=((119.0, 2.0),),
            monotonic_regression=True,
        ),
    )
    earlier = replay_independent_taker_orders(
        books,
        (
            _order(
                decision_ns=100,
                decision_ordinal=1,
                base_quantity=1.0,
            ),
        ),
        DepthReplayConfig(
            latency_ns=0,
            fee_rate=0.0,
            max_book_gap_ns=None,
        ),
    )[0]
    after_regression = replay_independent_taker_orders(
        books,
        (
            _order(
                decision_ns=100,
                decision_ordinal=3,
                base_quantity=1.0,
            ),
        ),
        DepthReplayConfig(
            latency_ns=0,
            fee_rate=0.0,
            max_book_gap_ns=None,
        ),
    )[0]

    assert earlier.selected_book_ordinal == 1
    assert earlier.execution is not None
    assert earlier.execution.vwap_price == pytest.approx(100.0)
    assert after_regression.selected_book_ordinal == 3
    assert after_regression.execution is not None
    assert after_regression.execution.vwap_price == pytest.approx(120.0)
