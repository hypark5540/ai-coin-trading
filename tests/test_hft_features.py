from __future__ import annotations

import copy

import pytest

from coinpilot.hft_data import normalize_upbit_message
from coinpilot.hft_features import (
    CausalHFTFeatureBuilder,
    HFTFeatureConfig,
    HFTFeatureResourceLimitError,
    HFTFeatureValidationError,
    build_causal_hft_features,
    build_hft_feature_rows,
    iter_hft_feature_rows,
)


_EPOCH_NS = 1_700_000_000_000_000_000
_EPOCH_MS = 1_700_000_000_000


def _book(
    offset_ms: int,
    *,
    mid: float = 100.0,
    bid_size: float = 3.0,
    ask_size: float = 1.0,
) -> dict[str, object]:
    spread = 1.0
    units = []
    for level in range(5):
        units.append(
            {
                "ask_price": mid + spread / 2 + level,
                "ask_size": ask_size + level,
                "bid_price": mid - spread / 2 - level,
                "bid_size": bid_size + level,
            }
        )
    return normalize_upbit_message(
        {
            "type": "orderbook",
            "code": "KRW-BTC",
            "timestamp": _EPOCH_MS + offset_ms,
            "total_ask_size": sum(float(unit["ask_size"]) for unit in units),
            "total_bid_size": sum(float(unit["bid_size"]) for unit in units),
            "orderbook_units": units,
        },
        received_at_ns=_EPOCH_NS + offset_ms * 1_000_000,
    )


def _trade(
    offset_ms: int,
    sequence: int,
    *,
    side: str = "BID",
    price: float = 100.0,
    volume: float = 1.0,
) -> dict[str, object]:
    return normalize_upbit_message(
        {
            "type": "trade",
            "code": "KRW-BTC",
            "trade_timestamp": _EPOCH_MS + offset_ms,
            "sequential_id": sequence,
            "trade_price": price,
            "trade_volume": volume,
            "ask_bid": side,
            "best_ask_price": 100.5,
            "best_ask_size": 1.0,
            "best_bid_price": 99.5,
            "best_bid_size": 3.0,
        },
        received_at_ns=_EPOCH_NS + offset_ms * 1_000_000,
    )


def _envelope(
    event: dict[str, object],
    monotonic_ms: int,
    *,
    connection_id: str = "connection-a",
    capture_id: str = "capture-1",
    ordinal: int | None = None,
) -> dict[str, object]:
    envelope: dict[str, object] = {
        "record_type": "event",
        "capture_id": capture_id,
        "connection_id": connection_id,
        "received_monotonic_ns": str(monotonic_ms * 1_000_000),
        "event": event,
    }
    if ordinal is not None:
        envelope["ordinal"] = ordinal
    return envelope


def _causal_part(row: dict[str, object]) -> dict[str, object]:
    result = copy.deepcopy(row)
    result.pop("labels")
    return result


def test_future_mutation_cannot_change_prior_causal_features() -> None:
    events = [
        _envelope(_trade(0, 1, side="BID", volume=1.0), 0),
        _envelope(_book(50, mid=100.0), 50),
        _envelope(_trade(200, 2, side="BID", volume=2.0), 200),
        _envelope(_book(1_000, mid=101.0), 1_000),
    ]
    baseline = build_hft_feature_rows(events)

    changed = copy.deepcopy(events)
    changed[2]["event"]["aggressor_side"] = "sell"
    changed[2]["event"]["trade_volume"] = 20.0
    changed[3]["event"]["levels"][0]["ask_size"] = 50.0
    changed[3]["event"]["best_ask_size"] = 50.0
    mutated = build_hft_feature_rows(changed)

    assert _causal_part(mutated[0]) == _causal_part(baseline[0])
    assert mutated[1]["trailing_signed_trade_volume"] != baseline[1][
        "trailing_signed_trade_volume"
    ]
    assert baseline[0]["trailing_trade_count"] == 1
    assert baseline[0]["trailing_signed_trade_notional"] == pytest.approx(100.0)


def test_labels_use_first_same_segment_book_at_or_after_each_horizon() -> None:
    events = [
        _envelope(_book(0, mid=100.0), 0),
        _envelope(_book(90, mid=101.0), 90),
        _envelope(_book(100, mid=102.0), 100),
        _envelope(_book(1_100, mid=103.0), 1_100),
        _envelope(_book(5_000, mid=105.0), 5_000),
    ]

    row = build_causal_hft_features(events)[0]

    assert row["labels"]["100ms"]["future_mid_return"] == pytest.approx(0.02)
    assert row["labels"]["100ms"]["label_end_arrival_index"] == 2
    assert row["labels"]["1000ms"]["future_mid_return"] == pytest.approx(0.03)
    assert row["labels"]["1000ms"]["label_end_arrival_index"] == 3
    assert row["labels"]["5000ms"]["future_mid_return"] == pytest.approx(0.05)
    assert row["labels"]["5000ms"]["label_end_arrival_index"] == 4
    assert isinstance(row["decision_received_at_ns"], str)
    assert isinstance(
        row["labels"]["100ms"]["label_end_received_at_ns"],
        str,
    )


@pytest.mark.parametrize("marker", ["gap", "reconnect"])
def test_explicit_gap_or_reconnect_invalidates_cross_segment_labels(
    marker: str,
) -> None:
    events = [
        _envelope(_book(0), 0),
        {
            "record_type": marker,
            "capture_id": "capture-1",
            "connection_id": "connection-a",
            "received_monotonic_ns": "50000000",
        },
        _envelope(_book(200, mid=102.0), 200),
    ]

    rows = build_hft_feature_rows(
        events,
        HFTFeatureConfig(label_horizons_ms=(100,)),
    )

    assert len(rows) == 2
    assert rows[0]["segment_id"] != rows[1]["segment_id"]
    assert rows[0]["labels"]["100ms"]["valid"] is False
    assert marker in rows[0]["labels"]["100ms"]["invalid_reason"]


def test_gap_before_closes_prior_segment_and_keeps_current_event() -> None:
    second = _envelope(_book(200, mid=102.0), 200)
    second["gap_before"] = True
    second["gap_reason"] = "archive_partition_discontinuity"

    rows = build_hft_feature_rows(
        [_envelope(_book(0), 0), second],
        HFTFeatureConfig(label_horizons_ms=(100,)),
    )

    assert len(rows) == 2
    assert rows[1]["arrival_index"] == 1
    assert rows[0]["segment_id"] != rows[1]["segment_id"]
    assert "gap_before:archive_partition_discontinuity" in rows[0][
        "labels"
    ]["100ms"]["invalid_reason"]


def test_archive_ordinal_is_preserved_and_discontinuity_starts_segment() -> None:
    rows = build_hft_feature_rows(
        [
            _envelope(_book(0), 0, ordinal=10),
            _envelope(_book(200, mid=102.0), 200, ordinal=12),
        ],
        HFTFeatureConfig(label_horizons_ms=(100,)),
    )

    assert [row["source_ordinal"] for row in rows] == [10, 12]
    assert rows[0]["feature_schema_version"] == 2
    assert rows[0]["source_kind"] == "captured_public_market_data"
    assert rows[0]["decision_source"] == "public_orderbook"
    assert rows[0]["public_trades_are_own_fills"] is False
    assert rows[0]["segment_id"] != rows[1]["segment_id"]
    assert rows[0]["labels"]["100ms"]["valid"] is False
    assert "archive_ordinal_discontinuity" in rows[0]["labels"]["100ms"][
        "invalid_reason"
    ]


def test_sequential_ordinals_and_legacy_records_remain_compatible() -> None:
    config = HFTFeatureConfig(label_horizons_ms=(100,))
    archived = build_hft_feature_rows(
        [
            _envelope(_book(0), 0, ordinal=10),
            _envelope(_book(100, mid=101.0), 100, ordinal=11),
        ],
        config,
    )
    legacy = build_hft_feature_rows(
        [
            _envelope(_book(0), 0),
            _envelope(_book(100, mid=101.0), 100),
        ],
        config,
    )

    assert archived[0]["segment_id"] == archived[1]["segment_id"]
    assert archived[0]["labels"]["100ms"]["valid"] is True
    assert [row["source_ordinal"] for row in archived] == [10, 11]
    assert legacy[0]["segment_id"] == legacy[1]["segment_id"]
    assert [row["source_ordinal"] for row in legacy] == [None, None]
    assert legacy[0]["labels"]["100ms"]["valid"] is True


def test_connection_change_and_monotonic_regression_start_new_segments() -> None:
    rows = build_hft_feature_rows(
        [
            _envelope(_book(0), 100, connection_id="connection-a"),
            _envelope(_book(10), 110, connection_id="connection-b"),
            _envelope(_book(20), 90, connection_id="connection-b"),
        ],
        HFTFeatureConfig(label_horizons_ms=(10,)),
    )

    assert [row["arrival_index"] for row in rows] == [0, 1, 2]
    assert len({row["segment_id"] for row in rows}) == 3
    assert "connection_changed" in rows[0]["labels"]["10ms"][
        "invalid_reason"
    ]
    assert "receive_time_regression" in rows[1]["labels"]["10ms"][
        "invalid_reason"
    ]


def test_duplicate_public_trades_are_counted_once_per_segment() -> None:
    duplicate = _trade(10, 99, side="BID", volume=2.0)
    events = [
        _envelope(duplicate, 10),
        _envelope(copy.deepcopy(duplicate), 20),
        _envelope(_trade(30, 100, side="ASK", volume=0.5), 30),
        _envelope(_book(40), 40),
        {"record_type": "gap"},
        _envelope(copy.deepcopy(duplicate), 50),
        _envelope(_book(60), 60),
    ]

    rows = build_hft_feature_rows(events)

    assert rows[0]["trailing_trade_count"] == 2
    assert rows[0]["trailing_signed_trade_count"] == 0
    assert rows[0]["trailing_signed_trade_volume"] == pytest.approx(1.5)
    assert rows[0]["trailing_trade_volume"] == pytest.approx(2.5)
    assert rows[1]["trailing_trade_count"] == 1
    assert rows[1]["trailing_signed_trade_volume"] == pytest.approx(2.0)


def test_trailing_window_excludes_old_trades_but_keeps_boundary_trade() -> None:
    rows = build_hft_feature_rows(
        [
            _envelope(_trade(0, 1, volume=1.0), 0),
            _envelope(_trade(1, 2, volume=2.0), 1),
            _envelope(_book(1_000), 1_000),
        ],
        HFTFeatureConfig(trailing_window_ms=1_000),
    )

    assert rows[0]["trailing_trade_count"] == 2
    assert rows[0]["trailing_trade_volume"] == pytest.approx(3.0)


def test_label_overshoot_limit_rejects_stale_future_book() -> None:
    config = HFTFeatureConfig(label_horizons_ms=(100,))
    assert config.max_label_overshoot_ms == 250

    at_limit = build_hft_feature_rows(
        [_envelope(_book(0), 0), _envelope(_book(350), 350)],
        config,
    )
    beyond_limit = build_hft_feature_rows(
        [_envelope(_book(0), 0), _envelope(_book(351), 351)],
        config,
    )

    assert at_limit[0]["labels"]["100ms"]["valid"] is True
    assert beyond_limit[0]["segment_id"] == beyond_limit[1]["segment_id"]
    assert beyond_limit[0]["labels"]["100ms"]["valid"] is False
    assert (
        beyond_limit[0]["labels"]["100ms"]["invalid_reason"]
        == "label_overshoot_exceeded"
    )


def test_in_memory_trade_id_limit_fails_closed_even_with_segment_policy() -> None:
    builder = CausalHFTFeatureBuilder(
        HFTFeatureConfig(
            invalid_event_policy="segment",
            max_in_memory_trade_ids=2,
        )
    )

    assert builder.feed(_envelope(_trade(0, 1), 0)) == ()
    assert builder.feed(_envelope(_trade(1, 2), 1)) == ()
    assert builder.feed(_envelope(_trade(2, 2), 2)) == ()
    assert builder.retained_trade_id_count == 2

    with pytest.raises(
        HFTFeatureResourceLimitError,
        match=r"record 3: max_in_memory_trade_ids=2 exhausted",
    ):
        builder.feed(_envelope(_trade(3, 3), 3))


@pytest.mark.parametrize(
    ("keyword", "value", "message"),
    [
        ("max_label_overshoot_ms", -1, "non-negative"),
        ("max_label_overshoot_ms", True, "non-negative"),
        ("max_in_memory_trade_ids", 0, "positive"),
        ("max_in_memory_trade_ids", False, "positive"),
    ],
)
def test_bounded_memory_config_validation(
    keyword: str,
    value: object,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        HFTFeatureConfig(**{keyword: value})


def test_zero_size_or_malformed_book_is_rejected() -> None:
    zero = _book(0, bid_size=0.0, ask_size=0.0)
    malformed = {**_book(10)}
    malformed["levels"] = []
    malformed["depth"] = 0

    with pytest.raises(HFTFeatureValidationError, match="cannot both be zero"):
        build_hft_feature_rows([zero])
    with pytest.raises(HFTFeatureValidationError, match="levels"):
        build_hft_feature_rows([malformed])


def test_segment_policy_skips_invalid_book_and_never_labels_across_it() -> None:
    zero = _book(50, bid_size=0.0, ask_size=0.0)
    rows = build_hft_feature_rows(
        [_book(0), zero, _book(200, mid=103.0)],
        HFTFeatureConfig(
            label_horizons_ms=(100,),
            invalid_event_policy="segment",
        ),
    )

    assert len(rows) == 2
    assert rows[0]["segment_id"] != rows[1]["segment_id"]
    assert "invalid_event" in rows[0]["labels"]["100ms"]["invalid_reason"]


def test_incremental_builder_streams_across_partitions_with_bounded_lookahead() -> None:
    config = HFTFeatureConfig(
        trailing_window_ms=100,
        label_horizons_ms=(100, 500),
    )
    builder = CausalHFTFeatureBuilder(config)
    first_partition = [
        _envelope(_book(0, mid=100.0), 0),
        _envelope(_book(100, mid=101.0), 100),
    ]
    second_partition = [
        _envelope(_book(500, mid=102.0), 500),
        _envelope(_book(600, mid=103.0), 600),
    ]

    emitted = []
    for record in first_partition:
        emitted.extend(builder.feed(record))
    assert emitted == []
    assert builder.pending_row_count == 2

    for record in second_partition:
        emitted.extend(builder.feed(record))
    emitted.extend(builder.finalize())

    assert [row["arrival_index"] for row in emitted] == [0, 1, 2, 3]
    assert emitted[0]["labels"]["500ms"]["future_mid_return"] == pytest.approx(
        0.02
    )
    assert builder.pending_row_count == 0
    assert builder.finalize() == ()
    with pytest.raises(RuntimeError, match="finalized"):
        builder.feed(_envelope(_book(700), 700))

    assert list(
        iter_hft_feature_rows(
            first_partition + second_partition,
            config,
        )
    ) == emitted
