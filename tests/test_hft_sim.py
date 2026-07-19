from __future__ import annotations

import dataclasses

import pytest

from coinpilot.hft_sim import (
    HFTSimulationConfig,
    MicrostructureEvent,
    event_signal,
    generate_synthetic_microstructure,
    simulate_hft_replay,
    validate_event_stream,
)


def _event(
    sequence: int,
    *,
    mid: float = 100.0,
    positive: bool,
    spread: float = 0.10,
) -> MicrostructureEvent:
    bid_size, ask_size, flow = (
        (9.0, 1.0, 0.9) if positive else (1.0, 9.0, -0.9)
    )
    return MicrostructureEvent(
        sequence=sequence,
        timestamp_ns=sequence * 100_000_000,
        bid_price=mid - spread / 2.0,
        ask_price=mid + spread / 2.0,
        bid_size=bid_size,
        ask_size=ask_size,
        trade_flow=flow,
        source_kind="real_observation",
        scenario="unit-test-observations",
    )


def test_synthetic_generation_and_replay_are_reproducible() -> None:
    first_events = generate_synthetic_microstructure(
        2_000,
        seed=19,
        scenario="weak_alpha",
    )
    second_events = generate_synthetic_microstructure(
        2_000,
        seed=19,
        scenario="weak_alpha",
    )
    assert first_events == second_events

    config = HFTSimulationConfig(equity_sample_points=61)
    first = simulate_hft_replay(first_events, config)
    second = simulate_hft_replay(second_events, config)

    assert first == second
    assert first.metrics["data_kind"] == "synthetic"
    assert first.metrics["is_synthetic"] is True
    assert first.metrics["scenario"] == "weak_alpha"
    assert first.metrics["execution_model"] == "conservative_taker"
    assert first.metrics["maker_fill_count"] == 0
    assert len(first.equity_curve) <= 61


def test_flat_round_trips_lose_spread_and_fees_not_false_profit() -> None:
    events = tuple(
        _event(index + 1, positive=index % 2 == 0)
        for index in range(40)
    )
    config = HFTSimulationConfig(
        initial_cash=10_000.0,
        order_notional=1_000.0,
        taker_fee_rate=0.001,
        latency_steps=0,
        max_holding_steps=10,
    )
    result = simulate_hft_replay(events, config)

    assert result.metrics["trade_count"] == 20
    assert result.metrics["gross_pnl_before_fees"] < 0
    assert result.metrics["total_fees"] > 0
    assert result.metrics["estimated_spread_cost"] > 0
    assert result.metrics["net_pnl"] < result.metrics["gross_pnl_before_fees"]
    assert result.metrics["net_return"] < 0
    assert result.metrics["profit_factor"] == pytest.approx(0.0)
    assert all(trade.entry_price > trade.exit_price for trade in result.trades)


def test_higher_taker_cost_cannot_improve_identical_replay() -> None:
    events = tuple(
        _event(
            index + 1,
            mid=100.0 + (0.04 if index % 4 in (1, 2) else 0.0),
            positive=index % 2 == 0,
        )
        for index in range(80)
    )
    base_config = HFTSimulationConfig(
        initial_cash=100_000.0,
        order_notional=1_000.0,
        taker_fee_rate=0.0005,
        latency_steps=0,
    )
    base = simulate_hft_replay(events, base_config)
    stressed = simulate_hft_replay(
        events,
        dataclasses.replace(base_config, taker_fee_rate=0.0010),
    )

    assert stressed.metrics["trade_count"] == base.metrics["trade_count"]
    assert [decision for decision in stressed.decisions] == [
        decision for decision in base.decisions
    ]
    assert stressed.metrics["gross_pnl_before_fees"] == pytest.approx(
        base.metrics["gross_pnl_before_fees"]
    )
    assert stressed.metrics["total_fees"] > base.metrics["total_fees"]
    assert stressed.metrics["net_pnl"] < base.metrics["net_pnl"]
    assert stressed.metrics["net_return"] < base.metrics["net_return"]


def test_latency_fills_only_at_due_event_and_never_peeks_ahead() -> None:
    events = (
        _event(1, mid=100.0, positive=True),
        _event(2, mid=110.0, positive=False),
        _event(3, mid=120.0, positive=False),
        _event(4, mid=120.0, positive=False),
    )
    config = HFTSimulationConfig(
        initial_cash=100_000.0,
        order_notional=1_000.0,
        taker_fee_rate=0.0,
        latency_steps=2,
        max_holding_steps=10,
    )
    result = simulate_hft_replay(events, config)

    assert result.trades[0].entry_decision_sequence == 1
    assert result.trades[0].entry_fill_sequence == 3
    assert result.trades[0].entry_price == pytest.approx(events[2].ask_price)
    assert result.trades[0].exit_reason == "end_of_replay"

    changed_future = list(events)
    changed_future[2] = _event(3, mid=150.0, positive=False)
    changed = simulate_hft_replay(tuple(changed_future), config)
    assert changed.trades[0].entry_decision_sequence == 1
    assert changed.trades[0].entry_fill_sequence == 3
    assert changed.trades[0].entry_price == pytest.approx(
        changed_future[2].ask_price
    )
    assert changed.decisions[0] == result.decisions[0]


def test_signal_uses_only_current_event_and_stream_provenance_is_strict() -> None:
    config = HFTSimulationConfig()
    current = _event(1, positive=True)
    assert event_signal(current, config) == pytest.approx(0.83)

    mixed = (
        current,
        dataclasses.replace(
            _event(2, positive=False),
            source_kind="synthetic",
            scenario="null_alpha",
        ),
    )
    with pytest.raises(ValueError, match="mix provenance"):
        validate_event_stream(mixed)


def test_seeded_null_alpha_does_not_overcome_taker_costs() -> None:
    events = generate_synthetic_microstructure(
        8_000,
        seed=23,
        scenario="null_alpha",
    )
    result = simulate_hft_replay(
        events,
        HFTSimulationConfig(
            initial_cash=10_000_000.0,
            order_notional=250_000.0,
            taker_fee_rate=0.0005,
            latency_steps=1,
        ),
    )

    assert result.metrics["trade_count"] > 20
    assert result.metrics["net_return"] < 0
    assert result.metrics["total_fees"] > 0
    assert result.metrics["estimated_spread_cost"] > 0
