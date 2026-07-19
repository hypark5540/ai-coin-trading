"""Deterministic, research-only event replay for spot HFT experiments.

This module deliberately does not place orders or imply that synthetic results
are evidence of live profitability.  It provides:

* an explicit schema for either synthetic or real *observations*;
* a causal signal using only fields available on the current event;
* price-conservative L1 taker execution (buy at ask, sell at bid), with an
  explicit full-fill/no-impact limitation;
* event-step latency, fees, spread-cost attribution, and forced liquidation;
* deterministic null-alpha and weak-alpha synthetic microstructure scenarios.

Real observations can be constructed with ``source_kind="real_observation"``.
The simulator rejects streams that mix real and synthetic provenance.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Literal, Sequence

import numpy as np


SourceKind = Literal["synthetic", "real_observation"]
SyntheticScenario = Literal["null_alpha", "weak_alpha"]


@dataclass(frozen=True, slots=True)
class MicrostructureEvent:
    """One causally ordered top-of-book observation.

    ``trade_flow`` is a normalized, contemporaneously observable signed-flow
    feature in [-1, 1].  Positive values represent buyer-initiated pressure.
    No future return or future book field is stored on the event.
    """

    sequence: int
    timestamp_ns: int
    bid_price: float
    ask_price: float
    bid_size: float
    ask_size: float
    trade_flow: float
    source_kind: SourceKind
    scenario: str

    @property
    def mid_price(self) -> float:
        return (self.bid_price + self.ask_price) / 2.0

    @property
    def spread(self) -> float:
        return self.ask_price - self.bid_price

    @property
    def spread_bps(self) -> float:
        return self.spread / self.mid_price * 10_000.0

    @property
    def book_imbalance(self) -> float:
        depth = self.bid_size + self.ask_size
        return (self.bid_size - self.ask_size) / depth


@dataclass(frozen=True, slots=True)
class HFTSimulationConfig:
    """Execution and signal assumptions for one replay.

    ``latency_steps=0`` permits a fill at the same observation used for the
    decision.  A positive value fills at the book observed that many events
    later.  Signals are never recomputed with future events for an already
    pending order.
    """

    initial_cash: float = 10_000_000.0
    order_notional: float = 1_000_000.0
    maker_fee_rate: float = 0.0005
    taker_fee_rate: float = 0.0005
    latency_steps: int = 1
    entry_threshold: float = 0.45
    exit_threshold: float = 0.0
    max_holding_steps: int = 25
    signal_book_weight: float = 0.70
    signal_flow_weight: float = 0.30
    equity_sample_points: int = 250

    def validate(self) -> HFTSimulationConfig:
        finite_values = {
            "initial_cash": self.initial_cash,
            "order_notional": self.order_notional,
            "maker_fee_rate": self.maker_fee_rate,
            "taker_fee_rate": self.taker_fee_rate,
            "entry_threshold": self.entry_threshold,
            "exit_threshold": self.exit_threshold,
            "signal_book_weight": self.signal_book_weight,
            "signal_flow_weight": self.signal_flow_weight,
        }
        for name, value in finite_values.items():
            if not math.isfinite(float(value)):
                raise ValueError(f"{name} must be finite")
        if self.initial_cash <= 0:
            raise ValueError("initial_cash must be positive")
        if self.order_notional <= 0:
            raise ValueError("order_notional must be positive")
        if not 0 <= self.maker_fee_rate < 1:
            raise ValueError("maker_fee_rate must be in [0, 1)")
        if not 0 <= self.taker_fee_rate < 1:
            raise ValueError("taker_fee_rate must be in [0, 1)")
        if (
            isinstance(self.latency_steps, bool)
            or not isinstance(self.latency_steps, int)
            or self.latency_steps < 0
        ):
            raise ValueError("latency_steps must be a non-negative integer")
        if (
            isinstance(self.max_holding_steps, bool)
            or not isinstance(self.max_holding_steps, int)
            or self.max_holding_steps < 1
        ):
            raise ValueError("max_holding_steps must be a positive integer")
        if not -1 <= self.entry_threshold <= 1:
            raise ValueError("entry_threshold must be in [-1, 1]")
        if not -1 <= self.exit_threshold <= 1:
            raise ValueError("exit_threshold must be in [-1, 1]")
        if self.exit_threshold >= self.entry_threshold:
            raise ValueError("exit_threshold must be below entry_threshold")
        if self.signal_book_weight < 0 or self.signal_flow_weight < 0:
            raise ValueError("signal weights must be non-negative")
        if self.signal_book_weight + self.signal_flow_weight <= 0:
            raise ValueError("at least one signal weight must be positive")
        if (
            isinstance(self.equity_sample_points, bool)
            or not isinstance(self.equity_sample_points, int)
            or self.equity_sample_points < 2
        ):
            raise ValueError("equity_sample_points must be at least two")
        return self


@dataclass(frozen=True, slots=True)
class HFTDecision:
    action: Literal["buy", "sell"]
    reason: Literal["entry_signal", "signal_exit", "max_holding", "end_of_replay"]
    decision_event_index: int
    decision_sequence: int
    due_event_index: int
    signal: float


@dataclass(frozen=True, slots=True)
class HFTTrade:
    entry_decision_sequence: int
    entry_fill_sequence: int
    exit_decision_sequence: int
    exit_fill_sequence: int
    entry_price: float
    exit_price: float
    quantity: float
    entry_notional: float
    exit_notional: float
    gross_pnl_before_fees: float
    fees: float
    estimated_spread_cost: float
    net_pnl: float
    holding_steps: int
    exit_reason: str


@dataclass(frozen=True, slots=True)
class EquityPoint:
    event_index: int
    sequence: int
    timestamp_ns: int
    equity: float
    cash: float
    quantity: float
    liquidation_price: float
    drawdown: float


@dataclass(frozen=True, slots=True)
class HFTSimulationResult:
    """Replay output with sampled equity and full completed-trade records."""

    metrics: dict[str, int | float | str | bool | None]
    equity_curve: tuple[EquityPoint, ...]
    trades: tuple[HFTTrade, ...]
    decisions: tuple[HFTDecision, ...]


@dataclass(slots=True)
class _OpenPosition:
    quantity: float
    entry_price: float
    entry_notional: float
    entry_fee: float
    entry_spread_cost: float
    entry_event_index: int
    entry_decision_sequence: int
    entry_fill_sequence: int


def _finite(value: float) -> bool:
    return math.isfinite(float(value))


def validate_event_stream(
    events: Sequence[MicrostructureEvent],
) -> tuple[SourceKind, str]:
    """Validate ordering, book integrity, and a single provenance."""

    if len(events) < 2:
        raise ValueError("at least two microstructure events are required")
    source_kind = events[0].source_kind
    scenario = events[0].scenario
    if source_kind not in ("synthetic", "real_observation"):
        raise ValueError("source_kind must be synthetic or real_observation")
    if not scenario.strip():
        raise ValueError("scenario must be non-empty")

    previous_sequence: int | None = None
    previous_timestamp: int | None = None
    for index, event in enumerate(events):
        if event.source_kind != source_kind or event.scenario != scenario:
            raise ValueError("event stream cannot mix provenance or scenarios")
        if isinstance(event.sequence, bool) or not isinstance(event.sequence, int):
            raise ValueError(f"event {index} sequence must be an integer")
        if (
            isinstance(event.timestamp_ns, bool)
            or not isinstance(event.timestamp_ns, int)
            or event.timestamp_ns < 0
        ):
            raise ValueError(f"event {index} timestamp_ns must be non-negative")
        if previous_sequence is not None and event.sequence <= previous_sequence:
            raise ValueError("event sequences must be strictly increasing")
        if previous_timestamp is not None and event.timestamp_ns < previous_timestamp:
            raise ValueError("event timestamps must be monotonic")
        numeric_fields = (
            event.bid_price,
            event.ask_price,
            event.bid_size,
            event.ask_size,
            event.trade_flow,
        )
        if not all(_finite(value) for value in numeric_fields):
            raise ValueError(f"event {index} contains a non-finite field")
        if event.bid_price <= 0 or event.ask_price <= event.bid_price:
            raise ValueError(f"event {index} must have a positive crossed-free book")
        if event.bid_size <= 0 or event.ask_size <= 0:
            raise ValueError(f"event {index} sizes must be positive")
        if not -1 <= event.trade_flow <= 1:
            raise ValueError(f"event {index} trade_flow must be in [-1, 1]")
        previous_sequence = event.sequence
        previous_timestamp = event.timestamp_ns
    return source_kind, scenario


def event_signal(
    event: MicrostructureEvent,
    config: HFTSimulationConfig,
) -> float:
    """Return a current-event-only signal in [-1, 1]."""

    weight_sum = config.signal_book_weight + config.signal_flow_weight
    value = (
        config.signal_book_weight * event.book_imbalance
        + config.signal_flow_weight * event.trade_flow
    ) / weight_sum
    return float(np.clip(value, -1.0, 1.0))


def generate_synthetic_microstructure(
    event_count: int = 5_000,
    *,
    seed: int = 7,
    scenario: SyntheticScenario = "null_alpha",
    initial_mid_price: float = 100_000_000.0,
    interval_ns: int = 100_000_000,
    volatility_bps: float = 0.80,
    base_spread_bps: float = 1.20,
    weak_alpha_bps: float = 0.18,
) -> tuple[MicrostructureEvent, ...]:
    """Generate a reproducible teaching/research stream.

    In ``null_alpha`` the current imbalance has no role in the next mid-price
    move.  In ``weak_alpha`` it adds a small drift to the *next* move, so the
    causal relationship is explicit without embedding future fields into the
    event.  Neither scenario is calibrated evidence about a real venue.
    """

    if isinstance(event_count, bool) or not isinstance(event_count, int):
        raise ValueError("event_count must be an integer")
    if event_count < 2:
        raise ValueError("event_count must be at least two")
    if scenario not in ("null_alpha", "weak_alpha"):
        raise ValueError("scenario must be null_alpha or weak_alpha")
    if not _finite(initial_mid_price) or initial_mid_price <= 0:
        raise ValueError("initial_mid_price must be positive and finite")
    if (
        isinstance(interval_ns, bool)
        or not isinstance(interval_ns, int)
        or interval_ns <= 0
    ):
        raise ValueError("interval_ns must be a positive integer")
    for name, value in (
        ("volatility_bps", volatility_bps),
        ("base_spread_bps", base_spread_bps),
        ("weak_alpha_bps", weak_alpha_bps),
    ):
        if not _finite(value) or value < 0:
            raise ValueError(f"{name} must be finite and non-negative")
    if base_spread_bps == 0:
        raise ValueError("base_spread_bps must be positive")

    rng = np.random.default_rng(seed)
    start_ns = 1_735_689_600_000_000_000  # 2025-01-01T00:00:00Z
    mid = float(initial_mid_price)
    latent_pressure = 0.0
    alpha_bps = weak_alpha_bps if scenario == "weak_alpha" else 0.0
    output: list[MicrostructureEvent] = []

    for index in range(event_count):
        latent_pressure = float(
            np.clip(
                0.88 * latent_pressure + rng.normal(0.0, 0.34),
                -0.97,
                0.97,
            )
        )
        trade_flow = float(
            np.clip(0.72 * latent_pressure + rng.normal(0.0, 0.30), -1.0, 1.0)
        )
        displayed_imbalance = float(
            np.clip(0.82 * latent_pressure + rng.normal(0.0, 0.18), -0.98, 0.98)
        )
        total_depth = float(rng.lognormal(mean=1.0, sigma=0.35))
        bid_size = total_depth * (1.0 + displayed_imbalance) / 2.0
        ask_size = total_depth * (1.0 - displayed_imbalance) / 2.0
        spread_bps = max(
            base_spread_bps * float(rng.lognormal(mean=-0.04, sigma=0.18)),
            0.01,
        )
        half_spread = mid * spread_bps / 20_000.0
        output.append(
            MicrostructureEvent(
                sequence=index + 1,
                timestamp_ns=start_ns + index * interval_ns,
                bid_price=mid - half_spread,
                ask_price=mid + half_spread,
                bid_size=bid_size,
                ask_size=ask_size,
                trade_flow=trade_flow,
                source_kind="synthetic",
                scenario=scenario,
            )
        )

        # Only current-event fields influence the next event's optional drift.
        current_signal = 0.70 * displayed_imbalance + 0.30 * trade_flow
        move_bps = rng.normal(0.0, volatility_bps) + alpha_bps * current_signal
        mid *= math.exp(move_bps / 10_000.0)

    return tuple(output)


def _sample_indices(length: int, requested: int) -> tuple[int, ...]:
    if requested >= length:
        return tuple(range(length))
    return tuple(
        sorted(
            {
                int(round(value))
                for value in np.linspace(0, length - 1, requested)
            }
        )
    )


def _safe_profit_factor(trades: Sequence[HFTTrade]) -> float | None:
    profits = sum(max(trade.net_pnl, 0.0) for trade in trades)
    losses = -sum(min(trade.net_pnl, 0.0) for trade in trades)
    if losses == 0:
        return None
    return float(profits / losses)


def simulate_hft_replay(
    events: Sequence[MicrostructureEvent],
    config: HFTSimulationConfig | None = None,
) -> HFTSimulationResult:
    """Replay a long-only strategy without future-aware decisions.

    Entry and exit decisions use the current event only.  Fills occur at the
    event indexed by ``decision_index + latency_steps`` and use that event's
    ask (buy) or bid (sell).  The maker fee is reported for assumption
    completeness but all fills in this path are taker fills.  Price selection
    is adverse; available L1 size, multi-level sweep, and market impact are not
    modeled, so the full-fill liquidity assumption is optimistic.
    """

    selected = (config or HFTSimulationConfig()).validate()
    source_kind, scenario = validate_event_stream(events)
    event_list = tuple(events)

    cash = float(selected.initial_cash)
    position: _OpenPosition | None = None
    pending: HFTDecision | None = None
    decisions: list[HFTDecision] = []
    trades: list[HFTTrade] = []
    equity_rows: list[
        tuple[int, MicrostructureEvent, float, float, float, float]
    ] = []
    total_fees = 0.0
    total_spread_cost = 0.0
    total_turnover_notional = 0.0
    cancelled_orders = 0

    def fill_buy(
        event_index: int,
        event: MicrostructureEvent,
        decision: HFTDecision,
    ) -> bool:
        nonlocal cash, position, total_fees, total_spread_cost
        nonlocal total_turnover_notional
        available_notional = cash / (1.0 + selected.taker_fee_rate)
        notional = min(selected.order_notional, available_notional)
        if notional <= 0:
            return False
        quantity = notional / event.ask_price
        fee = notional * selected.taker_fee_rate
        spread_cost = quantity * (event.ask_price - event.mid_price)
        cash -= notional + fee
        position = _OpenPosition(
            quantity=quantity,
            entry_price=event.ask_price,
            entry_notional=notional,
            entry_fee=fee,
            entry_spread_cost=spread_cost,
            entry_event_index=event_index,
            entry_decision_sequence=decision.decision_sequence,
            entry_fill_sequence=event.sequence,
        )
        total_fees += fee
        total_spread_cost += spread_cost
        total_turnover_notional += notional
        return True

    def fill_sell(
        event_index: int,
        event: MicrostructureEvent,
        decision: HFTDecision,
    ) -> None:
        nonlocal cash, position, total_fees, total_spread_cost
        nonlocal total_turnover_notional
        if position is None:
            return
        exit_notional = position.quantity * event.bid_price
        fee = exit_notional * selected.taker_fee_rate
        spread_cost = position.quantity * (event.mid_price - event.bid_price)
        gross_pnl = exit_notional - position.entry_notional
        all_fees = position.entry_fee + fee
        net_pnl = gross_pnl - all_fees
        cash += exit_notional - fee
        total_fees += fee
        total_spread_cost += spread_cost
        total_turnover_notional += exit_notional
        trades.append(
            HFTTrade(
                entry_decision_sequence=position.entry_decision_sequence,
                entry_fill_sequence=position.entry_fill_sequence,
                exit_decision_sequence=decision.decision_sequence,
                exit_fill_sequence=event.sequence,
                entry_price=position.entry_price,
                exit_price=event.bid_price,
                quantity=position.quantity,
                entry_notional=position.entry_notional,
                exit_notional=exit_notional,
                gross_pnl_before_fees=gross_pnl,
                fees=all_fees,
                estimated_spread_cost=position.entry_spread_cost + spread_cost,
                net_pnl=net_pnl,
                holding_steps=event_index - position.entry_event_index,
                exit_reason=decision.reason,
            )
        )
        position = None

    for event_index, event in enumerate(event_list):
        filled_this_event = False
        if pending is not None and pending.due_event_index <= event_index:
            if pending.action == "buy":
                fill_buy(event_index, event, pending)
            else:
                fill_sell(event_index, event, pending)
            pending = None
            filled_this_event = True

        signal = event_signal(event, selected)
        if not filled_this_event and pending is None:
            decision: HFTDecision | None = None
            if position is None and signal >= selected.entry_threshold:
                decision = HFTDecision(
                    action="buy",
                    reason="entry_signal",
                    decision_event_index=event_index,
                    decision_sequence=event.sequence,
                    due_event_index=event_index + selected.latency_steps,
                    signal=signal,
                )
            elif position is not None:
                holding_steps = event_index - position.entry_event_index
                if signal <= selected.exit_threshold:
                    decision = HFTDecision(
                        action="sell",
                        reason="signal_exit",
                        decision_event_index=event_index,
                        decision_sequence=event.sequence,
                        due_event_index=event_index + selected.latency_steps,
                        signal=signal,
                    )
                elif holding_steps >= selected.max_holding_steps:
                    decision = HFTDecision(
                        action="sell",
                        reason="max_holding",
                        decision_event_index=event_index,
                        decision_sequence=event.sequence,
                        due_event_index=event_index + selected.latency_steps,
                        signal=signal,
                    )
            if decision is not None:
                decisions.append(decision)
                if selected.latency_steps == 0:
                    if decision.action == "buy":
                        fill_buy(event_index, event, decision)
                    else:
                        fill_sell(event_index, event, decision)
                else:
                    pending = decision

        quantity = position.quantity if position is not None else 0.0
        liquidation_value = (
            quantity * event.bid_price * (1.0 - selected.taker_fee_rate)
        )
        equity_rows.append(
            (
                event_index,
                event,
                cash + liquidation_value,
                cash,
                quantity,
                event.bid_price,
            )
        )

    final_event_index = len(event_list) - 1
    final_event = event_list[-1]
    if pending is not None:
        cancelled_orders += 1
        pending = None
    if position is not None:
        final_signal = event_signal(final_event, selected)
        forced = HFTDecision(
            action="sell",
            reason="end_of_replay",
            decision_event_index=final_event_index,
            decision_sequence=final_event.sequence,
            due_event_index=final_event_index,
            signal=final_signal,
        )
        decisions.append(forced)
        fill_sell(final_event_index, final_event, forced)
        equity_rows[-1] = (
            final_event_index,
            final_event,
            cash,
            cash,
            0.0,
            final_event.bid_price,
        )

    equities = np.asarray([row[2] for row in equity_rows], dtype=float)
    peaks = np.maximum.accumulate(equities)
    drawdowns = np.divide(equities, peaks) - 1.0
    max_drawdown = max(0.0, float(-np.min(drawdowns)))
    sampled = _sample_indices(len(equity_rows), selected.equity_sample_points)
    curve = tuple(
        EquityPoint(
            event_index=equity_rows[index][0],
            sequence=equity_rows[index][1].sequence,
            timestamp_ns=equity_rows[index][1].timestamp_ns,
            equity=float(equity_rows[index][2]),
            cash=float(equity_rows[index][3]),
            quantity=float(equity_rows[index][4]),
            liquidation_price=float(equity_rows[index][5]),
            drawdown=float(drawdowns[index]),
        )
        for index in sampled
    )

    gross_pnl = float(sum(trade.gross_pnl_before_fees for trade in trades))
    net_pnl = float(cash - selected.initial_cash)
    win_count = sum(trade.net_pnl > 0 for trade in trades)
    spread_bps = np.asarray(
        [event.spread_bps for event in event_list],
        dtype=float,
    )
    metrics: dict[str, int | float | str | bool | None] = {
        "data_kind": source_kind,
        "scenario": scenario,
        "is_synthetic": source_kind == "synthetic",
        "execution_model": "conservative_taker",
        "liquidity_model": "l1_full_fill_no_impact",
        "size_aware": False,
        "event_count": len(event_list),
        "start_timestamp_ns": event_list[0].timestamp_ns,
        "end_timestamp_ns": event_list[-1].timestamp_ns,
        "initial_cash": float(selected.initial_cash),
        "final_equity": float(cash),
        "net_pnl": net_pnl,
        "net_return": float(cash / selected.initial_cash - 1.0),
        "max_drawdown": max_drawdown,
        "profit_factor": _safe_profit_factor(trades),
        "trade_count": len(trades),
        "win_rate": float(win_count / len(trades)) if trades else None,
        "turnover": float(total_turnover_notional / selected.initial_cash),
        "gross_pnl_before_fees": gross_pnl,
        "total_fees": float(total_fees),
        "estimated_spread_cost": float(total_spread_cost),
        "maker_fee_rate": float(selected.maker_fee_rate),
        "taker_fee_rate": float(selected.taker_fee_rate),
        "maker_fill_count": 0,
        "taker_fill_count": len(trades) * 2,
        "latency_steps": selected.latency_steps,
        "cancelled_order_count": cancelled_orders,
        "mean_spread_bps": float(spread_bps.mean()),
        "p95_spread_bps": float(np.quantile(spread_bps, 0.95)),
        "equity_sample_count": len(curve),
    }
    return HFTSimulationResult(
        metrics=metrics,
        equity_curve=curve,
        trades=tuple(trades),
        decisions=tuple(decisions),
    )
