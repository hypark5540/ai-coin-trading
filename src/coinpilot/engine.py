from __future__ import annotations

import dataclasses
import uuid
from dataclasses import dataclass
from typing import Any, Mapping

import numpy as np
import pandas as pd

from coinpilot.broker import (
    HALT_ACTIVE,
    HALT_PENDING,
    HALTED,
    Fill,
    PortfolioState,
    SimulatedBroker,
    Trade,
)
from coinpilot.config import ModelConfig, RiskConfig
from coinpilot.risk import RiskManager


def _engine_event(
    event_type: str,
    timestamp: pd.Timestamp,
    market: str,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    stable_payload = "|".join(f"{key}={payload[key]}" for key in sorted(payload))
    event_id = uuid.uuid5(
        uuid.NAMESPACE_URL,
        f"{market}|{pd.Timestamp(timestamp).isoformat()}|{event_type}|{stable_payload}",
    ).hex
    return {
        "event_id": f"event-{event_id}",
        "timestamp": pd.Timestamp(timestamp).isoformat(),
        "event_type": event_type,
        "payload": dict(payload),
    }


def _fill_event(fill: Fill) -> dict[str, Any]:
    return {
        "event_id": fill.event_id,
        "timestamp": fill.timestamp,
        "event_type": "fill",
        "payload": fill.to_dict(),
    }


@dataclass(frozen=True, slots=True)
class BarStep:
    timestamp: str
    equity: float
    cash: float
    quantity: float
    close: float
    drawdown: float
    halt_state: str
    pending_probability: float | None
    pending_expected_net_edge: float | None
    pending_signal_eligible: bool
    events: tuple[dict[str, Any], ...]
    trades: tuple[Trade, ...]

    def equity_record(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "equity": self.equity,
            "cash": self.cash,
            "quantity": self.quantity,
            "close": self.close,
            "drawdown": self.drawdown,
            "halt_state": self.halt_state,
            "pending_probability": self.pending_probability,
            "pending_expected_net_edge": self.pending_expected_net_edge,
            "pending_signal_eligible": self.pending_signal_eligible,
        }


class BarExecutionEngine:
    """Shared next-bar execution engine used by backtest and paper modes."""

    def __init__(
        self,
        *,
        state: PortfolioState,
        risk_config: RiskConfig,
        model_config: ModelConfig,
        interval_minutes: int,
        halt_on_data_gap: bool = True,
    ) -> None:
        self.state = state
        self.risk_config = risk_config
        self.model_config = model_config
        self.interval_minutes = interval_minutes
        self.halt_on_data_gap = halt_on_data_gap
        self.broker = SimulatedBroker(state, risk_config)
        self.risk = RiskManager(risk_config)

    def process_bar(
        self,
        bar: Mapping[str, Any],
        *,
        current_probability: float | None,
        current_atr_pct: float | None,
        current_model_id: str | None,
        current_expected_gross_return: float | None = None,
        current_calibration_buffer: float | None = None,
        current_expected_net_edge: float | None = None,
        current_signal_eligible: bool = False,
        current_no_trade_reason: str | None = None,
    ) -> BarStep:
        timestamp = pd.Timestamp(bar["timestamp"])
        if timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize("UTC")
        else:
            timestamp = timestamp.tz_convert("UTC")
        if str(bar["market"]) != self.state.market:
            raise ValueError("Bar market does not match portfolio market")
        raw_open = float(bar["open"])
        high = float(bar["high"])
        low = float(bar["low"])
        close = float(bar["close"])
        events: list[dict[str, Any]] = []
        trades: list[Trade] = []
        gap_requires_exit = False

        if self.state.last_bar_time is not None:
            previous_time = pd.Timestamp(self.state.last_bar_time)
            if timestamp <= previous_time:
                raise ValueError("Bars must be processed once in increasing order")
            expected = pd.Timedelta(minutes=self.interval_minutes)
            if timestamp - previous_time != expected:
                self.state.pending_probability = None
                self.state.pending_expected_gross_return = None
                self.state.pending_calibration_buffer = None
                self.state.pending_expected_net_edge = None
                self.state.pending_signal_eligible = False
                self.state.pending_no_trade_reason = None
                self.state.pending_atr_pct = None
                self.state.pending_signal_time = None
                self.state.pending_model_id = None
                if self.halt_on_data_gap:
                    self.state.halt_state = (
                        HALT_PENDING if self.state.quantity > 0 else HALTED
                    )
                else:
                    gap_requires_exit = self.state.quantity > 0
                events.append(
                    _engine_event(
                        "market_data_gap",
                        timestamp,
                        self.state.market,
                        {
                            "previous_timestamp": previous_time.isoformat(),
                            "halt_state": self.state.halt_state,
                        },
                    )
                )

        execution_probability = self.state.pending_probability
        execution_expected_edge = self.state.pending_expected_net_edge
        execution_signal_eligible = self.state.pending_signal_eligible
        execution_atr = self.state.pending_atr_pct
        execution_signal_time = self.state.pending_signal_time
        execution_model_id = self.state.pending_model_id
        cooldown_at_start = self.state.cooldown_remaining
        stop_triggered = False
        position_exited_this_bar = False

        if self.state.quantity > 0:
            if gap_requires_exit:
                fill, trade = self.broker.sell_all(
                    timestamp=timestamp,
                    raw_price=raw_open,
                    reason="market_data_gap",
                    signal_time=execution_signal_time,
                    model_id=execution_model_id,
                )
                events.append(_fill_event(fill))
                trades.append(trade)
                position_exited_this_bar = True
                if self.state.halt_state == HALT_PENDING:
                    self.state.halt_state = HALTED
            elif self.state.halt_state in (HALT_PENDING, HALTED):
                fill, trade = self.broker.sell_all(
                    timestamp=timestamp,
                    raw_price=raw_open,
                    reason="risk_halt",
                    signal_time=execution_signal_time,
                    model_id=execution_model_id,
                )
                events.append(_fill_event(fill))
                trades.append(trade)
                position_exited_this_bar = True
                self.state.halt_state = HALTED
            elif (
                self.model_config.signal_mode == "expected_return"
                and self.state.entry_horizon_exit_time is not None
                and timestamp
                >= pd.Timestamp(self.state.entry_horizon_exit_time)
            ):
                fill, trade = self.broker.sell_all(
                    timestamp=timestamp,
                    raw_price=raw_open,
                    reason="horizon_exit",
                    signal_time=execution_signal_time,
                    model_id=execution_model_id,
                )
                events.append(_fill_event(fill))
                trades.append(trade)
                position_exited_this_bar = True
            elif (
                self.model_config.signal_mode == "probability"
                and
                execution_probability is not None
                and np.isfinite(execution_probability)
                and execution_probability <= self.model_config.exit_probability
            ):
                fill, trade = self.broker.sell_all(
                    timestamp=timestamp,
                    raw_price=raw_open,
                    reason="model_exit",
                    signal_time=execution_signal_time,
                    model_id=execution_model_id,
                )
                events.append(_fill_event(fill))
                trades.append(trade)
                position_exited_this_bar = True

        probability_entry = bool(
            self.model_config.signal_mode == "probability"
            and execution_probability is not None
            and np.isfinite(execution_probability)
            and execution_probability >= self.model_config.entry_probability
        )
        expected_return_entry = bool(
            self.model_config.signal_mode == "expected_return"
            and execution_signal_eligible
            and execution_expected_edge is not None
            and np.isfinite(execution_expected_edge)
            and execution_expected_edge >= self.model_config.minimum_edge_pct
        )
        if (
            self.state.quantity <= 0
            and self.state.halt_state == HALT_ACTIVE
            and cooldown_at_start == 0
            and not position_exited_this_bar
            and (probability_entry or expected_return_entry)
        ):
            decision = self.risk.size_entry(
                equity=self.state.cash,
                cash=self.state.cash,
                raw_price=raw_open,
                atr_pct=execution_atr,
            )
            if decision.accepted:
                fill = self.broker.buy(
                    timestamp=timestamp,
                    raw_price=raw_open,
                    requested_notional=decision.notional,
                    stop_distance_pct=decision.stop_distance_pct,
                    reason="model_entry",
                    signal_time=execution_signal_time,
                    model_id=execution_model_id,
                )
                events.append(_fill_event(fill))
                if self.model_config.signal_mode == "expected_return":
                    self.state.entry_horizon_exit_time = (
                        timestamp
                        + pd.Timedelta(
                            minutes=(
                                self.interval_minutes
                                * self.model_config.horizon_bars
                            )
                        )
                    ).isoformat()
            else:
                events.append(
                    _engine_event(
                        "entry_rejected",
                        timestamp,
                        self.state.market,
                        {"reason": decision.reason},
                    )
                )

        if self.state.quantity > 0:
            fixed_stop = self.state.entry_price * (
                1 - self.state.entry_stop_distance_pct
            )
            trailing_stop = self.state.peak_position_price * (
                1 - self.risk_config.trailing_stop_pct
            )
            stop_price = max(fixed_stop, trailing_stop)
            stop_reason = (
                "trailing_stop" if trailing_stop >= fixed_stop else "stop_loss"
            )
            if low <= stop_price:
                raw_exit_price = min(raw_open, stop_price)
                fill, trade = self.broker.sell_all(
                    timestamp=timestamp,
                    raw_price=raw_exit_price,
                    reason=stop_reason,
                    signal_time=execution_signal_time,
                    model_id=execution_model_id,
                )
                events.append(_fill_event(fill))
                trades.append(trade)
                self.state.cooldown_remaining = self.risk_config.cooldown_bars
                stop_triggered = True
                position_exited_this_bar = True
            else:
                self.state.peak_position_price = max(
                    self.state.peak_position_price, high
                )

        if self.state.quantity > 0:
            liquidation_price = close * (
                1 - self.risk_config.slippage_bps / 10_000
            )
            equity = float(
                self.state.cash
                + self.state.quantity
                * liquidation_price
                * (1 - self.risk_config.fee_rate)
            )
        else:
            equity = self.state.cash
        self.state.peak_equity = max(self.state.peak_equity, equity)
        drawdown = self.risk.drawdown(equity, self.state.peak_equity)
        if (
            self.state.halt_state == HALT_ACTIVE
            and drawdown + 1e-12
            >= self.risk_config.max_strategy_drawdown_pct
        ):
            self.state.halt_state = (
                HALT_PENDING if self.state.quantity > 0 else HALTED
            )
            events.append(
                _engine_event(
                    "drawdown_halt",
                    timestamp,
                    self.state.market,
                    {
                        "drawdown": drawdown,
                        "threshold": self.risk_config.max_strategy_drawdown_pct,
                        "halt_state": self.state.halt_state,
                    },
                )
            )

        if cooldown_at_start > 0 and not stop_triggered:
            self.state.cooldown_remaining = max(0, cooldown_at_start - 1)

        probability = (
            float(current_probability)
            if current_probability is not None
            and np.isfinite(current_probability)
            and self.model_config.signal_mode == "probability"
            and self.state.halt_state == HALT_ACTIVE
            else None
        )
        expected_gross_return = (
            float(current_expected_gross_return)
            if current_expected_gross_return is not None
            and np.isfinite(current_expected_gross_return)
            and self.model_config.signal_mode == "expected_return"
            and self.state.halt_state == HALT_ACTIVE
            else None
        )
        calibration_buffer = (
            float(current_calibration_buffer)
            if current_calibration_buffer is not None
            and np.isfinite(current_calibration_buffer)
            and self.model_config.signal_mode == "expected_return"
            and self.state.halt_state == HALT_ACTIVE
            else None
        )
        expected_net_edge = (
            float(current_expected_net_edge)
            if current_expected_net_edge is not None
            and np.isfinite(current_expected_net_edge)
            and self.model_config.signal_mode == "expected_return"
            and self.state.halt_state == HALT_ACTIVE
            else None
        )
        signal_available = (
            probability is not None
            if self.model_config.signal_mode == "probability"
            else expected_net_edge is not None
        )
        atr_value = (
            float(current_atr_pct)
            if current_atr_pct is not None
            and np.isfinite(current_atr_pct)
            and signal_available
            and self.state.halt_state == HALT_ACTIVE
            else None
        )
        self.state.pending_probability = probability
        self.state.pending_expected_gross_return = expected_gross_return
        self.state.pending_calibration_buffer = calibration_buffer
        self.state.pending_expected_net_edge = expected_net_edge
        self.state.pending_signal_eligible = bool(
            signal_available
            and current_signal_eligible
            and self.state.halt_state == HALT_ACTIVE
        )
        self.state.pending_no_trade_reason = (
            current_no_trade_reason if signal_available else None
        )
        self.state.pending_atr_pct = atr_value
        self.state.pending_signal_time = (
            (timestamp + pd.Timedelta(minutes=self.interval_minutes)).isoformat()
            if signal_available
            else None
        )
        self.state.pending_model_id = (
            current_model_id if signal_available else None
        )
        self.state.last_bar_time = timestamp.isoformat()
        self.state.updated_at = pd.Timestamp.now(tz="UTC").isoformat()

        return BarStep(
            timestamp=timestamp.isoformat(),
            equity=equity,
            cash=self.state.cash,
            quantity=self.state.quantity,
            close=close,
            drawdown=drawdown,
            halt_state=self.state.halt_state,
            pending_probability=probability,
            pending_expected_net_edge=expected_net_edge,
            pending_signal_eligible=self.state.pending_signal_eligible,
            events=tuple(events),
            trades=tuple(trades),
        )

    def liquidate(
        self, *, timestamp: pd.Timestamp, raw_price: float
    ) -> tuple[Fill, Trade] | None:
        if self.state.quantity <= 0:
            return None
        fill, trade = self.broker.sell_all(
            timestamp=timestamp,
            raw_price=raw_price,
            reason="end_of_run",
            signal_time=self.state.pending_signal_time,
            model_id=self.state.pending_model_id,
        )
        if self.state.halt_state == HALT_PENDING:
            self.state.halt_state = HALTED
        self.state.updated_at = pd.Timestamp.now(tz="UTC").isoformat()
        return fill, trade
