from __future__ import annotations

import dataclasses
import uuid
from dataclasses import dataclass
from typing import Any

import pandas as pd

from coinpilot.config import RiskConfig


HALT_ACTIVE = "ACTIVE"
HALT_PENDING = "HALT_PENDING"
HALTED = "HALTED"


def _event_id(prefix: str, *parts: object) -> str:
    material = "|".join(str(part) for part in parts)
    return f"{prefix}-{uuid.uuid5(uuid.NAMESPACE_URL, material).hex}"


@dataclass(slots=True)
class PortfolioState:
    market: str
    cash: float
    peak_equity: float
    schema_version: int = 1
    revision: int = 0
    config_fingerprint: str | None = None
    quantity: float = 0.0
    entry_time: str | None = None
    entry_signal_time: str | None = None
    entry_horizon_exit_time: str | None = None
    entry_price: float = 0.0
    entry_notional: float = 0.0
    entry_fee: float = 0.0
    entry_stop_distance_pct: float = 0.0
    peak_position_price: float = 0.0
    realized_pnl: float = 0.0
    halt_state: str = HALT_ACTIVE
    cooldown_remaining: int = 0
    pending_probability: float | None = None
    pending_expected_gross_return: float | None = None
    pending_calibration_buffer: float | None = None
    pending_expected_net_edge: float | None = None
    pending_signal_eligible: bool = False
    pending_no_trade_reason: str | None = None
    pending_atr_pct: float | None = None
    pending_signal_time: str | None = None
    pending_model_id: str | None = None
    last_bar_time: str | None = None
    last_ticker_exchange_time: str | None = None
    updated_at: str | None = None

    @classmethod
    def initial(
        cls,
        market: str,
        cash: float,
        *,
        config_fingerprint: str | None = None,
    ) -> "PortfolioState":
        return cls(
            market=market,
            cash=float(cash),
            peak_equity=float(cash),
            config_fingerprint=config_fingerprint,
        )

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "PortfolioState":
        allowed = {field.name for field in dataclasses.fields(cls)}
        values = {key: value for key, value in raw.items() if key in allowed}
        return cls(**values)

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)

    def equity(self, mark_price: float) -> float:
        return float(self.cash + self.quantity * mark_price)


@dataclass(frozen=True, slots=True)
class Fill:
    event_id: str
    timestamp: str
    side: str
    reason: str
    signal_time: str | None
    model_id: str | None
    raw_price: float
    fill_price: float
    quantity: float
    notional: float
    fee: float
    slippage_cost: float

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


@dataclass(frozen=True, slots=True)
class Trade:
    entry_time: str
    exit_time: str
    entry_signal_time: str | None
    entry_price: float
    exit_price: float
    quantity: float
    entry_notional: float
    exit_notional: float
    fees: float
    slippage_cost: float
    pnl: float
    return_pct: float
    exit_reason: str

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


class SimulatedBroker:
    def __init__(self, state: PortfolioState, config: RiskConfig) -> None:
        self.state = state
        self.config = config

    @property
    def slippage_rate(self) -> float:
        return self.config.slippage_bps / 10_000

    def buy(
        self,
        *,
        timestamp: pd.Timestamp,
        raw_price: float,
        requested_notional: float,
        stop_distance_pct: float,
        reason: str,
        signal_time: str | None,
        model_id: str | None,
    ) -> Fill:
        if self.state.quantity > 1e-15:
            raise RuntimeError("Cannot buy while a long position is already open")
        fill_price = float(raw_price * (1 + self.slippage_rate))
        maximum_notional = self.state.cash / (1 + self.config.fee_rate)
        notional = float(min(requested_notional, maximum_notional))
        if notional <= 0:
            raise RuntimeError("Buy notional must be positive")
        quantity = notional / fill_price
        fee = notional * self.config.fee_rate
        self.state.cash -= notional + fee
        if self.state.cash < -1e-7:
            raise RuntimeError("Buy created a negative cash balance")
        self.state.cash = max(self.state.cash, 0.0)
        timestamp_text = pd.Timestamp(timestamp).isoformat()
        self.state.quantity = quantity
        self.state.entry_time = timestamp_text
        self.state.entry_signal_time = signal_time
        self.state.entry_price = fill_price
        self.state.entry_notional = notional
        self.state.entry_fee = fee
        self.state.entry_stop_distance_pct = stop_distance_pct
        self.state.peak_position_price = fill_price

        return Fill(
            event_id=_event_id(
                "fill", self.state.market, timestamp_text, "buy", reason, signal_time
            ),
            timestamp=timestamp_text,
            side="buy",
            reason=reason,
            signal_time=signal_time,
            model_id=model_id,
            raw_price=float(raw_price),
            fill_price=fill_price,
            quantity=quantity,
            notional=notional,
            fee=fee,
            slippage_cost=(fill_price - raw_price) * quantity,
        )

    def sell_all(
        self,
        *,
        timestamp: pd.Timestamp,
        raw_price: float,
        reason: str,
        signal_time: str | None,
        model_id: str | None,
    ) -> tuple[Fill, Trade]:
        if self.state.quantity <= 0:
            raise RuntimeError("Cannot sell without inventory")
        timestamp_text = pd.Timestamp(timestamp).isoformat()
        quantity = self.state.quantity
        fill_price = float(raw_price * (1 - self.slippage_rate))
        notional = quantity * fill_price
        fee = notional * self.config.fee_rate
        proceeds = notional - fee
        total_cost = self.state.entry_notional + self.state.entry_fee
        pnl = proceeds - total_cost
        entry_time = self.state.entry_time
        if entry_time is None:
            raise RuntimeError("Open position is missing its entry timestamp")
        entry_slippage = max(
            0.0,
            self.state.entry_notional
            - quantity
            * (self.state.entry_price / (1 + self.slippage_rate)),
        )
        exit_slippage = max(0.0, (raw_price - fill_price) * quantity)

        fill = Fill(
            event_id=_event_id(
                "fill", self.state.market, timestamp_text, "sell", reason, entry_time
            ),
            timestamp=timestamp_text,
            side="sell",
            reason=reason,
            signal_time=signal_time,
            model_id=model_id,
            raw_price=float(raw_price),
            fill_price=fill_price,
            quantity=quantity,
            notional=notional,
            fee=fee,
            slippage_cost=exit_slippage,
        )
        trade = Trade(
            entry_time=entry_time,
            exit_time=timestamp_text,
            entry_signal_time=self.state.entry_signal_time,
            entry_price=self.state.entry_price,
            exit_price=fill_price,
            quantity=quantity,
            entry_notional=self.state.entry_notional,
            exit_notional=notional,
            fees=self.state.entry_fee + fee,
            slippage_cost=entry_slippage + exit_slippage,
            pnl=pnl,
            return_pct=pnl / total_cost if total_cost > 0 else 0.0,
            exit_reason=reason,
        )
        self.state.cash += proceeds
        self.state.realized_pnl += pnl
        self.state.quantity = 0.0
        self.state.entry_time = None
        self.state.entry_signal_time = None
        self.state.entry_horizon_exit_time = None
        self.state.entry_price = 0.0
        self.state.entry_notional = 0.0
        self.state.entry_fee = 0.0
        self.state.entry_stop_distance_pct = 0.0
        self.state.peak_position_price = 0.0
        return fill, trade
