from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from coinpilot.config import RiskConfig


@dataclass(frozen=True, slots=True)
class EntryRiskDecision:
    accepted: bool
    notional: float = 0.0
    stop_distance_pct: float = 0.0
    reason: str = ""


class RiskManager:
    def __init__(self, config: RiskConfig) -> None:
        self.config = config

    def size_entry(
        self,
        *,
        equity: float,
        cash: float,
        raw_price: float,
        atr_pct: float | None,
    ) -> EntryRiskDecision:
        if equity <= 0 or cash <= 0 or raw_price <= 0:
            return EntryRiskDecision(False, reason="invalid_account_or_price")
        if atr_pct is None or not np.isfinite(atr_pct) or atr_pct <= 0:
            return EntryRiskDecision(False, reason="invalid_atr")

        raw_stop_distance = self.config.atr_stop_multiple * float(atr_pct)
        if raw_stop_distance > self.config.maximum_stop_pct:
            return EntryRiskDecision(False, reason="volatility_too_high")
        stop_distance = max(raw_stop_distance, self.config.minimum_stop_pct)

        trading_cost_buffer = 2 * (
            self.config.fee_rate + self.config.slippage_bps / 10_000
        )
        loss_fraction = stop_distance + trading_cost_buffer
        risk_cap = equity * self.config.risk_per_trade / loss_fraction
        allocation_cap = equity * self.config.max_position_fraction
        cash_cap = cash / (1 + self.config.fee_rate)
        notional = min(risk_cap, allocation_cap, cash_cap)
        if notional < self.config.minimum_order_quote:
            return EntryRiskDecision(False, reason="below_minimum_order")
        return EntryRiskDecision(
            True,
            notional=float(notional),
            stop_distance_pct=float(stop_distance),
            reason="accepted",
        )

    def drawdown(self, equity: float, peak_equity: float) -> float:
        if peak_equity <= 0:
            return 0.0
        return max(0.0, 1 - equity / peak_equity)
