from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from coinpilot import __version__
from coinpilot.broker import (
    HALT_ACTIVE,
    HALT_PENDING,
    HALTED,
    Fill,
    PortfolioState,
    SimulatedBroker,
    Trade,
)
from coinpilot.config import AppConfig
from coinpilot.data import MarketTicker, UpbitCandleClient, closed_candles
from coinpilot.features import FEATURE_COLUMNS
from coinpilot.risk import RiskManager
from coinpilot.store import SQLiteStore
from coinpilot.strategy import generate_walk_forward_predictions


PAPER_SCHEMA_VERSION = 8
PAPER_STRATEGY_VERSION = 5


class TickerNotReadyError(ValueError):
    """Raised when a ticker cannot safely advance or monitor paper state yet."""


@dataclass(frozen=True, slots=True)
class PaperRunSummary:
    account_key: str
    initialized: bool
    processed_bars: int
    fills: int
    trades: int
    last_bar_time: str | None
    ticker_time: str
    ticker_exchange_time: str
    ticker_price: float
    cash: float
    quantity: float
    equity: float
    halt_state: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "account_key": self.account_key,
            "initialized": self.initialized,
            "processed_bars": self.processed_bars,
            "fills": self.fills,
            "trades": self.trades,
            "last_bar_time": self.last_bar_time,
            "ticker_time": self.ticker_time,
            "ticker_exchange_time": self.ticker_exchange_time,
            "ticker_price": self.ticker_price,
            "cash": self.cash,
            "quantity": self.quantity,
            "equity": self.equity,
            "halt_state": self.halt_state,
        }


@dataclass(frozen=True, slots=True)
class PaperSnapshotResult:
    processed_bar: bool
    equity: float
    events: tuple[dict[str, Any], ...]
    trades: tuple[Trade, ...]


def _paper_run_summary(
    *,
    account_key: str,
    initialized: bool,
    state: PortfolioState,
    ticker: MarketTicker,
    result: PaperSnapshotResult,
) -> PaperRunSummary:
    return PaperRunSummary(
        account_key=account_key,
        initialized=initialized,
        processed_bars=int(result.processed_bar),
        fills=sum(event["event_type"] == "fill" for event in result.events),
        trades=len(result.trades),
        last_bar_time=state.last_bar_time,
        ticker_time=ticker.observed_at.isoformat(),
        ticker_exchange_time=ticker.exchange_timestamp.isoformat(),
        ticker_price=ticker.price,
        cash=state.cash,
        quantity=state.quantity,
        equity=result.equity,
        halt_state=state.halt_state,
    )


def paper_account_key(config: AppConfig) -> str:
    return (
        f"paper-v{PAPER_SCHEMA_VERSION}:{config.paper.account_name}:"
        f"{config.data.market}:{config.data.interval_minutes}m"
    )


def paper_config_fingerprint(config: AppConfig) -> str:
    values = config.as_dict()
    paper_policy = dict(values["paper"])
    paper_policy.pop("account_name", None)
    material = {
        "schema_version": PAPER_SCHEMA_VERSION,
        "strategy_version": PAPER_STRATEGY_VERSION,
        "coinpilot_version": __version__,
        "feature_columns": FEATURE_COLUMNS,
        "market": config.data.market,
        "interval_minutes": config.data.interval_minutes,
        "api_base_url": config.data.api_base_url,
        "model": values["model"],
        "risk": values["risk"],
        "paper_policy": paper_policy,
    }
    return hashlib.sha256(
        json.dumps(material, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _paper_event(
    event_type: str,
    timestamp: pd.Timestamp,
    market: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    stable = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    event_id = uuid.uuid5(
        uuid.NAMESPACE_URL,
        f"{market}|{timestamp.isoformat()}|{event_type}|{stable}",
    ).hex
    return {
        "event_id": f"event-{event_id}",
        "timestamp": timestamp.isoformat(),
        "event_type": event_type,
        "payload": payload,
    }


def _fill_event(fill: Fill) -> dict[str, Any]:
    return {
        "event_id": fill.event_id,
        "timestamp": fill.timestamp,
        "event_type": "fill",
        "payload": fill.to_dict(),
    }


class PaperSnapshotEngine:
    """Executes fresh model decisions at an observed public ticker price.

    It never fills at a historical candle open. If more than one closed bar was
    missed, it halts instead of replaying hypothetical fills.
    """

    def __init__(self, state: PortfolioState, config: AppConfig) -> None:
        self.state = state
        self.config = config
        self.broker = SimulatedBroker(state, config.risk)
        self.risk = RiskManager(config.risk)

    def _liquidation_equity(self, price: float) -> float:
        if self.state.quantity <= 0:
            return self.state.cash
        slippage = self.config.risk.slippage_bps / 10_000
        proceeds = self.state.quantity * price * (1 - slippage)
        return float(self.state.cash + proceeds * (1 - self.config.risk.fee_rate))

    def _clear_pending_signal(self) -> None:
        self.state.pending_probability = None
        self.state.pending_expected_gross_return = None
        self.state.pending_calibration_buffer = None
        self.state.pending_expected_net_edge = None
        self.state.pending_signal_eligible = False
        self.state.pending_no_trade_reason = None
        self.state.pending_atr_pct = None
        self.state.pending_signal_time = None
        self.state.pending_model_id = None

    def _sell(
        self,
        *,
        ticker: MarketTicker,
        reason: str,
        signal_time: str | None,
        model_id: str | None,
        events: list[dict[str, Any]],
        trades: list[Trade],
    ) -> None:
        fill, trade = self.broker.sell_all(
            timestamp=ticker.observed_at,
            raw_price=ticker.price,
            reason=reason,
            signal_time=signal_time,
            model_id=model_id,
        )
        events.append(_fill_event(fill))
        trades.append(trade)

    def process_snapshot(
        self,
        *,
        latest_closed_bar: pd.Series,
        probability: float | None,
        atr_pct: float | None,
        model_id: str | None,
        ticker: MarketTicker,
        expected_gross_return: float | None = None,
        calibration_buffer: float | None = None,
        expected_net_edge: float | None = None,
        signal_eligible: bool = False,
        no_trade_reason: str | None = None,
        model_ready: bool = True,
        model_error: str | None = None,
    ) -> PaperSnapshotResult:
        bar_time = pd.Timestamp(latest_closed_bar["timestamp"])
        if bar_time.tzinfo is None:
            bar_time = bar_time.tz_localize("UTC")
        else:
            bar_time = bar_time.tz_convert("UTC")
        signal_time = bar_time + pd.Timedelta(
            minutes=self.config.data.interval_minutes
        )
        if ticker.market != self.state.market:
            raise ValueError("Ticker market does not match paper account")
        if ticker.observed_at < signal_time:
            raise TickerNotReadyError(
                "Ticker observation predates the latest closed candle"
            )
        if ticker.exchange_timestamp < signal_time:
            raise TickerNotReadyError(
                "Ticker has no exchange trade at or after the latest candle close"
            )
        ticker_age = (
            ticker.observed_at - ticker.exchange_timestamp
        ).total_seconds()
        if ticker_age < -5 or ticker_age > self.config.paper.max_ticker_age_seconds:
            raise TickerNotReadyError(
                f"Ticker is stale or clock-skewed: age={ticker_age:.3f}s"
            )
        if (
            self.state.updated_at is not None
            and ticker.observed_at < pd.Timestamp(self.state.updated_at)
        ):
            raise TickerNotReadyError(
                "Ticker observation would rewind paper state time"
            )
        if (
            self.state.last_ticker_exchange_time is not None
            and ticker.exchange_timestamp
            < pd.Timestamp(self.state.last_ticker_exchange_time)
        ):
            raise TickerNotReadyError(
                "Ticker exchange trade time would rewind paper state"
            )

        events: list[dict[str, Any]] = []
        trades: list[Trade] = []
        initialized = self.state.last_bar_time is None
        processed_bar = initialized
        new_bar = False
        data_gap = False
        stale_signal = False
        model_not_ready = False

        if initialized:
            events.append(
                _paper_event(
                    "paper_initialized",
                    ticker.observed_at,
                    self.state.market,
                    {
                        "signal_bar": bar_time.isoformat(),
                        "message": (
                            "Account primed without a trade; future decisions use "
                            "observed ticker prices"
                        ),
                    },
                )
            )
        else:
            prior_bar = pd.Timestamp(self.state.last_bar_time)
            if bar_time < prior_bar:
                raise ValueError(
                    "Fetched market data is older than the persisted paper state"
                )
            delta = bar_time - prior_bar
            expected = pd.Timedelta(minutes=self.config.data.interval_minutes)
            new_bar = delta == expected
            data_gap = delta > expected
            processed_bar = new_bar or data_gap
            if data_gap:
                self._clear_pending_signal()
                self.state.halt_state = (
                    HALT_PENDING if self.state.quantity > 0 else HALTED
                )
                events.append(
                    _paper_event(
                        "market_data_gap",
                        ticker.observed_at,
                        self.state.market,
                        {
                            "previous_bar": prior_bar.isoformat(),
                            "latest_bar": bar_time.isoformat(),
                            "action": "halt_without_historical_replay",
                        },
                    )
                )
            elif new_bar:
                if not model_ready:
                    model_not_ready = True
                    self._clear_pending_signal()
                    self.state.halt_state = (
                        HALT_PENDING if self.state.quantity > 0 else HALTED
                    )
                    events.append(
                        _paper_event(
                            "model_not_ready",
                            ticker.observed_at,
                            self.state.market,
                            {
                                "signal_time": signal_time.isoformat(),
                                "action": "halt_without_new_entry",
                                "error_type": model_error,
                            },
                        )
                    )
                else:
                    execution_delay = (
                        ticker.observed_at - signal_time
                    ).total_seconds()
                    if (
                        execution_delay
                        > self.config.paper.max_signal_delay_seconds
                    ):
                        stale_signal = True
                        self._clear_pending_signal()
                        self.state.halt_state = (
                            HALT_PENDING
                            if self.state.quantity > 0
                            else HALTED
                        )
                        events.append(
                            _paper_event(
                                "stale_signal",
                                ticker.observed_at,
                                self.state.market,
                                {
                                    "signal_time": signal_time.isoformat(),
                                    "execution_delay_seconds": execution_delay,
                                    "maximum_seconds": (
                                        self.config.paper.max_signal_delay_seconds
                                    ),
                                },
                            )
                        )

        position_existed_at_snapshot = self.state.quantity > 0
        cooldown_at_snapshot_start = self.state.cooldown_remaining
        stop_triggered = False
        position_exited_this_snapshot = False
        if self.state.quantity > 0 and self.state.halt_state in (
            HALT_PENDING,
            HALTED,
        ):
            self._sell(
                ticker=ticker,
                reason="risk_halt",
                signal_time=self.state.pending_signal_time,
                model_id=self.state.pending_model_id,
                events=events,
                trades=trades,
            )
            self.state.halt_state = HALTED
            position_exited_this_snapshot = True
        elif (
            self.state.quantity > 0
            and self.config.model.signal_mode == "expected_return"
            and self.state.entry_horizon_exit_time is not None
            and ticker.observed_at
            >= pd.Timestamp(self.state.entry_horizon_exit_time)
        ):
            self._sell(
                ticker=ticker,
                reason="horizon_exit",
                signal_time=self.state.pending_signal_time,
                model_id=self.state.pending_model_id,
                events=events,
                trades=trades,
            )
            position_exited_this_snapshot = True
        elif self.state.quantity > 0:
            fixed_stop = self.state.entry_price * (
                1 - self.state.entry_stop_distance_pct
            )
            trailing_stop = self.state.peak_position_price * (
                1 - self.config.risk.trailing_stop_pct
            )
            stop_price = max(fixed_stop, trailing_stop)
            if ticker.price <= stop_price:
                self._sell(
                    ticker=ticker,
                    reason=(
                        "trailing_stop"
                        if trailing_stop >= fixed_stop
                        else "stop_loss"
                    ),
                    signal_time=self.state.pending_signal_time,
                    model_id=self.state.pending_model_id,
                    events=events,
                    trades=trades,
                )
                self.state.cooldown_remaining = self.config.risk.cooldown_bars
                stop_triggered = True
                position_exited_this_snapshot = True

        usable_probability = (
            float(probability)
            if probability is not None and np.isfinite(probability)
            else None
        )
        usable_atr = (
            float(atr_pct)
            if atr_pct is not None and np.isfinite(atr_pct)
            else None
        )
        usable_expected_gross_return = (
            float(expected_gross_return)
            if expected_gross_return is not None
            and np.isfinite(expected_gross_return)
            else None
        )
        usable_calibration_buffer = (
            float(calibration_buffer)
            if calibration_buffer is not None
            and np.isfinite(calibration_buffer)
            else None
        )
        usable_expected_net_edge = (
            float(expected_net_edge)
            if expected_net_edge is not None
            and np.isfinite(expected_net_edge)
            else None
        )
        usable_signal_eligible = bool(signal_eligible)
        usable_no_trade_reason = (
            str(no_trade_reason) if no_trade_reason is not None else None
        )
        should_trade_signal = (
            new_bar
            and not data_gap
            and not stale_signal
            and not model_not_ready
        )

        if (
            should_trade_signal
            and self.config.model.signal_mode == "probability"
            and self.state.quantity > 0
            and self.state.halt_state == HALT_ACTIVE
            and not position_exited_this_snapshot
            and usable_probability is not None
            and usable_probability <= self.config.model.exit_probability
        ):
            self._sell(
                ticker=ticker,
                reason="model_exit",
                signal_time=signal_time.isoformat(),
                model_id=model_id,
                events=events,
                trades=trades,
            )
            position_exited_this_snapshot = True

        probability_entry = (
            self.config.model.signal_mode == "probability"
            and usable_probability is not None
            and usable_probability >= self.config.model.entry_probability
        )
        expected_return_entry = (
            self.config.model.signal_mode == "expected_return"
            and usable_signal_eligible
            and usable_expected_net_edge is not None
            and usable_expected_net_edge
            >= self.config.model.minimum_edge_pct
        )
        if (
            should_trade_signal
            and self.state.quantity <= 0
            and self.state.halt_state == HALT_ACTIVE
            and cooldown_at_snapshot_start == 0
            and not stop_triggered
            and not position_exited_this_snapshot
            and (probability_entry or expected_return_entry)
        ):
            decision = self.risk.size_entry(
                equity=self.state.cash,
                cash=self.state.cash,
                raw_price=ticker.price,
                atr_pct=usable_atr,
            )
            if decision.accepted:
                fill = self.broker.buy(
                    timestamp=ticker.observed_at,
                    raw_price=ticker.price,
                    requested_notional=decision.notional,
                    stop_distance_pct=decision.stop_distance_pct,
                    reason="model_entry",
                    signal_time=signal_time.isoformat(),
                    model_id=model_id,
                )
                events.append(_fill_event(fill))
                if self.config.model.signal_mode == "expected_return":
                    self.state.entry_horizon_exit_time = (
                        signal_time
                        + pd.Timedelta(
                            minutes=(
                                self.config.model.horizon_bars
                                * self.config.data.interval_minutes
                            )
                        )
                    ).isoformat()
            else:
                events.append(
                    _paper_event(
                        "entry_rejected",
                        ticker.observed_at,
                        self.state.market,
                        {"reason": decision.reason},
                    )
                )

        if self.state.quantity > 0:
            self.state.peak_position_price = max(
                self.state.peak_position_price, ticker.price
            )

        equity = self._liquidation_equity(ticker.price)
        self.state.peak_equity = max(self.state.peak_equity, equity)
        drawdown = self.risk.drawdown(equity, self.state.peak_equity)
        if (
            self.state.halt_state == HALT_ACTIVE
            and drawdown + 1e-12
            >= self.config.risk.max_strategy_drawdown_pct
        ):
            self.state.halt_state = (
                HALT_PENDING if self.state.quantity > 0 else HALTED
            )
            events.append(
                _paper_event(
                    "drawdown_halt",
                    ticker.observed_at,
                    self.state.market,
                    {
                        "drawdown": drawdown,
                        "threshold": self.config.risk.max_strategy_drawdown_pct,
                    },
                )
            )

        if (
            new_bar
            and cooldown_at_snapshot_start > 0
            and not stop_triggered
        ):
            self.state.cooldown_remaining = max(
                0, cooldown_at_snapshot_start - 1
            )

        if initialized or new_bar:
            self.state.pending_probability = (
                usable_probability
                if self.config.model.signal_mode == "probability"
                and self.state.halt_state == HALT_ACTIVE
                else None
            )
            self.state.pending_expected_gross_return = (
                usable_expected_gross_return
                if self.config.model.signal_mode == "expected_return"
                and self.state.halt_state == HALT_ACTIVE
                else None
            )
            self.state.pending_calibration_buffer = (
                usable_calibration_buffer
                if self.config.model.signal_mode == "expected_return"
                and self.state.halt_state == HALT_ACTIVE
                else None
            )
            self.state.pending_expected_net_edge = (
                usable_expected_net_edge
                if self.config.model.signal_mode == "expected_return"
                and self.state.halt_state == HALT_ACTIVE
                else None
            )
            self.state.pending_signal_eligible = (
                usable_signal_eligible
                if self.config.model.signal_mode == "expected_return"
                and self.state.halt_state == HALT_ACTIVE
                else False
            )
            self.state.pending_no_trade_reason = (
                usable_no_trade_reason
                if self.config.model.signal_mode == "expected_return"
                and self.state.halt_state == HALT_ACTIVE
                else None
            )
            self.state.pending_atr_pct = (
                usable_atr if self.state.halt_state == HALT_ACTIVE else None
            )
            signal_value_present = (
                usable_probability is not None
                if self.config.model.signal_mode == "probability"
                else usable_expected_net_edge is not None
            )
            self.state.pending_signal_time = (
                signal_time.isoformat()
                if signal_value_present
                and self.state.halt_state == HALT_ACTIVE
                else None
            )
            self.state.pending_model_id = (
                model_id
                if signal_value_present
                and self.state.halt_state == HALT_ACTIVE
                else None
            )
        if self.state.halt_state != HALT_ACTIVE:
            self._clear_pending_signal()
        if initialized or new_bar or data_gap:
            self.state.last_bar_time = bar_time.isoformat()
        self.state.last_ticker_exchange_time = (
            ticker.exchange_timestamp.isoformat()
        )
        self.state.updated_at = ticker.observed_at.isoformat()

        # A position opened at this snapshot is never tested against historical
        # candle lows; only subsequent observed tickers can stop it.
        if not position_existed_at_snapshot and self.state.quantity > 0:
            self.state.peak_position_price = max(
                self.state.entry_price, ticker.price
            )

        return PaperSnapshotResult(
            processed_bar=processed_bar,
            equity=equity,
            events=tuple(events),
            trades=tuple(trades),
        )


def run_paper_once(
    config: AppConfig,
    *,
    store: SQLiteStore,
    client: UpbitCandleClient,
) -> PaperRunSummary:
    account_key = paper_account_key(config)
    fingerprint = paper_config_fingerprint(config)
    raw_state = store.load_paper_state(account_key)
    initialized = raw_state is None
    state = (
        PortfolioState.initial(
            config.data.market,
            config.risk.initial_cash,
            config_fingerprint=fingerprint,
        )
        if raw_state is None
        else PortfolioState.from_dict(raw_state)
    )
    if initialized:
        state.schema_version = PAPER_SCHEMA_VERSION
    if state.market != config.data.market:
        raise ValueError("Stored paper account market does not match configuration")
    if state.schema_version != PAPER_SCHEMA_VERSION:
        raise ValueError("Unsupported portfolio state schema version")
    if state.config_fingerprint != fingerprint:
        raise ValueError(
            "Paper configuration changed; use a new account_name or explicit migration"
        )

    # Risk monitoring for an existing position must not depend on candle sync or
    # model availability. Evaluate a fresh ticker against persisted stops first.
    if not initialized and state.quantity > 0:
        if state.last_bar_time is None:
            raise ValueError("Open paper position has no persisted signal bar")
        risk_ticker = client.fetch_ticker(market=config.data.market)
        risk_bar = pd.Series({"timestamp": pd.Timestamp(state.last_bar_time)})
        risk_result = PaperSnapshotEngine(state, config).process_snapshot(
            latest_closed_bar=risk_bar,
            probability=state.pending_probability,
            atr_pct=state.pending_atr_pct,
            model_id=state.pending_model_id,
            ticker=risk_ticker,
            expected_gross_return=state.pending_expected_gross_return,
            calibration_buffer=state.pending_calibration_buffer,
            expected_net_edge=state.pending_expected_net_edge,
            signal_eligible=state.pending_signal_eligible,
            no_trade_reason=state.pending_no_trade_reason,
        )
        if state.quantity > 0 and state.halt_state == HALT_PENDING:
            followup = PaperSnapshotEngine(state, config).process_snapshot(
                latest_closed_bar=risk_bar,
                probability=None,
                atr_pct=None,
                model_id=state.pending_model_id,
                ticker=risk_ticker,
                expected_gross_return=None,
                calibration_buffer=None,
                expected_net_edge=None,
                signal_eligible=False,
                no_trade_reason=None,
            )
            risk_result = PaperSnapshotResult(
                processed_bar=False,
                equity=followup.equity,
                events=risk_result.events + followup.events,
                trades=risk_result.trades + followup.trades,
            )
        expected_revision = state.revision
        state.revision = store.save_paper_step(
            account_key,
            state.to_dict(),
            risk_result.events,
            expected_revision=expected_revision,
        )
        if risk_result.events or state.quantity <= 0:
            return _paper_run_summary(
                account_key=account_key,
                initialized=False,
                state=state,
                ticker=risk_ticker,
                result=risk_result,
            )

    local_count = store.candle_count(
        config.data.market, config.data.interval_minutes
    )
    fetch_count = (
        config.paper.history_candles + 1
        if initialized or local_count < config.paper.history_candles
        else 200
    )
    fetched = client.fetch_candles(
        market=config.data.market,
        interval_minutes=config.data.interval_minutes,
        count=fetch_count,
    )
    finalized = closed_candles(
        fetched,
        config.data.interval_minutes,
        now=fetched.attrs.get("finalization_cutoff"),
    )
    store.upsert_candles(finalized, config.data.interval_minutes)
    history = (
        finalized.tail(config.paper.history_candles).reset_index(drop=True)
        if initialized
        else store.load_candles(
            config.data.market,
            config.data.interval_minutes,
            limit=config.paper.history_candles,
        )
    )
    minimum_history = config.minimum_history_bars
    if history.empty:
        raise ValueError("Paper mode has no finalized candles")
    expected_delta = pd.Timedelta(minutes=config.data.interval_minutes)
    discontinuities = history["timestamp"].diff().ne(expected_delta)
    discontinuities.iloc[0] = False
    latest_segment_start = 0
    if discontinuities.any():
        last_gap_index = int(discontinuities[discontinuities].index[-1])
        latest_segment_start = last_gap_index
    latest_segment_bars = len(history) - latest_segment_start
    gap_count = int(discontinuities.sum())
    feature_warmup = config.feature_warmup_bars
    gap_adjusted_minimum = minimum_history + gap_count * (
        feature_warmup + config.model.horizon_bars + 1
    )
    history_ready = (
        len(history) >= gap_adjusted_minimum
        and latest_segment_bars >= feature_warmup + 1
    )

    latest_bar = history.iloc[-1]
    latest_bar_time = pd.Timestamp(latest_bar["timestamp"])
    needs_prediction = initialized
    if state.last_bar_time is not None:
        delta = latest_bar_time - pd.Timestamp(state.last_bar_time)
        needs_prediction = delta == pd.Timedelta(
            minutes=config.data.interval_minutes
        )
    if initialized and not history_ready:
        raise ValueError(
            "Paper mode needs enough gap-adjusted finalized history "
            f"(have {len(history)}, need {gap_adjusted_minimum}; "
            f"latest segment {latest_segment_bars}, "
            f"need {feature_warmup + 1})"
        )

    model_ready = True
    model_error: str | None = None
    if needs_prediction:
        if not history_ready:
            probability = None
            expected_gross_return = None
            calibration_buffer = None
            expected_net_edge = None
            signal_eligible = False
            no_trade_reason = None
            atr = None
            model_id = None
            model_ready = False
        else:
            try:
                predictions = generate_walk_forward_predictions(
                    history,
                    interval_minutes=config.data.interval_minutes,
                    model_config=config.model,
                    round_trip_cost=config.round_trip_cost,
                )
                latest_index = len(history) - 1
                probability = predictions.probabilities.iloc[latest_index]
                expected_gross_return = (
                    predictions.expected_gross_returns.iloc[latest_index]
                )
                calibration_buffer = (
                    predictions.calibration_buffers.iloc[latest_index]
                )
                expected_net_edge = (
                    predictions.expected_net_edges.iloc[latest_index]
                )
                signal_eligible = (
                    predictions.signal_eligible.iloc[latest_index]
                )
                no_trade_reason = (
                    predictions.no_trade_reasons.iloc[latest_index]
                )
                atr = predictions.dataset.frame.at[latest_index, "atr_pct"]
                model_id = predictions.model_ids.iloc[latest_index]
                if config.model.signal_mode == "expected_return":
                    model_ready = (
                        pd.notna(expected_net_edge)
                        and np.isfinite(float(expected_net_edge))
                        and pd.notna(model_id)
                    )
                else:
                    model_ready = (
                        pd.notna(probability) and pd.notna(model_id)
                    )
            except Exception as exc:
                if initialized:
                    raise ValueError(
                        "Paper model failed during account initialization"
                    ) from exc
                probability = None
                expected_gross_return = None
                calibration_buffer = None
                expected_net_edge = None
                signal_eligible = False
                no_trade_reason = None
                atr = None
                model_id = None
                model_ready = False
                model_error = type(exc).__name__
        if initialized and not model_ready:
            raise ValueError(
                "Paper model is not ready for the latest finalized candle"
            )
    else:
        probability = state.pending_probability
        expected_gross_return = state.pending_expected_gross_return
        calibration_buffer = state.pending_calibration_buffer
        expected_net_edge = state.pending_expected_net_edge
        signal_eligible = state.pending_signal_eligible
        no_trade_reason = state.pending_no_trade_reason
        atr = state.pending_atr_pct
        model_id = state.pending_model_id
    ticker = client.fetch_ticker(market=config.data.market)
    engine = PaperSnapshotEngine(state, config)
    result = engine.process_snapshot(
        latest_closed_bar=latest_bar,
        probability=float(probability) if pd.notna(probability) else None,
        atr_pct=float(atr) if pd.notna(atr) else None,
        model_id=str(model_id) if pd.notna(model_id) else None,
        ticker=ticker,
        expected_gross_return=(
            float(expected_gross_return)
            if pd.notna(expected_gross_return)
            else None
        ),
        calibration_buffer=(
            float(calibration_buffer)
            if pd.notna(calibration_buffer)
            else None
        ),
        expected_net_edge=(
            float(expected_net_edge)
            if pd.notna(expected_net_edge)
            else None
        ),
        signal_eligible=(
            bool(signal_eligible) if pd.notna(signal_eligible) else False
        ),
        no_trade_reason=(
            str(no_trade_reason) if pd.notna(no_trade_reason) else None
        ),
        model_ready=model_ready,
        model_error=model_error,
    )
    expected_revision = state.revision
    next_revision = store.save_paper_step(
        account_key,
        state.to_dict(),
        result.events,
        expected_revision=expected_revision,
    )
    state.revision = next_revision

    return _paper_run_summary(
        account_key=account_key,
        initialized=initialized,
        state=state,
        ticker=ticker,
        result=result,
    )
