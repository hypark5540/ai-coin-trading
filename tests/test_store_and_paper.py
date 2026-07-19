from __future__ import annotations

import dataclasses

import pandas as pd
import pytest

from coinpilot.broker import PortfolioState, SimulatedBroker
from coinpilot.config import AppConfig, DataConfig, ModelConfig, PaperConfig
from coinpilot.data import CANDLE_COLUMNS, MarketTicker, synthetic_candles
from coinpilot.paper import (
    PAPER_SCHEMA_VERSION,
    PaperSnapshotEngine,
    paper_account_key,
    paper_config_fingerprint,
    run_paper_once,
)
from coinpilot.store import ConcurrentPaperUpdate, SQLiteStore


class _SequenceClient:
    def __init__(self, frames):
        self.frames = list(frames)
        self.index = 0
        self.current = None

    def fetch_candles(self, **_):
        frame = self.frames[min(self.index, len(self.frames) - 1)]
        self.index += 1
        self.current = frame
        return frame.copy()

    def fetch_ticker(self, *, market):
        frame = self.current if self.current is not None else self.frames[
            min(self.index, len(self.frames) - 1)
        ]
        last = frame.iloc[-1]
        exchange_timestamp = pd.Timestamp(last["timestamp"]) + pd.Timedelta(hours=1)
        return MarketTicker(
            market=market,
            price=float(last["close"]),
            exchange_timestamp=exchange_timestamp,
            observed_at=exchange_timestamp + pd.Timedelta(seconds=10),
        )


def _paper_config(database_path: str) -> AppConfig:
    base = AppConfig()
    return dataclasses.replace(
        base,
        data=DataConfig(candle_count=280, database_path=database_path),
        model=ModelConfig(
            horizon_bars=3,
            min_train_samples=100,
            train_window=160,
            retrain_every=24,
            max_iterations=80,
        ),
        paper=PaperConfig(history_candles=280, poll_seconds=1),
    ).validate()


def _expected_return_paper_config(database_path: str) -> AppConfig:
    base = _paper_config(database_path)
    return dataclasses.replace(
        base,
        data=dataclasses.replace(base.data, candle_count=400),
        model=dataclasses.replace(
            base.model,
            signal_mode="expected_return",
            horizon_bars=2,
            train_window=200,
            calibration_window=60,
            calibration_min_samples=30,
        ),
        paper=dataclasses.replace(base.paper, history_candles=400),
    ).validate()


def _ticker_at(hour: int, *, price: float = 100_000_000.0) -> MarketTicker:
    boundary = pd.Timestamp(f"2026-01-01T{hour:02d}:00:00Z")
    return MarketTicker(
        market="KRW-BTC",
        price=price,
        exchange_timestamp=boundary + pd.Timedelta(seconds=5),
        observed_at=boundary + pd.Timedelta(seconds=6),
    )


def test_paper_restart_processes_each_bar_once(tmp_path) -> None:
    all_candles = synthetic_candles(281)
    first_batch = all_candles.iloc[:280].reset_index(drop=True)
    second_batch = all_candles.copy()
    config = _paper_config(str(tmp_path / "paper.db"))
    store = SQLiteStore(config.data.database_path)
    client = _SequenceClient([first_batch, second_batch, second_batch])

    first = run_paper_once(config, store=store, client=client)
    second = run_paper_once(config, store=store, client=client)
    third = run_paper_once(config, store=store, client=client)

    assert first.initialized is True
    assert first.processed_bars == 1
    assert second.processed_bars == 1
    assert third.processed_bars == 0
    state = store.load_paper_state(paper_account_key(config))
    assert state is not None
    assert state["last_bar_time"] == all_candles.iloc[-1]["timestamp"].isoformat()
    initialized_events = [
        event
        for event in store.recent_paper_events(
            paper_account_key(config), limit=100
        )
        if event["event_type"] == "paper_initialized"
    ]
    assert len(initialized_events) == 1


def test_expected_return_paper_primes_then_enters_on_next_snapshot(
    tmp_path,
) -> None:
    config = _expected_return_paper_config(str(tmp_path / "paper.db"))
    state = PortfolioState.initial(
        "KRW-BTC",
        config.risk.initial_cash,
        config_fingerprint=paper_config_fingerprint(config),
    )
    engine = PaperSnapshotEngine(state, config)
    latest = synthetic_candles(100).iloc[-1].copy()
    latest["timestamp"] = pd.Timestamp("2026-01-01T00:00:00Z")

    initialized = engine.process_snapshot(
        latest_closed_bar=latest,
        probability=None,
        atr_pct=0.01,
        model_id="return-model",
        ticker=_ticker_at(1),
        expected_gross_return=0.02,
        calibration_buffer=0.002,
        expected_net_edge=0.01,
        signal_eligible=True,
        no_trade_reason="eligible",
    )

    assert not [
        event for event in initialized.events if event["event_type"] == "fill"
    ]
    assert state.pending_expected_net_edge == pytest.approx(0.01)
    assert state.pending_signal_eligible is True

    latest["timestamp"] = pd.Timestamp("2026-01-01T01:00:00Z")
    entered = engine.process_snapshot(
        latest_closed_bar=latest,
        probability=None,
        atr_pct=0.01,
        model_id="return-model",
        ticker=_ticker_at(2),
        expected_gross_return=0.02,
        calibration_buffer=0.002,
        expected_net_edge=0.01,
        signal_eligible=True,
        no_trade_reason="eligible",
    )

    fills = [
        event["payload"]
        for event in entered.events
        if event["event_type"] == "fill"
    ]
    assert [fill["side"] for fill in fills] == ["buy"]
    assert state.quantity > 0
    assert state.entry_horizon_exit_time == "2026-01-01T04:00:00+00:00"


def test_expected_return_paper_exits_at_horizon_without_same_snapshot_reentry(
    tmp_path,
) -> None:
    config = _expected_return_paper_config(str(tmp_path / "paper.db"))
    state = PortfolioState.initial(
        "KRW-BTC",
        config.risk.initial_cash,
        config_fingerprint=paper_config_fingerprint(config),
    )
    state.last_bar_time = "2026-01-01T00:00:00+00:00"
    engine = PaperSnapshotEngine(state, config)
    latest = synthetic_candles(100).iloc[-1].copy()

    latest["timestamp"] = pd.Timestamp("2026-01-01T01:00:00Z")
    engine.process_snapshot(
        latest_closed_bar=latest,
        probability=None,
        atr_pct=0.01,
        model_id="return-model",
        ticker=_ticker_at(2),
        expected_gross_return=0.02,
        calibration_buffer=0.002,
        expected_net_edge=0.01,
        signal_eligible=True,
        no_trade_reason="eligible",
    )
    assert state.entry_horizon_exit_time == "2026-01-01T04:00:00+00:00"

    latest["timestamp"] = pd.Timestamp("2026-01-01T02:00:00Z")
    before_horizon = engine.process_snapshot(
        latest_closed_bar=latest,
        probability=None,
        atr_pct=0.01,
        model_id="return-model",
        ticker=_ticker_at(3),
        expected_gross_return=0.02,
        calibration_buffer=0.002,
        expected_net_edge=0.01,
        signal_eligible=True,
        no_trade_reason="eligible",
    )
    assert not [
        event
        for event in before_horizon.events
        if event["event_type"] == "fill"
    ]
    assert state.quantity > 0

    latest["timestamp"] = pd.Timestamp("2026-01-01T03:00:00Z")
    at_horizon = engine.process_snapshot(
        latest_closed_bar=latest,
        probability=None,
        atr_pct=0.01,
        model_id="return-model",
        ticker=_ticker_at(4),
        expected_gross_return=0.02,
        calibration_buffer=0.002,
        expected_net_edge=0.01,
        signal_eligible=True,
        no_trade_reason="eligible",
    )

    fills = [
        event["payload"]
        for event in at_horizon.events
        if event["event_type"] == "fill"
    ]
    assert [fill["side"] for fill in fills] == ["sell"]
    assert fills[0]["reason"] == "horizon_exit"
    assert [trade.exit_reason for trade in at_horizon.trades] == [
        "horizon_exit"
    ]
    assert state.quantity == 0
    assert state.entry_horizon_exit_time is None


def test_paper_fill_uses_observed_ticker_not_historical_open(tmp_path) -> None:
    config = _paper_config(str(tmp_path / "paper.db"))
    state = PortfolioState.initial(
        "KRW-BTC",
        config.risk.initial_cash,
        config_fingerprint=paper_config_fingerprint(config),
    )
    state.last_bar_time = "2026-01-01T00:00:00+00:00"
    engine = PaperSnapshotEngine(state, config)
    latest = synthetic_candles(100).iloc[-1].copy()
    latest["timestamp"] = pd.Timestamp("2026-01-01T01:00:00Z")
    latest["open"] = 80_000_000.0
    ticker = MarketTicker(
        market="KRW-BTC",
        price=100_000_000.0,
        exchange_timestamp=pd.Timestamp("2026-01-01T02:00:06Z"),
        observed_at=pd.Timestamp("2026-01-01T02:00:07Z"),
    )

    result = engine.process_snapshot(
        latest_closed_bar=latest,
        probability=0.99,
        atr_pct=0.01,
        model_id="model",
        ticker=ticker,
    )
    fills = [
        event["payload"]
        for event in result.events
        if event["event_type"] == "fill"
    ]
    assert len(fills) == 1
    assert fills[0]["raw_price"] == ticker.price
    assert fills[0]["timestamp"] == ticker.observed_at.isoformat()
    assert fills[0]["raw_price"] != latest["open"]


def test_paper_downtime_halts_without_replaying_old_bars(tmp_path) -> None:
    config = _paper_config(str(tmp_path / "paper.db"))
    state = PortfolioState.initial(
        "KRW-BTC",
        config.risk.initial_cash,
        config_fingerprint=paper_config_fingerprint(config),
    )
    state.last_bar_time = "2026-01-01T00:00:00+00:00"
    engine = PaperSnapshotEngine(state, config)
    latest = synthetic_candles(100).iloc[-1].copy()
    latest["timestamp"] = pd.Timestamp("2026-01-01T03:00:00Z")
    ticker = MarketTicker(
        market="KRW-BTC",
        price=100_000_000,
        exchange_timestamp=pd.Timestamp("2026-01-01T04:00:09Z"),
        observed_at=pd.Timestamp("2026-01-01T04:00:10Z"),
    )
    result = engine.process_snapshot(
        latest_closed_bar=latest,
        probability=0.99,
        atr_pct=0.01,
        model_id="model",
        ticker=ticker,
    )
    assert state.halt_state == "HALTED"
    assert not [event for event in result.events if event["event_type"] == "fill"]
    assert any(
        event["event_type"] == "market_data_gap" for event in result.events
    )


def test_paper_rejects_a_late_ticker_instead_of_delayed_fill(tmp_path) -> None:
    config = _paper_config(str(tmp_path / "paper.db"))
    state = PortfolioState.initial(
        "KRW-BTC",
        config.risk.initial_cash,
        config_fingerprint=paper_config_fingerprint(config),
    )
    state.last_bar_time = "2026-01-01T00:00:00+00:00"
    engine = PaperSnapshotEngine(state, config)
    latest = synthetic_candles(100).iloc[-1].copy()
    latest["timestamp"] = pd.Timestamp("2026-01-01T01:00:00Z")
    ticker = MarketTicker(
        market="KRW-BTC",
        price=100_000_000,
        exchange_timestamp=pd.Timestamp("2026-01-01T02:02:00Z"),
        observed_at=pd.Timestamp("2026-01-01T02:02:01Z"),
    )
    result = engine.process_snapshot(
        latest_closed_bar=latest,
        probability=0.99,
        atr_pct=0.01,
        model_id="model",
        ticker=ticker,
    )
    assert state.halt_state == "HALTED"
    assert not [event for event in result.events if event["event_type"] == "fill"]
    assert any(event["event_type"] == "stale_signal" for event in result.events)


def test_paper_state_save_uses_revision_compare_and_swap(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "paper.db")
    state = PortfolioState.initial("KRW-BTC", 1000).to_dict()
    revision = store.save_paper_step(
        "account", state, [], expected_revision=0
    )
    assert revision == 1
    with pytest.raises(ConcurrentPaperUpdate):
        store.save_paper_step("account", state, [], expected_revision=0)


def test_paper_account_lock_rejects_a_concurrent_runner(tmp_path) -> None:
    first = SQLiteStore(tmp_path / "paper.db")
    second = SQLiteStore(tmp_path / "paper.db")

    with first.paper_account_lock("account"):
        with pytest.raises(ConcurrentPaperUpdate, match="already running"):
            with second.paper_account_lock("account"):
                pass

    with second.paper_account_lock("account"):
        pass


def test_sqlite_database_and_sidecars_are_owner_only(tmp_path) -> None:
    path = tmp_path / "paper.db"
    store = SQLiteStore(path)
    state = PortfolioState.initial("KRW-BTC", 1000).to_dict()
    store.save_paper_step("account", state, [], expected_revision=0)

    database_files = [
        candidate
        for candidate in (
            path,
            tmp_path / "paper.db-wal",
            tmp_path / "paper.db-shm",
        )
        if candidate.exists()
    ]
    assert database_files
    assert all(
        candidate.stat().st_mode & 0o077 == 0
        for candidate in database_files
    )


def test_candle_bounds_reports_inclusive_utc_extents(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "candles.db")
    candles = synthetic_candles(100)

    assert store.candle_bounds("KRW-BTC", 60) is None

    store.upsert_candles(candles, 60)

    assert store.candle_bounds("KRW-BTC", 60) == (
        candles.iloc[0]["timestamp"],
        candles.iloc[-1]["timestamp"],
    )
    assert store.candle_bounds("KRW-BTC", 30) is None


def test_load_candles_range_uses_half_open_chronological_bounds(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "candles.db")
    candles = synthetic_candles(100)
    store.upsert_candles(candles, 60)
    split_start = candles.iloc[20]["timestamp"]
    split_end = candles.iloc[40]["timestamp"]

    selected = store.load_candles_range(
        "KRW-BTC",
        60,
        start=split_start,
        end=split_end,
    )

    pd.testing.assert_frame_equal(
        selected,
        candles.iloc[20:40].reset_index(drop=True),
    )
    assert split_start in set(selected["timestamp"])
    assert split_end not in set(selected["timestamp"])
    assert selected["timestamp"].is_monotonic_increasing


def test_load_candles_range_normalizes_boundaries_and_supports_open_ends(
    tmp_path,
) -> None:
    store = SQLiteStore(tmp_path / "candles.db")
    candles = synthetic_candles(100)
    store.upsert_candles(candles, 60)
    boundary = candles.iloc[50]["timestamp"]
    boundary_in_seoul = boundary.tz_convert("Asia/Seoul")

    before = store.load_candles_range(
        "KRW-BTC", 60, end=boundary_in_seoul
    )
    after = store.load_candles_range(
        "KRW-BTC", 60, start=boundary.tz_localize(None)
    )

    pd.testing.assert_frame_equal(
        before,
        candles.iloc[:50].reset_index(drop=True),
    )
    pd.testing.assert_frame_equal(
        after,
        candles.iloc[50:].reset_index(drop=True),
    )


def test_load_candles_range_handles_empty_and_reversed_ranges(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "candles.db")
    candles = synthetic_candles(100)
    store.upsert_candles(candles, 60)
    boundary = candles.iloc[50]["timestamp"]

    empty = store.load_candles_range(
        "KRW-BTC",
        60,
        start=boundary,
        end=boundary,
    )

    assert empty.empty
    assert tuple(empty.columns) == CANDLE_COLUMNS
    with pytest.raises(ValueError, match="start must not be after end"):
        store.load_candles_range(
            "KRW-BTC",
            60,
            start=boundary + pd.Timedelta(hours=1),
            end=boundary,
        )


def test_event_ids_are_scoped_to_paper_account(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "paper.db")
    event = {
        "event_id": "same",
        "timestamp": "2026-01-01T00:00:00+00:00",
        "event_type": "decision",
        "payload": {"value": 1},
    }
    state = PortfolioState.initial("KRW-BTC", 1000).to_dict()
    store.save_paper_step(
        "account-a", state, [event], expected_revision=0
    )
    store.save_paper_step(
        "account-b", state, [event], expected_revision=0
    )
    assert len(store.recent_paper_events("account-a")) == 1
    assert len(store.recent_paper_events("account-b")) == 1


def test_paper_rejects_ticker_that_would_rewind_state(tmp_path) -> None:
    config = _paper_config(str(tmp_path / "paper.db"))
    state = PortfolioState.initial(
        "KRW-BTC",
        config.risk.initial_cash,
        config_fingerprint=paper_config_fingerprint(config),
    )
    state.last_bar_time = "2026-01-01T00:00:00+00:00"
    state.updated_at = "2026-01-01T02:00:10+00:00"
    before = state.to_dict()
    latest = synthetic_candles(100).iloc[-1].copy()
    latest["timestamp"] = pd.Timestamp("2026-01-01T01:00:00Z")
    ticker = MarketTicker(
        market="KRW-BTC",
        price=100_000_000,
        exchange_timestamp=pd.Timestamp("2026-01-01T02:00:04Z"),
        observed_at=pd.Timestamp("2026-01-01T02:00:05Z"),
    )

    with pytest.raises(ValueError, match="rewind"):
        PaperSnapshotEngine(state, config).process_snapshot(
            latest_closed_bar=latest,
            probability=0.99,
            atr_pct=0.01,
            model_id="model",
            ticker=ticker,
        )

    assert state.to_dict() == before


def test_paper_rejects_exchange_trade_time_rewind(tmp_path) -> None:
    config = _paper_config(str(tmp_path / "paper.db"))
    state = PortfolioState.initial(
        "KRW-BTC",
        config.risk.initial_cash,
        config_fingerprint=paper_config_fingerprint(config),
    )
    state.last_bar_time = "2026-01-01T01:00:00+00:00"
    state.updated_at = "2026-01-01T02:00:51+00:00"
    state.last_ticker_exchange_time = "2026-01-01T02:00:50+00:00"
    before = state.to_dict()
    latest = synthetic_candles(100).iloc[-1].copy()
    latest["timestamp"] = pd.Timestamp("2026-01-01T01:00:00Z")
    ticker = MarketTicker(
        market="KRW-BTC",
        price=98_000_000,
        exchange_timestamp=pd.Timestamp("2026-01-01T02:00:10Z"),
        observed_at=pd.Timestamp("2026-01-01T02:00:52Z"),
    )

    with pytest.raises(ValueError, match="exchange trade time"):
        PaperSnapshotEngine(state, config).process_snapshot(
            latest_closed_bar=latest,
            probability=0.99,
            atr_pct=0.01,
            model_id="model",
            ticker=ticker,
        )

    assert state.to_dict() == before


def test_paper_rejects_pre_close_exchange_tick_despite_fast_local_clock(
    tmp_path,
) -> None:
    config = _paper_config(str(tmp_path / "paper.db"))
    state = PortfolioState.initial(
        "KRW-BTC",
        config.risk.initial_cash,
        config_fingerprint=paper_config_fingerprint(config),
    )
    state.last_bar_time = "2026-01-01T00:00:00+00:00"
    before = state.to_dict()
    latest = synthetic_candles(100).iloc[-1].copy()
    latest["timestamp"] = pd.Timestamp("2026-01-01T01:00:00Z")
    ticker = MarketTicker(
        market="KRW-BTC",
        price=100_000_000,
        exchange_timestamp=pd.Timestamp("2026-01-01T01:59:57Z"),
        observed_at=pd.Timestamp("2026-01-01T02:00:07Z"),
    )

    with pytest.raises(ValueError, match="exchange trade"):
        PaperSnapshotEngine(state, config).process_snapshot(
            latest_closed_bar=latest,
            probability=0.99,
            atr_pct=0.01,
            model_id="model",
            ticker=ticker,
        )

    assert state.to_dict() == before


def test_paper_rejects_stale_exchange_ticker(tmp_path) -> None:
    config = _paper_config(str(tmp_path / "paper.db"))
    state = PortfolioState.initial(
        "KRW-BTC",
        config.risk.initial_cash,
        config_fingerprint=paper_config_fingerprint(config),
    )
    state.last_bar_time = "2026-01-01T00:00:00+00:00"
    latest = synthetic_candles(100).iloc[-1].copy()
    latest["timestamp"] = pd.Timestamp("2026-01-01T01:00:00Z")
    ticker = MarketTicker(
        market="KRW-BTC",
        price=100_000_000,
        exchange_timestamp=pd.Timestamp("2026-01-01T02:00:01Z"),
        observed_at=pd.Timestamp("2026-01-01T02:02:00Z"),
    )

    with pytest.raises(ValueError, match="stale"):
        PaperSnapshotEngine(state, config).process_snapshot(
            latest_closed_bar=latest,
            probability=0.99,
            atr_pct=0.01,
            model_id="model",
            ticker=ticker,
        )


def test_paper_stop_starts_full_cooldown_and_does_not_reenter(tmp_path) -> None:
    config = _paper_config(str(tmp_path / "paper.db"))
    state = PortfolioState.initial(
        "KRW-BTC",
        config.risk.initial_cash,
        config_fingerprint=paper_config_fingerprint(config),
    )
    state.last_bar_time = "2026-01-01T00:00:00+00:00"
    engine = PaperSnapshotEngine(state, config)
    engine.broker.buy(
        timestamp=pd.Timestamp("2026-01-01T01:00:00Z"),
        raw_price=100_000_000,
        requested_notional=1_000_000,
        stop_distance_pct=0.01,
        reason="fixture",
        signal_time="2026-01-01T01:00:00+00:00",
        model_id="fixture",
    )
    latest = synthetic_candles(100).iloc[-1].copy()
    latest["timestamp"] = pd.Timestamp("2026-01-01T01:00:00Z")
    ticker = MarketTicker(
        market="KRW-BTC",
        price=98_000_000,
        exchange_timestamp=pd.Timestamp("2026-01-01T02:00:05Z"),
        observed_at=pd.Timestamp("2026-01-01T02:00:06Z"),
    )

    result = engine.process_snapshot(
        latest_closed_bar=latest,
        probability=0.99,
        atr_pct=0.01,
        model_id="model",
        ticker=ticker,
    )

    fills = [
        event["payload"]
        for event in result.events
        if event["event_type"] == "fill"
    ]
    assert [fill["side"] for fill in fills] == ["sell"]
    assert state.quantity == 0
    assert state.cooldown_remaining == config.risk.cooldown_bars


def test_new_paper_account_rejects_short_latest_contiguous_segment(
    tmp_path,
) -> None:
    config = _paper_config(str(tmp_path / "paper.db"))
    candles = synthetic_candles(281).drop(index=100).reset_index(drop=True)
    store = SQLiteStore(config.data.database_path)
    client = _SequenceClient([candles])

    with pytest.raises(ValueError, match="gap-adjusted"):
        run_paper_once(config, store=store, client=client)

    assert store.load_paper_state(paper_account_key(config)) is None


def test_existing_position_is_liquidated_when_cache_reveals_a_gap(
    tmp_path,
) -> None:
    config = _paper_config(str(tmp_path / "paper.db"))
    candles = synthetic_candles(281).drop(index=100).reset_index(drop=True)
    state = PortfolioState.initial(
        "KRW-BTC",
        config.risk.initial_cash,
        config_fingerprint=paper_config_fingerprint(config),
    )
    state.schema_version = PAPER_SCHEMA_VERSION
    state.last_bar_time = candles.iloc[99]["timestamp"].isoformat()
    state.updated_at = (
        pd.Timestamp(candles.iloc[99]["timestamp"]) + pd.Timedelta(hours=1)
    ).isoformat()
    SimulatedBroker(state, config.risk).buy(
        timestamp=pd.Timestamp(state.updated_at),
        raw_price=float(candles.iloc[-1]["close"]),
        requested_notional=1_000_000,
        stop_distance_pct=0.08,
        reason="fixture",
        signal_time=state.updated_at,
        model_id="fixture",
    )
    store = SQLiteStore(config.data.database_path)
    account_key = paper_account_key(config)
    store.save_paper_step(
        account_key,
        state.to_dict(),
        [],
        expected_revision=0,
    )
    client = _SequenceClient([candles])

    summary = run_paper_once(config, store=store, client=client)
    persisted = store.load_paper_state(account_key)
    events = store.recent_paper_events(account_key, limit=10)

    assert summary.halt_state == "HALTED"
    assert persisted["quantity"] == 0
    assert any(event["event_type"] == "market_data_gap" for event in events)
    assert any(
        event["event_type"] == "fill"
        and event["payload"]["side"] == "sell"
        for event in events
    )


def test_existing_stop_is_persisted_before_candle_sync_failure(
    tmp_path,
) -> None:
    config = _paper_config(str(tmp_path / "paper.db"))
    state = PortfolioState.initial(
        "KRW-BTC",
        config.risk.initial_cash,
        config_fingerprint=paper_config_fingerprint(config),
    )
    state.schema_version = PAPER_SCHEMA_VERSION
    state.last_bar_time = "2026-01-01T00:00:00+00:00"
    state.updated_at = "2026-01-01T01:00:00+00:00"
    SimulatedBroker(state, config.risk).buy(
        timestamp=pd.Timestamp(state.updated_at),
        raw_price=100_000_000,
        requested_notional=1_000_000,
        stop_distance_pct=0.01,
        reason="fixture",
        signal_time=state.updated_at,
        model_id="fixture",
    )
    store = SQLiteStore(config.data.database_path)
    account_key = paper_account_key(config)
    store.save_paper_step(
        account_key, state.to_dict(), [], expected_revision=0
    )

    class _RiskFirstClient:
        candle_calls = 0

        def fetch_ticker(self, *, market):
            return MarketTicker(
                market=market,
                price=98_000_000,
                exchange_timestamp=pd.Timestamp("2026-01-01T01:00:05Z"),
                observed_at=pd.Timestamp("2026-01-01T01:00:06Z"),
            )

        def fetch_candles(self, **_kwargs):
            self.candle_calls += 1
            raise AssertionError("stop path must return before candle sync")

    client = _RiskFirstClient()
    summary = run_paper_once(config, store=store, client=client)
    persisted = store.load_paper_state(account_key)

    assert client.candle_calls == 0
    assert summary.fills == 1
    assert persisted["quantity"] == 0
    assert persisted["cooldown_remaining"] == config.risk.cooldown_bars


def test_model_exception_halts_and_liquidates_existing_position(
    tmp_path, monkeypatch
) -> None:
    config = _paper_config(str(tmp_path / "paper.db"))
    candles = synthetic_candles(281)
    prior = candles.iloc[-2]
    state = PortfolioState.initial(
        "KRW-BTC",
        config.risk.initial_cash,
        config_fingerprint=paper_config_fingerprint(config),
    )
    state.schema_version = PAPER_SCHEMA_VERSION
    state.last_bar_time = prior["timestamp"].isoformat()
    state.updated_at = (
        pd.Timestamp(prior["timestamp"]) + pd.Timedelta(hours=1)
    ).isoformat()
    SimulatedBroker(state, config.risk).buy(
        timestamp=pd.Timestamp(state.updated_at),
        raw_price=float(prior["close"]),
        requested_notional=1_000_000,
        stop_distance_pct=0.08,
        reason="fixture",
        signal_time=state.updated_at,
        model_id="fixture",
    )
    store = SQLiteStore(config.data.database_path)
    account_key = paper_account_key(config)
    store.save_paper_step(
        account_key, state.to_dict(), [], expected_revision=0
    )

    class _Client:
        def fetch_candles(self, **_kwargs):
            return candles.copy()

        def fetch_ticker(self, *, market):
            close_time = pd.Timestamp(candles.iloc[-1]["timestamp"]) + pd.Timedelta(
                hours=1
            )
            return MarketTicker(
                market=market,
                price=float(candles.iloc[-1]["close"]),
                exchange_timestamp=close_time + pd.Timedelta(seconds=5),
                observed_at=close_time + pd.Timedelta(seconds=6),
            )

    def _fail_model(*_args, **_kwargs):
        raise RuntimeError("model boom")

    monkeypatch.setattr(
        "coinpilot.paper.generate_walk_forward_predictions", _fail_model
    )
    summary = run_paper_once(config, store=store, client=_Client())
    persisted = store.load_paper_state(account_key)
    events = store.recent_paper_events(account_key, limit=10)

    assert summary.halt_state == "HALTED"
    assert persisted["quantity"] == 0
    assert any(event["event_type"] == "model_not_ready" for event in events)
    assert any(
        event["event_type"] == "fill"
        and event["payload"]["side"] == "sell"
        for event in events
    )


def test_paper_fingerprint_covers_history_policy(tmp_path) -> None:
    config = _paper_config(str(tmp_path / "paper.db"))
    changed = dataclasses.replace(
        config,
        data=dataclasses.replace(config.data, candle_count=300),
        paper=dataclasses.replace(config.paper, history_candles=300),
    ).validate()

    assert paper_config_fingerprint(config) != paper_config_fingerprint(changed)


def test_paper_fingerprint_covers_market_data_source(tmp_path) -> None:
    config = _paper_config(str(tmp_path / "paper.db"))
    changed = dataclasses.replace(
        config,
        data=dataclasses.replace(
            config.data, api_base_url="https://example.invalid"
        ),
    ).validate()

    assert paper_config_fingerprint(config) != paper_config_fingerprint(changed)


def test_stale_cas_writer_rolls_back_its_event(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "paper.db")
    state = PortfolioState.initial("KRW-BTC", 1000).to_dict()
    assert store.save_paper_step(
        "account", state, [], expected_revision=0
    ) == 1
    losing_event = {
        "event_id": "loser",
        "timestamp": "2026-01-01T00:00:00+00:00",
        "event_type": "decision",
        "payload": {"writer": "stale"},
    }

    with pytest.raises(ConcurrentPaperUpdate):
        store.save_paper_step(
            "account", state, [losing_event], expected_revision=0
        )

    assert store.load_paper_state("account")["revision"] == 1
    assert store.recent_paper_events("account") == []


def test_event_collision_rolls_back_whole_step(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "paper.db")
    state = PortfolioState.initial("KRW-BTC", 1000).to_dict()
    original = {
        "event_id": "collision",
        "timestamp": "2026-01-01T00:00:00+00:00",
        "event_type": "decision",
        "payload": {"value": 1},
    }
    assert store.save_paper_step(
        "account", state, [original], expected_revision=0
    ) == 1
    new_event = {
        "event_id": "new",
        "timestamp": "2026-01-01T01:00:00+00:00",
        "event_type": "decision",
        "payload": {"value": 2},
    }
    collision = {
        **original,
        "payload": {"value": 999},
    }

    with pytest.raises(ValueError, match="collided"):
        store.save_paper_step(
            "account",
            state,
            [new_event, collision],
            expected_revision=1,
        )

    assert store.load_paper_state("account")["revision"] == 1
    events = store.recent_paper_events("account", limit=10)
    assert len(events) == 1
    assert events[0]["event_id"].endswith(":collision")
