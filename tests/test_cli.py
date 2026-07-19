from __future__ import annotations

import argparse
import dataclasses
import gzip
import json
from pathlib import Path

import pandas as pd
import pytest

from coinpilot import cli
from coinpilot.config import AppConfig, DataConfig, ModelConfig, PaperConfig
from coinpilot.data import UpbitAPIError, synthetic_candles
from coinpilot.hft_archive import HFTArchiveResult
from coinpilot.hft_data import (
    HFTCaptureError,
    HFTCaptureResult,
    normalize_upbit_message,
    profile_hft_events,
)
from coinpilot.hft_workflow import verify_artifact_completion
from coinpilot.paper import TickerNotReadyError
from coinpilot.store import SQLiteStore


class _StaticCandleClient:
    def __init__(self, candles):
        self.candles = candles

    def fetch_candles(self, **_):
        return self.candles.copy()


def test_sync_never_persists_an_incomplete_candle(tmp_path, monkeypatch) -> None:
    candles = synthetic_candles(280)
    last_start = pd.Timestamp.now(tz="UTC") - pd.Timedelta(minutes=30)
    candles["timestamp"] = pd.date_range(
        end=last_start, periods=len(candles), freq="1h", tz="UTC"
    )
    base = AppConfig()
    config = dataclasses.replace(
        base,
        data=DataConfig(
            candle_count=280,
            database_path=str(tmp_path / "coinpilot.db"),
        ),
        model=ModelConfig(
            horizon_bars=3,
            min_train_samples=100,
            train_window=160,
            retrain_every=24,
            max_iterations=80,
        ),
        paper=PaperConfig(history_candles=280),
    ).validate()
    monkeypatch.setattr(
        cli, "_client", lambda _: _StaticCandleClient(candles)
    )

    result = cli._sync(config, 280)
    stored = SQLiteStore(config.data.database_path).load_candles(
        config.data.market, config.data.interval_minutes
    )

    assert result["closed"] == 279
    assert len(stored) == 279
    assert candles.iloc[-1]["timestamp"] not in set(stored["timestamp"])


def test_sync_uses_request_time_cutoff_not_later_wall_clock(
    tmp_path, monkeypatch
) -> None:
    candles = synthetic_candles(280)
    candles["timestamp"] = pd.date_range(
        end=pd.Timestamp("2026-01-01T01:00:00Z"),
        periods=len(candles),
        freq="1h",
    )
    candles.attrs["finalization_cutoff"] = pd.Timestamp(
        "2026-01-01T02:00:04Z"
    )
    base = AppConfig()
    config = dataclasses.replace(
        base,
        data=DataConfig(
            candle_count=280,
            database_path=str(tmp_path / "coinpilot.db"),
        ),
        model=ModelConfig(
            horizon_bars=3,
            min_train_samples=100,
            train_window=160,
            retrain_every=24,
            max_iterations=80,
        ),
        paper=PaperConfig(history_candles=280),
    ).validate()
    monkeypatch.setattr(
        cli, "_client", lambda _: _StaticCandleClient(candles)
    )

    result = cli._sync(config, 280)
    stored = SQLiteStore(config.data.database_path).load_candles(
        config.data.market, config.data.interval_minutes
    )

    assert result["closed"] == 279
    assert len(stored) == 279
    assert candles.iloc[-1]["timestamp"] not in set(stored["timestamp"])


def test_paper_loop_retries_a_transient_api_failure(
    tmp_path, monkeypatch, capsys
) -> None:
    base = AppConfig()
    config = dataclasses.replace(
        base,
        data=dataclasses.replace(
            base.data, database_path=str(tmp_path / "coinpilot.db")
        ),
    ).validate()
    attempts = 0

    class _Summary:
        def as_dict(self):
            return {"halt_state": "ACTIVE"}

    def _run(*_args, **_kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise UpbitAPIError("temporary")
        return _Summary()

    monkeypatch.setattr(cli, "_client", lambda _: object())
    monkeypatch.setattr(cli, "run_paper_once", _run)
    monkeypatch.setattr(cli.time, "sleep", lambda _: None)
    args = argparse.Namespace(
        config=tmp_path / "config.toml",
        loop=True,
        max_cycles=1,
    )

    assert cli._cmd_paper(args, config) == 0
    captured = capsys.readouterr()
    assert attempts == 2
    assert "paper_loop_error" in captured.err
    assert '"halt_state": "ACTIVE"' in captured.out


def test_paper_loop_retries_a_ticker_that_is_not_ready(
    tmp_path, monkeypatch, capsys
) -> None:
    base = AppConfig()
    config = dataclasses.replace(
        base,
        data=dataclasses.replace(
            base.data, database_path=str(tmp_path / "coinpilot.db")
        ),
    ).validate()
    attempts = 0

    class _Summary:
        def as_dict(self):
            return {"halt_state": "ACTIVE"}

    def _run(*_args, **_kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise TickerNotReadyError("no post-close trade yet")
        return _Summary()

    monkeypatch.setattr(cli, "_client", lambda _: object())
    monkeypatch.setattr(cli, "run_paper_once", _run)
    monkeypatch.setattr(cli.time, "sleep", lambda _: None)
    args = argparse.Namespace(
        config=tmp_path / "config.toml",
        loop=True,
        max_cycles=1,
    )

    assert cli._cmd_paper(args, config) == 0
    captured = capsys.readouterr()
    assert attempts == 2
    assert "TickerNotReadyError" in captured.err


def _hft_book(timestamp_ms: int, received_at_ns: int) -> dict[str, object]:
    return normalize_upbit_message(
        {
            "type": "orderbook",
            "code": "KRW-BTC",
            "timestamp": timestamp_ms,
            "total_ask_size": 1.0,
            "total_bid_size": 1.0,
            "orderbook_units": [
                {
                    "ask_price": 101.0,
                    "ask_size": 1.0,
                    "bid_price": 100.0,
                    "bid_size": 1.0,
                }
            ],
        },
        received_at_ns=received_at_ns,
    )


def test_hft_features_cli_writes_causal_gzip_dataset_and_summary(
    tmp_path: Path,
    capsys,
) -> None:
    source = tmp_path / "events.jsonl"
    source.write_text(
        "\n".join(
            json.dumps(
                _hft_book(
                    1 + index * 100,
                    1_000_000_000 + index * 100_000_000,
                )
            )
            for index in range(3)
        )
        + "\n",
        encoding="utf-8",
    )
    output = tmp_path / "features.jsonl.gz"
    args = argparse.Namespace(
        input=source,
        output=output,
        summary=None,
        trailing_window_ms=1_000,
        horizons_ms="100",
        invalid_event_policy="raise",
        time_regression_policy="segment",
        max_label_overshoot_ms=250,
        max_in_memory_trade_ids=250_000,
        max_records=100,
        overwrite=False,
    )

    assert cli._cmd_hft_features(args, AppConfig()) == 0

    with gzip.open(output, "rt", encoding="utf-8") as handle:
        rows = [json.loads(line) for line in handle]
    summary = json.loads(
        (tmp_path / "features.summary.json").read_text(encoding="utf-8")
    )
    assert len(rows) == 3
    assert rows[0]["labels"]["100ms"]["valid"] is True
    assert rows[0]["source_kind"] == "captured_public_market_data"
    assert rows[0]["public_trades_are_own_fills"] is False
    assert rows[-1]["labels"]["100ms"]["valid"] is False
    assert summary["feature_rows"] == 3
    assert summary["valid_labels"]["100ms"] == 2
    completion = json.loads(
        (tmp_path / "features.complete.json").read_text(encoding="utf-8")
    )
    assert completion["status"] == "complete"
    assert set(completion["members"]) == {"features", "summary"}
    assert '"mode": "causal_hft_feature_dataset"' in capsys.readouterr().out

    marker_path = tmp_path / "features.complete.json"
    marker_before = marker_path.read_bytes()
    bad_source = tmp_path / "bad-events.jsonl"
    bad_source.write_text("{bad}\n", encoding="utf-8")
    bad_args = argparse.Namespace(
        **{
            **vars(args),
            "input": bad_source,
            "overwrite": True,
        }
    )
    with pytest.raises(cli.HFTWorkflowError, match="invalid JSON"):
        cli._cmd_hft_features(bad_args, AppConfig())
    assert marker_path.read_bytes() == marker_before


def test_hft_record_cli_is_public_only_and_reports_zero_orders(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    data_path = tmp_path / "events.jsonl.gz"
    manifest_path = tmp_path / "events.manifest.json"
    data_path.write_bytes(b"archive-bytes")
    manifest_path.write_text("{}\n", encoding="utf-8")
    result = HFTArchiveResult(
        capture_id="test",
        market="KRW-BTC",
        requested_duration_seconds=1.0,
        elapsed_monotonic_seconds=1.0,
        raw_messages=2,
        written_events=2,
        rejected_messages=0,
        connection_count=1,
        reconnect_count=0,
        gap_count=0,
        pings_sent=0,
        heartbeats_received=0,
        error_counts={},
        data_paths=(data_path,),
        manifest_paths=(manifest_path,),
    )
    monkeypatch.setattr(
        cli,
        "record_upbit_public_archive",
        lambda **_: result,
    )
    args = argparse.Namespace(
        seconds=1.0,
        output_root=tmp_path,
        max_events=2,
        partition_seconds=3_600,
        depth=30,
        capture_id="test",
    )

    assert cli._cmd_hft_record(args, AppConfig()) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["public_only"] is True
    assert payload["orders_sent"] == 0
    run_summary = tmp_path / "test.run.json"
    run_completion = tmp_path / "test.complete.json"
    assert payload["run_summary_path"] == str(run_summary)
    assert payload["run_completion_path"] == str(run_completion)
    persisted = json.loads(run_summary.read_text(encoding="utf-8"))
    assert persisted["archive"]["requested_duration_seconds"] == 1.0
    assert persisted["archive"]["elapsed_monotonic_seconds"] == 1.0
    assert persisted["orders_sent"] == 0
    completion = verify_artifact_completion(run_completion)
    assert set(completion["members"]) == {
        "capture_id_reservation",
        "data_0001",
        "manifest_0001",
        "run_summary",
    }
    reservation = json.loads(
        (tmp_path / "test.reservation.json").read_text(encoding="utf-8")
    )
    assert reservation["status"] == "capture_id_reserved"
    assert reservation["orders_sent"] == 0


def test_hft_record_reserves_capture_id_before_recorder_and_blocks_reuse(
    tmp_path: Path,
    monkeypatch,
) -> None:
    calls = 0

    def interrupt_after_reservation(**kwargs):
        nonlocal calls
        calls += 1
        reservation = (
            tmp_path / f"{kwargs['capture_id']}.reservation.json"
        )
        assert reservation.is_file()
        raise KeyboardInterrupt

    monkeypatch.setattr(
        cli,
        "record_upbit_public_archive",
        interrupt_after_reservation,
    )
    args = argparse.Namespace(
        seconds=1.0,
        output_root=tmp_path,
        max_events=2,
        partition_seconds=3_600,
        depth=30,
        capture_id="interrupted",
    )

    with pytest.raises(KeyboardInterrupt):
        cli._cmd_hft_record(args, AppConfig())
    assert calls == 1
    assert (tmp_path / "interrupted.reservation.json").is_file()

    with pytest.raises(ValueError, match="already exists"):
        cli._cmd_hft_record(args, AppConfig())
    assert calls == 1

    invalid_args = argparse.Namespace(
        **{
            **vars(args),
            "seconds": -1.0,
            "capture_id": "invalid-duration",
        }
    )
    with pytest.raises(cli.HFTArchiveError, match="duration_seconds"):
        cli._cmd_hft_record(invalid_args, AppConfig())
    assert not (tmp_path / "invalid-duration.reservation.json").exists()


def test_hft_capture_cli_commits_a_hash_verified_bundle_last(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    event = _hft_book(1_700_000_000_000, 1_700_000_000_100_000_000)

    def capture(**kwargs):
        destination = Path(kwargs["output_path"])
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(
            json.dumps(event, separators=(",", ":"), sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return HFTCaptureResult(
            output_path=destination,
            requested_duration_seconds=1.0,
            elapsed_seconds=1.1,
            raw_messages=1,
            written_events=1,
            rejected_messages=0,
            quality=profile_hft_events([event]),
        )

    monkeypatch.setattr(cli, "capture_upbit_public", capture)
    output = tmp_path / "capture.jsonl"
    args = argparse.Namespace(
        seconds=1.0,
        depth=30,
        output=output,
        overwrite=False,
    )

    assert cli._cmd_hft_capture(args, AppConfig()) == 0

    completion_path = tmp_path / "capture.complete.json"
    completion = verify_artifact_completion(completion_path)
    assert set(completion["members"]) == {
        "capture",
        "public_trades",
        "quality",
        "run",
    }
    quality = json.loads(
        (tmp_path / "capture.quality.json").read_text(encoding="utf-8")
    )
    run = json.loads(
        (tmp_path / "capture.capture.json").read_text(encoding="utf-8")
    )
    assert quality["source_path"] == str(output)
    assert run["capture"]["output_path"] == str(output)
    assert run["orders_sent"] == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifacts"]["completion"] == str(completion_path)

    marker_before = completion_path.read_bytes()

    def fail_capture(**_kwargs):
        raise HFTCaptureError("injected capture failure")

    monkeypatch.setattr(cli, "capture_upbit_public", fail_capture)
    overwrite_args = argparse.Namespace(
        **{**vars(args), "overwrite": True}
    )
    with pytest.raises(HFTCaptureError, match="injected"):
        cli._cmd_hft_capture(overwrite_args, AppConfig())
    assert completion_path.read_bytes() == marker_before
    assert not tuple(tmp_path.glob(".*.stage.capture*"))


def test_hft_simulate_cli_commits_bundle_and_preserves_marker_on_stage_failure(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    args = argparse.Namespace(
        events=200,
        seed=23,
        interval_ms=100,
        volatility_bps=0.8,
        spread_bps=1.2,
        weak_alpha_bps=0.18,
        strong_alpha_bps=4.0,
        fee_rate=None,
        order_notional=25_000.0,
        latency_steps=1,
        entry_threshold=0.45,
        exit_threshold=0.0,
        max_holding_steps=25,
        equity_points=25,
        output_dir=tmp_path,
        overwrite=False,
    )

    assert cli._cmd_hft_simulate(args, AppConfig()) == 0

    completion_path = tmp_path / "simulation.complete.json"
    completion = verify_artifact_completion(completion_path)
    assert set(completion["members"]) == {
        "decisions",
        "equity",
        "simulated_executions",
        "summary",
    }
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifacts"]["completion"] == str(completion_path)
    marker_before = completion_path.read_bytes()

    def fail_stage(*_args, **_kwargs):
        raise RuntimeError("injected stage failure")

    monkeypatch.setattr(cli, "write_hft_study_artifacts", fail_stage)
    overwrite_args = argparse.Namespace(
        **{**vars(args), "overwrite": True}
    )
    with pytest.raises(RuntimeError, match="injected"):
        cli._cmd_hft_simulate(overwrite_args, AppConfig())
    assert completion_path.read_bytes() == marker_before


def test_hft_depth_replay_cli_sweeps_multiple_levels_without_orders(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    records = []
    for index in range(3):
        wall_ns = 1_700_000_000_000_000_000 + index * 100_000_000
        event = normalize_upbit_message(
            {
                "type": "orderbook",
                "code": "KRW-BTC",
                "timestamp": 1_700_000_000_000 + index * 100,
                "total_ask_size": 3.0,
                "total_bid_size": 3.0,
                "orderbook_units": [
                    {
                        "ask_price": 101.0,
                        "ask_size": 1.0,
                        "bid_price": 100.0,
                        "bid_size": 1.0,
                    },
                    {
                        "ask_price": 102.0,
                        "ask_size": 2.0,
                        "bid_price": 99.0,
                        "bid_size": 2.0,
                    },
                ],
            },
            received_at_ns=wall_ns,
        )
        records.append(
            {
                "schema_version": 1,
                "capture_id": "cli-depth",
                "connection_id": "cli-depth-connection-000001",
                "ordinal": index + 1,
                "received_wall_ns": str(wall_ns),
                "received_monotonic_ns": str(index * 100_000_000),
                "receive_delta_monotonic_ns": (
                    None if index == 0 else "100000000"
                ),
                "monotonic_regression": False,
                "gap_before": index == 2,
                "gap_reason": (
                    "receive_interval" if index == 2 else None
                ),
                "gap_duration_monotonic_ns": (
                    "100000000" if index == 2 else None
                ),
                "event": event,
            }
        )
    source = tmp_path / "archive.jsonl"
    source.write_text(
        "".join(json.dumps(record) + "\n" for record in records),
        encoding="utf-8",
    )
    output = tmp_path / "depth.json"
    args = argparse.Namespace(
        input=source,
        quote_notional=250.0,
        side="buy",
        latency_ms=0.0,
        max_book_gap_ms=1_000.0,
        fee_rate=0.001,
        sample_every=1,
        max_orders=100,
        max_records=100,
        allow_rejected=False,
        output=output,
        executions_output=None,
        overwrite=False,
    )

    assert cli._cmd_hft_depth_replay(args, AppConfig()) == 0

    summary = json.loads(output.read_text(encoding="utf-8"))
    with gzip.open(
        tmp_path / "depth.executions.jsonl.gz",
        "rt",
        encoding="utf-8",
    ) as handle:
        execution_row = json.loads(handle.readline())
    assert summary["base"]["orders"] == 3
    assert summary["base"]["execution_rate"] == pytest.approx(1.0)
    assert summary["base"]["full_fills"] == 3
    assert summary["base"][
        "conditional_visible_depth_full_fill_rate"
    ] == pytest.approx(1.0)
    assert summary["base"]["full_fill_rate_all_orders"] == pytest.approx(1.0)
    assert "visible_depth_full_fill_rate" not in summary["base"]
    assert summary["base"][
        "decision_to_selected_book_ms_p50"
    ] == pytest.approx(0.0)
    assert summary["base"][
        "decision_to_selected_book_ms_p95"
    ] == pytest.approx(0.0)
    assert "decision_to_fill_ms_p50" not in summary["base"]
    assert "decision_to_fill_ms_p95" not in summary["base"]
    assert summary["base"]["walked_multiple_levels_count"] == 3
    assert summary["fee_2x"]["fee_quote_total"] == pytest.approx(
        summary["base"]["fee_quote_total"] * 2
    )
    assert summary["fee_2x_selection_unchanged"] is True
    assert summary["source_capture"]["actual_coverage_seconds"] == (
        pytest.approx(0.2)
    )
    assert summary["source_capture"]["gap_reason_counts"] == {
        "receive_interval": 1
    }
    assert summary["source_capture"][
        "receive_interval_observation_silence_count"
    ] == 1
    assert summary["source_capture"][
        "confirmed_connection_or_error_gap_count"
    ] == 0
    assert summary["source_capture"]["captures"][0][
        "actual_coverage_seconds"
    ] == pytest.approx(0.2)
    assert summary["ingestion"]["ordinal_discontinuity_count"] == 0
    assert summary["config"]["max_orders"] == 100
    assert summary["config"]["selected_orderbooks"] == 3
    assert summary["config"]["selected_orderbook_decisions"] == 3
    assert summary["orders_sent"] == 0
    completion = json.loads(
        (tmp_path / "depth.complete.json").read_text(encoding="utf-8")
    )
    assert completion["status"] == "complete"
    assert set(completion["members"]) == {"executions", "summary"}
    depth_marker = tmp_path / "depth.complete.json"
    depth_marker_before = depth_marker.read_bytes()
    invalid_latency_args = argparse.Namespace(
        **{
            **vars(args),
            "latency_ms": -1.0,
            "overwrite": True,
        }
    )
    with pytest.raises(ValueError, match="latency-ms"):
        cli._cmd_hft_depth_replay(invalid_latency_args, AppConfig())
    assert depth_marker.read_bytes() == depth_marker_before
    assert execution_row["simulated"] is True
    assert execution_row["counterfactual"] is True
    assert execution_row["own_execution"] is False
    assert execution_row["orders_sent"] == 0
    assert '"mode": "independent_visible_depth_taker_replay"' in (
        capsys.readouterr().out
    )

    capped_output = tmp_path / "depth-capped.json"
    capped_args = argparse.Namespace(
        **{
            **vars(args),
            "side": "both",
            "max_orders": 3,
            "output": capped_output,
        }
    )

    assert cli._cmd_hft_depth_replay(capped_args, AppConfig()) == 0

    capped_summary = json.loads(capped_output.read_text(encoding="utf-8"))
    with gzip.open(
        tmp_path / "depth-capped.executions.jsonl.gz",
        "rt",
        encoding="utf-8",
    ) as handle:
        capped_execution_rows = [
            json.loads(line) for line in handle if line.strip()
        ]
    assert capped_summary["base"]["orders"] == 3
    assert capped_summary["config"]["max_orders"] == 3
    assert capped_summary["config"]["selected_orderbooks"] == 2
    assert capped_summary["config"]["selected_orderbook_decisions"] == 3
    assert len(capped_execution_rows) == 3
    assert [
        row["execution"]["side"] for row in capped_execution_rows
    ] == ["buy", "sell", "buy"]

    real_replay = cli.replay_independent_taker_orders
    replay_call_count = 0

    def replay_with_fee_dependent_order_drift(*replay_args, **replay_kwargs):
        nonlocal replay_call_count
        replay_call_count += 1
        replayed = real_replay(*replay_args, **replay_kwargs)
        if replay_call_count == 2:
            return (
                dataclasses.replace(
                    replayed[0],
                    reason="fee-dependent-order-drift",
                ),
                *replayed[1:],
            )
        return replayed

    monkeypatch.setattr(
        cli,
        "replay_independent_taker_orders",
        replay_with_fee_dependent_order_drift,
    )
    drift_args = argparse.Namespace(
        **{
            **vars(args),
            "output": tmp_path / "depth-fee-drift.json",
        }
    )

    with pytest.raises(
        RuntimeError,
        match="fee stress unexpectedly changed depth selection",
    ):
        cli._cmd_hft_depth_replay(drift_args, AppConfig())
