from __future__ import annotations

import argparse
import dataclasses
import json
import math
import re
import signal
import sqlite3
import sys
import time
import uuid
from collections import Counter
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from coinpilot.backtest import (
    run_backtest,
    with_scaled_costs,
    write_backtest_artifacts,
)
from coinpilot.config import AppConfig, load_config
from coinpilot.data import (
    CandleValidationError,
    UpbitAPIError,
    UpbitCandleClient,
    candle_gaps,
    closed_candles,
    synthetic_candles,
)
from coinpilot.hft_archive import (
    HFTArchiveError,
    record_upbit_public_archive,
    validate_hft_archive_request,
    validate_hft_capture_id,
)
from coinpilot.hft_data import HFTCaptureError, capture_upbit_public
from coinpilot.hft_depth import (
    DepthBookValidationError,
    DepthReplayConfig,
    IndependentTakerOrder,
    TakerReplayOutcome,
    public_orderbooks_from_records,
    replay_independent_taker_orders,
)
from coinpilot.hft_features import (
    HFTFeatureConfig,
    HFTFeatureValidationError,
    iter_hft_feature_rows,
)
from coinpilot.hft_research import (
    hft_study_artifact_paths,
    run_hft_scenario_suite,
    write_capture_companions,
    write_hft_study_artifacts,
)
from coinpilot.hft_sim import HFTSimulationConfig
from coinpilot.hft_workflow import (
    DEFAULT_MAX_RESEARCH_RECORDS,
    HFTWorkflowError,
    artifact_bundle_lock,
    artifact_staging_path,
    invalidate_artifact_completion,
    load_hft_records,
    promote_staged_artifacts,
    stream_hft_records,
    write_artifact_completion,
    write_json_atomic,
    write_jsonl_atomic,
)
from coinpilot.paper import (
    TickerNotReadyError,
    paper_account_key,
    run_paper_once,
)
from coinpilot.research import (
    run_research_assessment,
    write_research_assessment,
)
from coinpilot.store import SQLiteStore


def _safe_cli_error(exc: BaseException) -> str:
    """Render a bounded, scrubbed error and one useful chained cause."""

    message = str(exc)
    if isinstance(exc, HFTArchiveError) and exc.__cause__ is not None:
        cause = exc.__cause__
        message = f"{message}; cause={type(cause).__name__}: {cause}"
    message = message[:2_000]
    message = re.sub(
        r"https://hooks\.slack\.com/services/[A-Za-z0-9/_-]+",
        "[REDACTED_SLACK_WEBHOOK]",
        message,
    )
    return re.sub(
        r"\b(?:xox[a-z]-|Bearer\s+)[A-Za-z0-9._-]+",
        "[REDACTED_TOKEN]",
        message,
        flags=re.IGNORECASE,
    )
from coinpilot.hft_shadow_signal import ShadowSignalError
from coinpilot.hft_shadow_store import ShadowStoreError
from coinpilot.shadow_cli import (
    cmd_shadow_notify,
    cmd_shadow_run,
    cmd_shadow_status,
    cmd_shadow_watchdog,
    cmd_shadow_web,
)
from coinpilot.shadow_ops import ShadowOperationsError
from coinpilot.shadow_service import ShadowServiceError


def _json_print(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def _client(config: AppConfig) -> UpbitCandleClient:
    return UpbitCandleClient(
        base_url=config.data.api_base_url,
        timeout_seconds=config.data.request_timeout_seconds,
    )


def _store(config: AppConfig) -> SQLiteStore:
    return SQLiteStore(config.data.database_path)


def _sync(config: AppConfig, count: int) -> dict[str, Any]:
    client = _client(config)
    candles = client.fetch_candles(
        market=config.data.market,
        interval_minutes=config.data.interval_minutes,
        count=count,
    )
    closed = closed_candles(
        candles,
        config.data.interval_minutes,
        now=candles.attrs.get("finalization_cutoff"),
    )
    store = _store(config)
    changed = store.upsert_candles(closed, config.data.interval_minutes)
    cached = store.load_candles(
        config.data.market, config.data.interval_minutes, limit=count
    )
    gaps = candle_gaps(cached, config.data.interval_minutes)
    return {
        "market": config.data.market,
        "interval_minutes": config.data.interval_minutes,
        "fetched": len(candles),
        "closed": len(closed),
        "database_rows_changed": changed,
        "first_timestamp": closed.iloc[0]["timestamp"].isoformat(),
        "last_timestamp": closed.iloc[-1]["timestamp"].isoformat(),
        "gap_count": len(gaps),
    }


def _load_backtest_data(
    config: AppConfig, *, count: int, offline: bool
):
    if isinstance(count, bool) or not isinstance(count, int) or count < 1:
        raise ValueError("Candle count must be a positive integer")
    store = _store(config)
    if not offline:
        fetched = _client(config).fetch_candles(
            market=config.data.market,
            interval_minutes=config.data.interval_minutes,
            # The newest API row is commonly still open. Request one extra so
            # the finalized cache can still satisfy the exact requested count.
            count=count + 1,
        )
        finalized = closed_candles(
            fetched,
            config.data.interval_minutes,
            now=fetched.attrs.get("finalization_cutoff"),
        )
        store.upsert_candles(finalized, config.data.interval_minutes)
    candles = store.load_candles(
        config.data.market, config.data.interval_minutes, limit=count
    )
    if candles.empty:
        raise ValueError("No local candles. Run `coinpilot sync` or omit --offline.")
    finalized = closed_candles(candles, config.data.interval_minutes)
    if len(finalized) < count:
        raise ValueError(
            f"Requested {count} closed candles; only {len(finalized)} are cached"
        )
    return finalized.tail(count).reset_index(drop=True)


def _cmd_sync(args: argparse.Namespace, config: AppConfig) -> int:
    count = config.data.candle_count if args.count is None else args.count
    if count < 1:
        raise ValueError("--count must be positive")
    _json_print(_sync(config, count))
    return 0


def _cmd_demo(args: argparse.Namespace, config: AppConfig) -> int:
    count = max(args.count, config.minimum_history_bars)
    candles = synthetic_candles(
        count,
        market=config.data.market,
        interval_minutes=config.data.interval_minutes,
        seed=args.seed,
    )
    result = run_backtest(candles, config)
    paths = write_backtest_artifacts(
        result, config.output.artifacts_dir, name="demo"
    )
    _json_print(
        {
            "mode": "synthetic_demo",
            "metrics": result.metrics,
            "artifacts": {key: str(path) for key, path in paths.items()},
        }
    )
    return 0


def _cmd_backtest(args: argparse.Namespace, config: AppConfig) -> int:
    count = config.data.candle_count if args.count is None else args.count
    candles = _load_backtest_data(config, count=count, offline=args.offline)
    minimum = config.minimum_history_bars
    if len(candles) < minimum:
        raise ValueError(
            f"Need at least {minimum} closed candles; only {len(candles)} available"
        )

    result = run_backtest(candles, config)
    stress_config = with_scaled_costs(config, 2.0)
    # Cost stress keeps the already fitted OOS model and signals fixed. Retraining
    # with a higher-cost label would test a different strategy, not execution stress.
    stress_result = run_backtest(
        candles, stress_config, predictions=result.predictions
    )
    paths = write_backtest_artifacts(
        result, config.output.artifacts_dir, name="backtest"
    )
    stress_paths = write_backtest_artifacts(
        stress_result, config.output.artifacts_dir, name="backtest-cost-2x"
    )
    _json_print(
        {
            "metrics": result.metrics,
            "cost_2x_metrics": stress_result.metrics,
            "artifacts": {key: str(path) for key, path in paths.items()},
            "cost_2x_artifacts": {
                key: str(path) for key, path in stress_paths.items()
            },
            "warning": (
                "Research result only. OHLCV full-fill assumptions do not prove "
                "future profitability."
            ),
        }
    )
    return 0


def _cmd_research(args: argparse.Namespace, config: AppConfig) -> int:
    count = config.data.candle_count if args.count is None else args.count
    candles = _load_backtest_data(
        config, count=count, offline=args.offline
    )
    if len(candles) < config.minimum_history_bars:
        raise ValueError(
            f"Need at least {config.minimum_history_bars} closed candles; "
            f"only {len(candles)} available"
        )
    run = run_research_assessment(
        candles,
        config,
        holdout_start=args.holdout_start,
        holdout_end=args.holdout_end,
    )
    assessment_path = write_research_assessment(
        run.assessment,
        Path(config.output.artifacts_dir) / "research-assessment.json",
    )
    artifacts: dict[str, dict[str, str]] = {}
    for name, result in (
        ("research-dev", run.development),
        ("research-holdout", run.confirmation),
        ("research-holdout-cost-2x", run.cost_stress_2x),
    ):
        if result is not None:
            paths = write_backtest_artifacts(
                result, config.output.artifacts_dir, name=name
            )
            artifacts[name] = {
                key: str(path) for key, path in paths.items()
            }
    _json_print(
        {
            "assessment": run.assessment.as_dict(),
            "assessment_path": str(assessment_path),
            "artifacts": artifacts,
        }
    )
    return 0


def _cmd_paper(args: argparse.Namespace, config: AppConfig) -> int:
    if args.config is None:
        raise ValueError(
            "paper mode requires --config so its database path is stable"
        )
    store = _store(config)
    client = _client(config)
    cycles = 0
    consecutive_errors = 0
    with store.paper_account_lock(paper_account_key(config)):
        while True:
            try:
                summary = run_paper_once(config, store=store, client=client)
            except (
                UpbitAPIError,
                CandleValidationError,
                TickerNotReadyError,
                sqlite3.OperationalError,
            ) as exc:
                if not args.loop:
                    raise
                consecutive_errors += 1
                retry_seconds = min(
                    config.paper.poll_seconds
                    * (2 ** min(consecutive_errors - 1, 4)),
                    300,
                )
                print(
                    json.dumps(
                        {
                            "paper_loop_error": type(exc).__name__,
                            "message": str(exc),
                            "retry_in_seconds": retry_seconds,
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    file=sys.stderr,
                )
                time.sleep(retry_seconds)
                continue
            consecutive_errors = 0
            _json_print(summary.as_dict())
            cycles += 1
            if not args.loop or (
                args.max_cycles and cycles >= args.max_cycles
            ):
                return 0
            time.sleep(config.paper.poll_seconds)


def _cmd_status(args: argparse.Namespace, config: AppConfig) -> int:
    if args.config is None:
        raise ValueError(
            "status requires --config so it reads the intended paper database"
        )
    if not 1 <= args.events <= 100:
        raise ValueError("--events must be between 1 and 100")
    store = _store(config)
    account_key = paper_account_key(config)
    state = store.load_paper_state(account_key)
    events = store.recent_paper_events(account_key, limit=args.events)
    _json_print(
        {
            "mode": "forward_paper_status",
            "simulated": True,
            "live_order_routing": False,
            "orders_sent": 0,
            "account_key": account_key,
            "state": state,
            "recent_events": events,
        }
    )
    return 0 if state is not None else 1


def _cmd_live(_: argparse.Namespace, __: AppConfig) -> int:
    print(
        "Live trading is deliberately disabled in this MVP. "
        "Complete out-of-sample and paper validation before implementing a "
        "separately reviewed, manually armed live adapter.",
        file=sys.stderr,
    )
    return 3


def _cmd_hft_capture(args: argparse.Namespace, config: AppConfig) -> int:
    output = (
        Path(config.output.artifacts_dir) / "hft" / "live-events.jsonl"
        if args.output is None
        else args.output
    )
    if not output.name.endswith(".jsonl"):
        raise ValueError("HFT capture output must end in .jsonl")
    quality_path = output.with_suffix(".quality.json")
    trades_path = output.with_suffix(".trades.csv")
    run_path = output.with_suffix(".capture.json")
    completion_path = _completion_sibling(output)
    final_paths = {
        "capture": output,
        "quality": quality_path,
        "public_trades": trades_path,
        "run": run_path,
    }
    if len({*final_paths.values(), completion_path}) != 5:
        raise ValueError("HFT capture artifact paths must be distinct")
    for path in (*final_paths.values(), completion_path):
        if path.is_symlink():
            raise ValueError(
                f"HFT capture artifact cannot be a symbolic link: {path}"
            )
        if path.exists() and not args.overwrite:
            raise ValueError(
                f"HFT capture artifact already exists: {path}; use --overwrite"
            )

    staged_paths = {
        name: artifact_staging_path(path)
        for name, path in final_paths.items()
    }
    try:
        staged_result = capture_upbit_public(
            market=config.data.market,
            duration_seconds=args.seconds,
            output_path=staged_paths["capture"],
            max_depth=args.depth,
        )
        result = dataclasses.replace(staged_result, output_path=output)
        write_capture_companions(
            staged_paths["capture"],
            quality=result.quality,
            quality_path=staged_paths["quality"],
            trades_path=staged_paths["public_trades"],
            reported_source_path=output,
        )
        run_payload = {
            "schema_version": 1,
            "mode": "public_market_data_capture",
            "capture": result.to_dict(),
            "public_only": True,
            "orders_sent": 0,
            "warning": (
                "This is a public market observation, not an account fill or "
                "evidence of HFT profitability."
            ),
        }
        write_json_atomic(run_payload, staged_paths["run"])
        with artifact_bundle_lock(completion_path):
            if args.overwrite:
                invalidate_artifact_completion(completion_path)
            promote_staged_artifacts(
                {
                    staged_paths[name]: final_paths[name]
                    for name in final_paths
                },
                overwrite=args.overwrite,
            )
            write_artifact_completion(
                final_paths,
                completion_path,
                metadata={
                    "mode": run_payload["mode"],
                    "source_kind": "captured_public_feed",
                    "public_only": True,
                    "orders_sent": 0,
                },
            )
    finally:
        for staged_path in staged_paths.values():
            staged_path.unlink(missing_ok=True)
    _json_print(
        {
            **run_payload,
            "artifacts": {
                **{key: str(path) for key, path in final_paths.items()},
                "completion": str(completion_path),
            },
        }
    )
    return 0


def _cmd_hft_record(args: argparse.Namespace, config: AppConfig) -> int:
    output_root = (
        Path(config.output.artifacts_dir) / "hft" / "archive"
        if args.output_root is None
        else args.output_root
    )
    capture_id = validate_hft_archive_request(
        market=config.data.market,
        duration_seconds=args.seconds,
        max_events=args.max_events,
        partition_seconds=args.partition_seconds,
        max_depth=args.depth,
        capture_id=validate_hft_capture_id(
            uuid.uuid4().hex if args.capture_id is None else args.capture_id
        ),
    )
    reservation_path = output_root / f"{capture_id}.reservation.json"
    prospective_summary = output_root / f"{capture_id}.run.json"
    prospective_completion = output_root / f"{capture_id}.complete.json"
    for prospective in (
        reservation_path,
        prospective_summary,
        prospective_completion,
    ):
        if prospective.exists() or prospective.is_symlink():
            raise ValueError(
                "HFT archive run artifact already exists for capture_id "
                f"{capture_id}: {prospective}"
            )
    write_json_atomic(
        {
            "schema_version": 1,
            "status": "capture_id_reserved",
            "capture_id": capture_id,
            "mode": "continuous_public_market_archive",
            "public_only": True,
            "orders_sent": 0,
            "interpretation": (
                "This durable reservation prevents capture_id reuse after "
                "interruptions or process crashes."
            ),
        },
        reservation_path,
    )
    stop = False

    def request_stop(_signum: int, _frame: Any) -> None:
        nonlocal stop
        stop = True

    previous_sigterm: Any = None
    sigterm_installed = False
    try:
        previous_sigterm = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, request_stop)
        sigterm_installed = True
    except (AttributeError, ValueError):
        # Signal handlers can only be installed from the main interpreter
        # thread. Bounded duration/max-events still constrain library callers.
        pass
    try:
        result = record_upbit_public_archive(
            market=config.data.market,
            duration_seconds=args.seconds,
            output_root=output_root,
            max_events=args.max_events,
            partition_seconds=args.partition_seconds,
            max_depth=args.depth,
            capture_id=capture_id,
            stop_requested=lambda: stop,
        )
    finally:
        if sigterm_installed:
            signal.signal(signal.SIGTERM, previous_sigterm)
    if len(result.data_paths) != len(result.manifest_paths):
        raise HFTWorkflowError(
            "HFT archive result has mismatched data and manifest paths"
        )
    run_summary_path = output_root / f"{result.capture_id}.run.json"
    run_completion_path = (
        output_root / f"{result.capture_id}.complete.json"
    )
    run_payload = {
        "schema_version": 1,
        "mode": "continuous_public_market_archive",
        "archive": result.to_dict(),
        "completion_path": str(run_completion_path),
        "public_only": True,
        "orders_sent": 0,
        "warning": (
            "Public trades are market observations, not CoinPilot fills. "
            "Archive creation does not establish profitability."
        ),
    }
    write_json_atomic(run_payload, run_summary_path)
    completion_members: dict[str, Path] = {
        "capture_id_reservation": reservation_path,
        "run_summary": run_summary_path,
    }
    for index, (data_path, manifest_path) in enumerate(
        zip(result.data_paths, result.manifest_paths, strict=True),
        start=1,
    ):
        completion_members[f"data_{index:04d}"] = data_path
        completion_members[f"manifest_{index:04d}"] = manifest_path
    write_artifact_completion(
        completion_members,
        run_completion_path,
        metadata={
            "mode": run_payload["mode"],
            "capture_id": result.capture_id,
            "partitions": len(result.data_paths),
            "public_only": True,
            "orders_sent": 0,
        },
    )
    _json_print(
        {
            **run_payload,
            "run_summary_path": str(run_summary_path),
            "run_completion_path": str(run_completion_path),
        }
    )
    return 0


def _parse_horizons_ms(value: str) -> tuple[int, ...]:
    try:
        horizons = tuple(int(item.strip()) for item in value.split(","))
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "--horizons-ms must be comma-separated positive integers"
        ) from exc
    if not horizons or any(item <= 0 for item in horizons):
        raise ValueError(
            "--horizons-ms must be comma-separated positive integers"
        )
    if len(horizons) != len(set(horizons)):
        raise ValueError("--horizons-ms cannot contain duplicates")
    return horizons


def _summary_sibling(path: Path) -> Path:
    name = path.name
    for suffix in (".jsonl.gz", ".jsonl"):
        if name.endswith(suffix):
            return path.with_name(
                name.removesuffix(suffix) + ".summary.json"
            )
    return path.with_name(name + ".summary.json")


def _completion_sibling(path: Path) -> Path:
    name = path.name
    for suffix in (".jsonl.gz", ".jsonl", ".json"):
        if name.endswith(suffix):
            return path.with_name(
                name.removesuffix(suffix) + ".complete.json"
            )
    return path.with_name(name + ".complete.json")


def _cmd_hft_features(args: argparse.Namespace, config: AppConfig) -> int:
    output = (
        Path(config.output.artifacts_dir) / "hft" / "features.jsonl.gz"
        if args.output is None
        else args.output
    )
    summary_path = (
        _summary_sibling(output)
        if args.summary is None
        else args.summary
    )
    completion_path = _completion_sibling(output)
    if len({output, summary_path, completion_path}) != 3:
        raise ValueError(
            "feature output, summary, and completion paths must differ"
        )
    for path in (output, summary_path, completion_path):
        if path.exists() and not args.overwrite:
            raise ValueError(
                f"HFT feature artifact already exists: {path}; use --overwrite"
            )

    horizons = _parse_horizons_ms(args.horizons_ms)
    feature_config = HFTFeatureConfig(
        trailing_window_ms=args.trailing_window_ms,
        label_horizons_ms=horizons,
        invalid_event_policy=args.invalid_event_policy,
        time_regression_policy=args.time_regression_policy,
        max_label_overshoot_ms=args.max_label_overshoot_ms,
        max_in_memory_trade_ids=args.max_in_memory_trade_ids,
    )
    stream = stream_hft_records(
        args.input,
        max_records=args.max_records,
    )
    row_count = 0
    segments: set[str] = set()
    valid_labels = {f"{horizon}ms": 0 for horizon in horizons}
    invalid_labels = {f"{horizon}ms": 0 for horizon in horizons}
    invalid_label_reasons = {
        f"{horizon}ms": Counter() for horizon in horizons
    }

    def counted_rows():
        nonlocal row_count
        for row in iter_hft_feature_rows(stream, feature_config):
            row_count += 1
            segments.add(str(row["segment_id"]))
            labels = row["labels"]
            for key in valid_labels:
                if labels[key]["valid"]:
                    valid_labels[key] += 1
                else:
                    invalid_labels[key] += 1
                    invalid_label_reasons[key][
                        str(labels[key]["invalid_reason"])
                    ] += 1
            yield row

    staged_output = artifact_staging_path(output)
    staged_summary = artifact_staging_path(summary_path)
    try:
        write_jsonl_atomic(counted_rows(), staged_output)
        summary = {
            "mode": "causal_hft_feature_dataset",
            "input": stream.to_dict(),
            "output_path": str(output),
            "completion_path": str(completion_path),
            "feature_rows": row_count,
            "segments": len(segments),
            "config": {
                "trailing_window_ms": args.trailing_window_ms,
                "label_horizons_ms": list(horizons),
                "invalid_event_policy": args.invalid_event_policy,
                "time_regression_policy": args.time_regression_policy,
                "max_label_overshoot_ms": args.max_label_overshoot_ms,
                "max_in_memory_trade_ids": (
                    args.max_in_memory_trade_ids
                ),
            },
            "valid_labels": valid_labels,
            "invalid_labels": invalid_labels,
            "invalid_label_reasons": {
                key: dict(sorted(reasons.items()))
                for key, reasons in invalid_label_reasons.items()
            },
            "causal_contract": (
                "Features use only records already received in archive order; "
                "future books are used only inside explicit label fields."
            ),
            "public_trades_are_own_fills": False,
        }
        write_json_atomic(summary, staged_summary)
        with artifact_bundle_lock(completion_path):
            if args.overwrite:
                invalidate_artifact_completion(completion_path)
            promote_staged_artifacts(
                {
                    staged_output: output,
                    staged_summary: summary_path,
                },
                overwrite=args.overwrite,
            )
            write_artifact_completion(
                {"features": output, "summary": summary_path},
                completion_path,
                metadata={
                    "mode": summary["mode"],
                    "input_combined_sha256": stream.combined_sha256,
                },
            )
    finally:
        staged_output.unlink(missing_ok=True)
        staged_summary.unlink(missing_ok=True)
    _json_print(
        {
            **summary,
            "summary_path": str(summary_path),
            "completion_path": str(completion_path),
        }
    )
    return 0


def _finite_percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    if not 0 <= quantile <= 1:
        raise ValueError("quantile must be in [0, 1]")
    ordered = sorted(value for value in values if math.isfinite(value))
    if not ordered:
        return None
    position = (len(ordered) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def _sample_evenly(values: tuple[Any, ...], maximum: int) -> tuple[Any, ...]:
    if isinstance(maximum, bool) or maximum < 1:
        raise ValueError("--max-orders must be a positive integer")
    if len(values) <= maximum:
        return values
    if maximum == 1:
        return (values[0],)
    indices = [
        round(index * (len(values) - 1) / (maximum - 1))
        for index in range(maximum)
    ]
    return tuple(values[index] for index in indices)


def _depth_outcome_invariance_signature(
    outcome: TakerReplayOutcome,
) -> tuple[Any, ...]:
    """Return every replay field that a fee-only stress must not change."""

    execution = outcome.execution
    execution_signature = (
        None
        if execution is None
        else (
            execution.capture_id,
            execution.connection_id,
            execution.market,
            execution.side,
            execution.status,
            execution.request_kind,
            execution.requested_base,
            execution.requested_quote,
            execution.filled_base,
            execution.unfilled_base,
            execution.filled_quote,
            execution.unfilled_quote,
            execution.fully_filled,
            execution.levels_consumed,
            execution.best_price,
            execution.mid_price,
            execution.vwap_price,
            execution.slippage_quote_vs_best,
            execution.slippage_bps_vs_best,
            execution.slippage_quote_vs_mid,
            execution.slippage_bps_vs_mid,
            execution.book_ordinal,
            execution.book_received_monotonic_ns,
            execution.book_received_wall_ns,
            execution.book_exchange_timestamp_ms,
        )
    )
    return (
        outcome.order_id,
        outcome.status,
        outcome.reason,
        outcome.decision_book_ordinal,
        outcome.decision_monotonic_ns,
        outcome.due_monotonic_ns,
        outcome.selected_book_ordinal,
        outcome.selected_book_monotonic_ns,
        execution_signature,
    )


def _summarize_depth_source_capture(
    records: tuple[Mapping[str, Any], ...],
) -> dict[str, Any]:
    """Describe actual monotonic coverage without conflating silence and loss."""

    capture_times: dict[str, list[int]] = {}
    capture_record_counts: Counter[str] = Counter()
    capture_gap_reasons: dict[str, Counter[str]] = {}
    gap_reason_counts: Counter[str] = Counter()
    for record in records:
        if not isinstance(record, Mapping):
            continue
        capture_value = record.get("capture_id")
        if not isinstance(capture_value, str) or not capture_value.strip():
            continue
        capture_id = capture_value.strip()
        try:
            monotonic_ns = int(record["received_monotonic_ns"])
        except (KeyError, TypeError, ValueError, OverflowError):
            continue
        if monotonic_ns < 0:
            continue
        capture_times.setdefault(capture_id, []).append(monotonic_ns)
        capture_record_counts[capture_id] += 1
        if record.get("gap_before") is True:
            reason_value = record.get("gap_reason")
            reason = (
                reason_value.strip()
                if isinstance(reason_value, str) and reason_value.strip()
                else "unspecified"
            )
            gap_reason_counts[reason] += 1
            capture_gap_reasons.setdefault(capture_id, Counter())[reason] += 1

    captures: list[dict[str, Any]] = []
    total_coverage = 0.0
    for capture_id in sorted(capture_times):
        times = capture_times[capture_id]
        first = min(times)
        last = max(times)
        coverage = max(0.0, (last - first) / 1_000_000_000)
        total_coverage += coverage
        captures.append(
            {
                "capture_id": capture_id,
                "records_with_monotonic_time": capture_record_counts[capture_id],
                "first_received_monotonic_ns": str(first),
                "last_received_monotonic_ns": str(last),
                "actual_coverage_seconds": coverage,
                "gap_reason_counts": dict(
                    sorted(capture_gap_reasons.get(capture_id, {}).items())
                ),
            }
        )

    observation_silence_count = gap_reason_counts.get("receive_interval", 0)
    return {
        "capture_count": len(captures),
        "actual_coverage_seconds": total_coverage,
        "coverage_definition": (
            "sum of last-minus-first monotonic receive spans per capture"
        ),
        "captures": captures,
        "gap_reason_counts": dict(sorted(gap_reason_counts.items())),
        "receive_interval_observation_silence_count": (
            observation_silence_count
        ),
        "confirmed_connection_or_error_gap_count": (
            sum(gap_reason_counts.values()) - observation_silence_count
        ),
        "gap_interpretation": (
            "receive_interval marks observation silence beyond the recorder "
            "threshold; it is not by itself proof of packet loss or a "
            "connection failure."
        ),
    }


def _depth_outcome_dict(outcome: TakerReplayOutcome) -> dict[str, Any]:
    result = dataclasses.asdict(outcome)
    result.update(
        {
            "record_schema_version": 1,
            "source_kind": "public_orderbook_counterfactual",
            "execution_model": "independent_visible_depth_taker",
            "simulated": True,
            "counterfactual": True,
            "own_execution": False,
            "orders_sent": 0,
        }
    )
    for key in (
        "decision_monotonic_ns",
        "due_monotonic_ns",
        "selected_book_monotonic_ns",
    ):
        if result[key] is not None:
            result[key] = str(result[key])
    execution = result.get("execution")
    if execution is not None:
        for key in (
            "book_received_monotonic_ns",
            "book_received_wall_ns",
        ):
            if execution[key] is not None:
                execution[key] = str(execution[key])
    return result


def _summarize_depth_outcomes(
    outcomes: tuple[TakerReplayOutcome, ...],
) -> dict[str, Any]:
    status_counts = Counter(outcome.status for outcome in outcomes)
    reason_counts = Counter(outcome.reason for outcome in outcomes)
    executions = [
        outcome.execution
        for outcome in outcomes
        if outcome.execution is not None
    ]
    levels = [execution.levels_consumed for execution in executions]
    best_slippage = [
        execution.slippage_bps_vs_best for execution in executions
    ]
    mid_slippage = [
        execution.slippage_bps_vs_mid for execution in executions
    ]
    latency_ms = [
        (
            int(outcome.selected_book_monotonic_ns)
            - outcome.decision_monotonic_ns
        )
        / 1_000_000
        for outcome in outcomes
        if outcome.selected_book_monotonic_ns is not None
    ]
    order_count = len(outcomes)
    execution_count = len(executions)
    full_fill_count = sum(
        execution.fully_filled for execution in executions
    )
    return {
        "orders": order_count,
        "status_counts": dict(sorted(status_counts.items())),
        "reason_counts": dict(sorted(reason_counts.items())),
        "executions": execution_count,
        "execution_rate": (
            execution_count / order_count if order_count else None
        ),
        "full_fills": full_fill_count,
        "partial_fills": sum(
            not execution.fully_filled for execution in executions
        ),
        "conditional_visible_depth_full_fill_rate": (
            full_fill_count / execution_count
            if execution_count
            else None
        ),
        "full_fill_rate_all_orders": (
            full_fill_count / order_count if order_count else None
        ),
        "full_fill_at_l1_count": sum(
            execution.fully_filled and execution.levels_consumed == 1
            for execution in executions
        ),
        "walked_multiple_levels_count": sum(
            execution.levels_consumed > 1 for execution in executions
        ),
        "levels_consumed_p50": _finite_percentile(
            [float(value) for value in levels], 0.50
        ),
        "levels_consumed_p95": _finite_percentile(
            [float(value) for value in levels], 0.95
        ),
        "levels_consumed_max": max(levels) if levels else None,
        "slippage_bps_vs_best_p50": _finite_percentile(
            best_slippage, 0.50
        ),
        "slippage_bps_vs_best_p95": _finite_percentile(
            best_slippage, 0.95
        ),
        "slippage_bps_vs_mid_p50": _finite_percentile(
            mid_slippage, 0.50
        ),
        "slippage_bps_vs_mid_p95": _finite_percentile(
            mid_slippage, 0.95
        ),
        "decision_to_selected_book_ms_p50": _finite_percentile(
            latency_ms,
            0.50,
        ),
        "decision_to_selected_book_ms_p95": _finite_percentile(
            latency_ms,
            0.95,
        ),
        "filled_quote_total": sum(
            execution.filled_quote for execution in executions
        ),
        "fee_quote_total": sum(
            execution.fee_quote for execution in executions
        ),
    }


def _cmd_hft_depth_replay(args: argparse.Namespace, config: AppConfig) -> int:
    output = (
        Path(config.output.artifacts_dir)
        / "hft"
        / "depth-replay-summary.json"
        if args.output is None
        else args.output
    )
    executions_output = (
        output.with_name(
            output.name.removesuffix(".json") + ".executions.jsonl.gz"
        )
        if args.executions_output is None
        else args.executions_output
    )
    completion_path = _completion_sibling(output)
    if len({output, executions_output, completion_path}) != 3:
        raise ValueError(
            "depth summary, execution, and completion paths must differ"
        )
    for path in (output, executions_output, completion_path):
        if path.exists() and not args.overwrite:
            raise ValueError(
                f"HFT depth artifact already exists: {path}; use --overwrite"
            )
    if not math.isfinite(args.latency_ms) or args.latency_ms < 0:
        raise ValueError("--latency-ms must be finite and non-negative")
    if (
        not math.isfinite(args.max_book_gap_ms)
        or args.max_book_gap_ms <= 0
    ):
        raise ValueError("--max-book-gap-ms must be finite and positive")
    if not math.isfinite(args.quote_notional) or args.quote_notional <= 0:
        raise ValueError("--quote-notional must be finite and positive")
    if isinstance(args.sample_every, bool) or args.sample_every < 1:
        raise ValueError("--sample-every must be a positive integer")

    fee_rate = (
        config.risk.fee_rate if args.fee_rate is None else args.fee_rate
    )
    batch = load_hft_records(
        args.input,
        max_records=args.max_records,
    )
    loaded = public_orderbooks_from_records(batch.records)
    if loaded.rejected_records and not args.allow_rejected:
        raise ValueError(
            "depth input contains "
            f"{loaded.rejected_records} rejected records; inspect or use "
            "--allow-rejected only after review"
        )
    if not loaded.books:
        raise ValueError(
            "depth replay requires hft-record archive envelopes with order books"
        )

    sides = ("buy", "sell") if args.side == "both" else (args.side,)
    if isinstance(args.max_orders, bool) or args.max_orders < 1:
        raise ValueError("--max-orders must be a positive integer")
    candidate_books = loaded.books[:: args.sample_every]
    maximum_books = max(1, math.ceil(args.max_orders / len(sides)))
    selected_books = _sample_evenly(candidate_books, maximum_books)
    orders: list[IndependentTakerOrder] = []
    for sample_index, book in enumerate(selected_books, start=1):
        for side in sides:
            orders.append(
                IndependentTakerOrder(
                    order_id=f"sample-{sample_index:08d}-{side}",
                    capture_id=book.capture_id,
                    connection_id=book.connection_id,
                    market=book.market,
                    side=side,
                    decision_book_ordinal=book.ordinal,
                    decision_monotonic_ns=book.received_monotonic_ns,
                    quote_notional=args.quote_notional,
                )
            )
    if len(orders) > args.max_orders:
        orders = orders[: args.max_orders]
    replay_config = DepthReplayConfig(
        latency_ns=int(args.latency_ms * 1_000_000),
        fee_rate=fee_rate,
        max_book_gap_ns=int(args.max_book_gap_ms * 1_000_000),
    ).validate()
    outcomes = replay_independent_taker_orders(
        loaded.books,
        tuple(orders),
        replay_config,
    )
    fee_2x_outcomes = replay_independent_taker_orders(
        loaded.books,
        tuple(orders),
        dataclasses.replace(
            replay_config,
            fee_rate=replay_config.fee_rate * 2,
        ),
    )
    base_summary = _summarize_depth_outcomes(outcomes)
    fee_2x_summary = _summarize_depth_outcomes(fee_2x_outcomes)
    base_signatures = tuple(
        _depth_outcome_invariance_signature(outcome)
        for outcome in outcomes
    )
    fee_2x_signatures = tuple(
        _depth_outcome_invariance_signature(outcome)
        for outcome in fee_2x_outcomes
    )
    fee_2x_selection_unchanged = base_signatures == fee_2x_signatures
    if not fee_2x_selection_unchanged:
        raise RuntimeError(
            "fee stress unexpectedly changed depth selection or filled quantity"
        )
    ingestion = {
        "total_records": loaded.total_records,
        "orderbooks": len(loaded.books),
        "skipped_non_orderbook_records": (
            loaded.skipped_non_orderbook_records
        ),
        "rejected_records": loaded.rejected_records,
        "rejection_examples": list(loaded.rejection_examples),
        "capture_ids": list(loaded.capture_ids),
        "connection_segments": [
            dataclasses.asdict(segment)
            for segment in loaded.connection_segments
        ],
        "gap_marker_count": loaded.gap_marker_count,
        "monotonic_regression_marker_count": (
            loaded.monotonic_regression_marker_count
        ),
        "propagated_gap_to_orderbook_count": (
            loaded.propagated_gap_to_orderbook_count
        ),
        "ordinal_discontinuity_count": loaded.ordinal_discontinuity_count,
        "reordered_record_count": loaded.reordered_record_count,
    }
    summary = {
        "mode": "independent_visible_depth_taker_replay",
        "input": batch.to_dict(),
        "source_capture": _summarize_depth_source_capture(batch.records),
        "ingestion": ingestion,
        "config": {
            "quote_notional": args.quote_notional,
            "side": args.side,
            "latency_ms": args.latency_ms,
            "max_book_gap_ms": args.max_book_gap_ms,
            "fee_rate": replay_config.fee_rate,
            "sample_every": args.sample_every,
            "max_orders": args.max_orders,
            "selected_orderbooks": len(selected_books),
            "selected_orderbook_decisions": len(orders),
        },
        "base": base_summary,
        "fee_2x": fee_2x_summary,
        "fee_2x_selection_unchanged": fee_2x_selection_unchanged,
        "executions_path": str(executions_output),
        "completion_path": str(completion_path),
        "assumptions": [
            "aggregated visible public depth only",
            "independent marketable taker orders",
            "no hidden liquidity, replenishment, or endogenous market impact",
            "not a portfolio PnL or return simulation",
        ],
        "orders_sent": 0,
        "public_trades_are_own_fills": False,
    }
    staged_executions = artifact_staging_path(executions_output)
    staged_summary = artifact_staging_path(output)
    try:
        write_jsonl_atomic(
            (_depth_outcome_dict(outcome) for outcome in outcomes),
            staged_executions,
        )
        write_json_atomic(summary, staged_summary)
        with artifact_bundle_lock(completion_path):
            if args.overwrite:
                invalidate_artifact_completion(completion_path)
            promote_staged_artifacts(
                {
                    staged_executions: executions_output,
                    staged_summary: output,
                },
                overwrite=args.overwrite,
            )
            write_artifact_completion(
                {"executions": executions_output, "summary": output},
                completion_path,
                metadata={
                    "mode": summary["mode"],
                    "input_combined_sha256": batch.combined_sha256,
                },
            )
    finally:
        staged_executions.unlink(missing_ok=True)
        staged_summary.unlink(missing_ok=True)
    _json_print(
        {
            **summary,
            "summary_path": str(output),
            "completion_path": str(completion_path),
        }
    )
    return 0


def _cmd_hft_simulate(args: argparse.Namespace, config: AppConfig) -> int:
    fee_rate = (
        config.risk.fee_rate if args.fee_rate is None else args.fee_rate
    )
    order_notional = (
        min(250_000.0, config.risk.initial_cash * 0.025)
        if args.order_notional is None
        else args.order_notional
    )
    base_config = HFTSimulationConfig(
        initial_cash=config.risk.initial_cash,
        order_notional=order_notional,
        maker_fee_rate=fee_rate,
        taker_fee_rate=fee_rate,
        latency_steps=args.latency_steps,
        entry_threshold=args.entry_threshold,
        exit_threshold=args.exit_threshold,
        max_holding_steps=args.max_holding_steps,
        equity_sample_points=args.equity_points,
    ).validate()
    study = run_hft_scenario_suite(
        event_count=args.events,
        seed=args.seed,
        base_config=base_config,
        interval_ns=args.interval_ms * 1_000_000,
        volatility_bps=args.volatility_bps,
        base_spread_bps=args.spread_bps,
        weak_alpha_bps=args.weak_alpha_bps,
        illustrative_strong_alpha_bps=args.strong_alpha_bps,
    )
    output_dir = (
        Path(config.output.artifacts_dir) / "hft"
        if args.output_dir is None
        else args.output_dir
    )
    paths = hft_study_artifact_paths(output_dir)
    completion_path = output_dir / "simulation.complete.json"
    if len({*paths.values(), completion_path}) != len(paths) + 1:
        raise ValueError("HFT simulation artifact paths must be distinct")
    for path in (*paths.values(), completion_path):
        if path.is_symlink():
            raise ValueError(
                f"HFT simulation artifact cannot be a symbolic link: {path}"
            )
        if path.exists() and not args.overwrite:
            raise ValueError(
                f"HFT simulation artifact already exists: {path}; "
                "use --overwrite"
            )
    staged_paths = {
        name: artifact_staging_path(path)
        for name, path in paths.items()
    }
    try:
        write_hft_study_artifacts(
            study,
            output_dir,
            artifact_paths=staged_paths,
        )
        with artifact_bundle_lock(completion_path):
            if args.overwrite:
                invalidate_artifact_completion(completion_path)
            promote_staged_artifacts(
                {
                    staged_paths[name]: paths[name]
                    for name in paths
                },
                overwrite=args.overwrite,
            )
            write_artifact_completion(
                paths,
                completion_path,
                metadata={
                    "mode": "synthetic_hft_research",
                    "source_kind": "synthetic",
                    "event_count": study.event_count,
                    "seed": study.seed,
                },
            )
    finally:
        for staged_path in staged_paths.values():
            staged_path.unlink(missing_ok=True)
    _json_print(
        {
            "mode": "synthetic_hft_research",
            "scenarios": [
                scenario.summary_dict() for scenario in study.scenarios
            ],
            "artifacts": {
                **{key: str(path) for key, path in paths.items()},
                "completion": str(completion_path),
            },
            "warning": (
                "Every fill is simulated. Event-window returns are not annual "
                "returns and do not support a live-profit claim."
            ),
        }
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="coinpilot",
        description="Leakage-aware crypto research and paper trading",
    )
    parser.add_argument(
        "--config",
        type=Path,
        help="TOML configuration path (defaults are used when omitted)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    sync = subparsers.add_parser("sync", help="Fetch public Upbit candles")
    sync.add_argument("--count", type=int)
    sync.set_defaults(handler=_cmd_sync)

    demo = subparsers.add_parser(
        "demo", help="Run a deterministic synthetic-data smoke test"
    )
    demo.add_argument("--count", type=int, default=1200)
    demo.add_argument("--seed", type=int, default=7)
    demo.set_defaults(handler=_cmd_demo)

    backtest = subparsers.add_parser(
        "backtest", help="Run base and 2x-cost walk-forward backtests"
    )
    backtest.add_argument("--count", type=int)
    backtest.add_argument(
        "--offline", action="store_true", help="Use only the local SQLite cache"
    )
    backtest.set_defaults(handler=_cmd_backtest)

    research = subparsers.add_parser(
        "research",
        help=(
            "Evaluate one frozen candidate on an explicit 365-day "
            "historical confirmation range"
        ),
    )
    research.add_argument("--holdout-start", required=True)
    research.add_argument("--holdout-end", required=True)
    research.add_argument("--count", type=int)
    research.add_argument(
        "--offline", action="store_true", help="Use only the local SQLite cache"
    )
    research.set_defaults(handler=_cmd_research)

    paper = subparsers.add_parser(
        "paper", help="Process new closed candles in a persistent paper account"
    )
    paper.add_argument("--loop", action="store_true")
    paper.add_argument(
        "--max-cycles",
        type=int,
        default=0,
        help="Stop a loop after N cycles; zero means no limit",
    )
    paper.set_defaults(handler=_cmd_paper)

    status = subparsers.add_parser("status", help="Show paper account state")
    status.add_argument("--events", type=int, default=10)
    status.set_defaults(handler=_cmd_status)

    hft_capture = subparsers.add_parser(
        "hft-capture",
        help="Capture bounded public Upbit orderbook and trade events",
    )
    hft_capture.add_argument("--seconds", type=float, default=30.0)
    hft_capture.add_argument("--depth", type=int, default=30)
    hft_capture.add_argument("--output", type=Path)
    hft_capture.add_argument("--overwrite", action="store_true")
    hft_capture.set_defaults(handler=_cmd_hft_capture)

    hft_record = subparsers.add_parser(
        "hft-record",
        help=(
            "Continuously archive public Upbit depth/trades with reconnects, "
            "UTC gzip partitions, and SHA-256 manifests"
        ),
    )
    hft_record.add_argument("--seconds", type=float, default=3_600.0)
    hft_record.add_argument("--partition-seconds", type=int, default=3_600)
    hft_record.add_argument("--depth", type=int, default=30)
    hft_record.add_argument("--max-events", type=int)
    hft_record.add_argument("--capture-id")
    hft_record.add_argument("--output-root", type=Path)
    hft_record.set_defaults(handler=_cmd_hft_record)

    hft_features = subparsers.add_parser(
        "hft-features",
        help="Build causal microstructure features and future-return labels",
    )
    hft_features.add_argument("--input", type=Path, required=True)
    hft_features.add_argument("--output", type=Path)
    hft_features.add_argument("--summary", type=Path)
    hft_features.add_argument("--trailing-window-ms", type=int, default=1_000)
    hft_features.add_argument(
        "--horizons-ms",
        default="100,1000,5000",
        help="Comma-separated future label horizons",
    )
    hft_features.add_argument(
        "--invalid-event-policy",
        choices=("raise", "segment"),
        default="raise",
    )
    hft_features.add_argument(
        "--time-regression-policy",
        choices=("raise", "segment"),
        default="segment",
    )
    hft_features.add_argument(
        "--max-label-overshoot-ms",
        type=int,
        default=250,
        help=(
            "Reject a future label when the first eligible book arrives more "
            "than this many milliseconds after its target horizon"
        ),
    )
    hft_features.add_argument(
        "--max-in-memory-trade-ids",
        type=int,
        default=250_000,
        help=(
            "Fail closed after this many exact public-trade IDs in one "
            "uninterrupted feature segment"
        ),
    )
    hft_features.add_argument(
        "--max-records",
        type=int,
        default=DEFAULT_MAX_RESEARCH_RECORDS,
        help=(
            "Safety bound across selected partitions; select a narrower UTC "
            "range or raise explicitly for larger research jobs"
        ),
    )
    hft_features.add_argument("--overwrite", action="store_true")
    hft_features.set_defaults(handler=_cmd_hft_features)

    hft_depth = subparsers.add_parser(
        "hft-depth-replay",
        help=(
            "Replay independent taker orders through visible multi-level "
            "archive depth after monotonic latency"
        ),
    )
    hft_depth.add_argument("--input", type=Path, required=True)
    hft_depth.add_argument("--quote-notional", type=float, default=250_000.0)
    hft_depth.add_argument(
        "--side",
        choices=("buy", "sell", "both"),
        default="buy",
    )
    hft_depth.add_argument("--latency-ms", type=float, default=0.0)
    hft_depth.add_argument("--max-book-gap-ms", type=float, default=1_000.0)
    hft_depth.add_argument("--fee-rate", type=float)
    hft_depth.add_argument("--sample-every", type=int, default=1)
    hft_depth.add_argument("--max-orders", type=int, default=10_000)
    hft_depth.add_argument(
        "--max-records",
        type=int,
        default=DEFAULT_MAX_RESEARCH_RECORDS,
    )
    hft_depth.add_argument("--allow-rejected", action="store_true")
    hft_depth.add_argument("--output", type=Path)
    hft_depth.add_argument("--executions-output", type=Path)
    hft_depth.add_argument("--overwrite", action="store_true")
    hft_depth.set_defaults(handler=_cmd_hft_depth_replay)

    hft_simulate = subparsers.add_parser(
        "hft-simulate",
        help="Run deterministic synthetic HFT cost and latency scenarios",
    )
    hft_simulate.add_argument("--events", type=int, default=10_000)
    hft_simulate.add_argument("--seed", type=int, default=23)
    hft_simulate.add_argument("--interval-ms", type=int, default=100)
    hft_simulate.add_argument("--volatility-bps", type=float, default=0.80)
    hft_simulate.add_argument("--spread-bps", type=float, default=1.20)
    hft_simulate.add_argument("--weak-alpha-bps", type=float, default=0.18)
    hft_simulate.add_argument("--strong-alpha-bps", type=float, default=4.0)
    hft_simulate.add_argument("--fee-rate", type=float)
    hft_simulate.add_argument("--order-notional", type=float)
    hft_simulate.add_argument("--latency-steps", type=int, default=1)
    hft_simulate.add_argument("--entry-threshold", type=float, default=0.45)
    hft_simulate.add_argument("--exit-threshold", type=float, default=0.0)
    hft_simulate.add_argument("--max-holding-steps", type=int, default=25)
    hft_simulate.add_argument("--equity-points", type=int, default=250)
    hft_simulate.add_argument("--output-dir", type=Path)
    hft_simulate.add_argument("--overwrite", action="store_true")
    hft_simulate.set_defaults(handler=_cmd_hft_simulate)

    shadow_run = subparsers.add_parser(
        "shadow-run",
        help=(
            "Archive the public feed and run the stateful, zero-live-order "
            "shadow account"
        ),
    )
    shadow_run.add_argument(
        "--seconds",
        type=float,
        help="Override shadow.session_seconds for a bounded smoke run",
    )
    shadow_run.add_argument("--max-events", type=int)
    shadow_run.add_argument("--capture-id")
    shadow_run.set_defaults(handler=cmd_shadow_run)

    shadow_notify = subparsers.add_parser(
        "shadow-notify",
        help="Deliver the durable shadow outbox to Slack via macOS Keychain",
    )
    shadow_notify.add_argument("--once", action="store_true")
    shadow_notify.set_defaults(handler=cmd_shadow_notify)

    shadow_web = subparsers.add_parser(
        "shadow-web",
        help="Serve the read-only shadow dashboard on localhost",
    )
    shadow_web.add_argument("--host")
    shadow_web.add_argument("--port", type=int)
    shadow_web.set_defaults(handler=cmd_shadow_web)

    shadow_watchdog = subparsers.add_parser(
        "shadow-watchdog",
        help="Check the latest shadow heartbeat and enqueue a stale alert",
    )
    shadow_watchdog.add_argument(
        "--once",
        action="store_true",
        help="Run one check (watchdog is intentionally one-shot)",
    )
    shadow_watchdog.set_defaults(handler=cmd_shadow_watchdog)

    shadow_status = subparsers.add_parser(
        "shadow-status",
        help="Show the latest shadow state, fills, and sampled equity",
    )
    shadow_status.add_argument("--fills", type=int, default=20)
    shadow_status.add_argument("--equity", type=int, default=100)
    shadow_status.set_defaults(handler=cmd_shadow_status)

    live = subparsers.add_parser("live", help="Explain the hard live-trading gate")
    live.set_defaults(handler=_cmd_live)
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        config = load_config(args.config)
        exit_code = int(args.handler(args, config))
    except (
        ValueError,
        CandleValidationError,
        UpbitAPIError,
        HFTCaptureError,
        HFTArchiveError,
        HFTFeatureValidationError,
        HFTWorkflowError,
        DepthBookValidationError,
        ShadowSignalError,
        ShadowStoreError,
        ShadowOperationsError,
        ShadowServiceError,
    ) as exc:
        print(f"error: {_safe_cli_error(exc)}", file=sys.stderr)
        exit_code = 2
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        exit_code = 130
    raise SystemExit(exit_code)
