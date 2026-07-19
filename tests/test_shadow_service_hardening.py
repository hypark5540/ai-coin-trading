from __future__ import annotations

import dataclasses
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from coinpilot.config import AppConfig
from coinpilot.hft_shadow import ShadowEngine
from coinpilot.hft_shadow_store import ShadowInvariantError, ShadowStore
from coinpilot.shadow_service import (
    ShadowServiceAlreadyRunning,
    ShadowServiceProcessLock,
    installed_code_version,
    run_shadow_service,
    runtime_shadow_config,
    shadow_deployment_version,
    shadow_process_lock_path,
    shadow_runtime_fingerprint,
    shadow_runtime_manifest,
)


def _config(tmp_path: Path) -> AppConfig:
    base = AppConfig()
    return dataclasses.replace(
        base,
        shadow=dataclasses.replace(
            base.shadow,
            mode="diagnostic",
            database_path=str(tmp_path / "shadow.db"),
            archive_root=str(tmp_path / "archive"),
            warmup_books=1,
            entry_threshold=0.30,
            exit_threshold=0.0,
            latency_ms=0.0,
            session_seconds=10.0,
        ),
        operations=dataclasses.replace(
            base.operations,
            heartbeat_seconds=1,
            stale_after_seconds=2,
            backup_dir=str(tmp_path / "backups"),
        ),
    ).validate()


def _source_tree(tmp_path: Path) -> Path:
    root = tmp_path / "source"
    package = root / "src" / "coinpilot"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "strategy.py").write_text("VALUE = 1\n", encoding="utf-8")
    (root / "pyproject.toml").write_text(
        "[project]\nname='coinpilot'\nversion='0.1.0'\n",
        encoding="utf-8",
    )
    return root


def test_runtime_fingerprint_is_canonical_and_covers_complete_shadow_config(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    manifest = shadow_runtime_manifest(config)

    assert manifest["shadow"] == dataclasses.asdict(config.shadow)
    assert manifest["shadow"]["model_version"] == config.shadow.model_version
    expected = shadow_runtime_fingerprint(config)
    assert shadow_runtime_fingerprint(config) == expected

    alternatives: dict[str, object] = {
        "mode": "observe",
        "database_path": str(tmp_path / "other.db"),
        "archive_root": str(tmp_path / "other-archive"),
        "initial_cash": 11_000_000.0,
        "order_quote": 300_000.0,
        "fee_rate": 0.001,
        "latency_ms": 25.0,
        "max_book_gap_ms": 500.0,
        "warmup_books": 2,
        "trade_flow_window_ms": 500,
        "entry_threshold": 0.40,
        "exit_threshold": -0.10,
        "max_holding_seconds": 30.0,
        "max_spread_bps": 5.0,
        "max_daily_loss_pct": 0.02,
        "max_drawdown_pct": 0.06,
        "equity_sample_seconds": 7,
        "health_sample_seconds": 11,
        "partition_seconds": 60,
        "session_seconds": 20.0,
        "model_version": "diagnostic-model-v2",
    }
    assert set(alternatives) == {
        field.name for field in dataclasses.fields(config.shadow)
    }
    for field_name, value in alternatives.items():
        changed = dataclasses.replace(
            config,
            shadow=dataclasses.replace(
                config.shadow,
                **{field_name: value},
            ),
        ).validate()
        assert shadow_runtime_fingerprint(changed) != expected, field_name

    assert (
        shadow_runtime_fingerprint(config, duration_seconds=11.0) != expected
    )
    assert shadow_runtime_fingerprint(config, max_events=10) != expected


def test_code_fingerprint_changes_with_editable_source(tmp_path: Path) -> None:
    source = _source_tree(tmp_path)
    first = installed_code_version(source)
    assert installed_code_version(source) == first

    (source / "src" / "coinpilot" / "strategy.py").write_text(
        "VALUE = 2\n",
        encoding="utf-8",
    )
    assert installed_code_version(source) != first


@pytest.mark.parametrize(
    ("field_name", "value"),
    (
        ("entry_threshold", 0.40),
        ("max_daily_loss_pct", 0.02),
        ("model_version", "diagnostic-model-v2"),
    ),
)
def test_policy_or_model_change_forces_restart_halt(
    tmp_path: Path,
    field_name: str,
    value: object,
) -> None:
    config = _config(tmp_path)
    source = _source_tree(tmp_path)
    store = ShadowStore(config.shadow.database_path)
    first, _ = ShadowEngine.start(
        store,
        runtime_shadow_config(config),
        run_id="fingerprint-1",
        started_wall_ns=1_800_000_000_000_000_000,
        code_version=shadow_deployment_version(
            config,
            source_root=source,
        ),
    )
    first.stop(
        reason="deploy",
        ended_wall_ns=1_800_000_000_000_000_100,
    )

    changed = dataclasses.replace(
        config,
        shadow=dataclasses.replace(
            config.shadow,
            **{field_name: value},
        ),
    ).validate()
    restarted, result = ShadowEngine.start(
        store,
        runtime_shadow_config(changed),
        run_id="fingerprint-2",
        started_wall_ns=1_800_000_000_000_000_200,
        code_version=shadow_deployment_version(
            changed,
            source_root=source,
        ),
    )

    assert result.lifecycle_status == "halted_recovery"
    assert restarted.status()["halt_reason"] == "restart_fingerprint_changed"
    assert restarted.status()["orders_sent"] == 0

    restarted.stop(
        reason="session_complete",
        ended_wall_ns=1_800_000_000_000_000_300,
    )
    still_halted, next_result = ShadowEngine.start(
        store,
        runtime_shadow_config(changed),
        run_id="fingerprint-3",
        started_wall_ns=1_800_000_000_000_000_400,
        code_version=shadow_deployment_version(
            changed,
            source_root=source,
        ),
    )
    assert next_result.lifecycle_status == "halted_recovery"
    assert still_halted.status()["halt_reason"] == (
        "restart_fingerprint_changed"
    )


def test_same_run_id_rejects_changed_code_fingerprint(tmp_path: Path) -> None:
    config = _config(tmp_path)
    store = ShadowStore(config.shadow.database_path)
    ShadowEngine.start(
        store,
        runtime_shadow_config(config),
        run_id="same-run",
        started_wall_ns=1_800_000_000_000_000_000,
        code_version="code-v1",
    )
    with pytest.raises(ShadowInvariantError, match="immutable data"):
        ShadowEngine.start(
            store,
            runtime_shadow_config(config),
            run_id="same-run",
            started_wall_ns=1_800_000_000_000_000_100,
            code_version="code-v2",
        )


def test_service_fails_before_database_or_archive_mutation_when_locked(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    with ShadowServiceProcessLock(shadow_process_lock_path(config)):
        with pytest.raises(ShadowServiceAlreadyRunning, match="already running"):
            run_shadow_service(config, capture_id="blocked-capture")

    assert not Path(config.shadow.database_path).exists()
    assert not Path(config.shadow.archive_root).exists()


def test_kernel_lock_excludes_another_process_and_is_stale_safe(
    tmp_path: Path,
) -> None:
    lock_path = tmp_path / "process.lock"
    ready_path = tmp_path / "ready"
    project_root = Path(__file__).resolve().parents[1]
    environment = os.environ.copy()
    environment["PYTHONPATH"] = os.pathsep.join(
        filter(
            None,
            (
                str(project_root / "src"),
                environment.get("PYTHONPATH", ""),
            ),
        )
    )
    script = (
        "import sys,time\n"
        "from pathlib import Path\n"
        "from coinpilot.shadow_service import ShadowServiceProcessLock\n"
        "lock=ShadowServiceProcessLock(sys.argv[1])\n"
        "lock.acquire()\n"
        "Path(sys.argv[2]).touch()\n"
        "time.sleep(30)\n"
    )
    process = subprocess.Popen(
        [sys.executable, "-c", script, str(lock_path), str(ready_path)],
        cwd=project_root,
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 5
        while not ready_path.exists() and process.poll() is None:
            if time.monotonic() >= deadline:
                break
            time.sleep(0.02)
        if not ready_path.exists():
            stderr = process.stderr.read() if process.stderr is not None else ""
            pytest.fail(f"lock holder failed to start: {stderr}")

        with pytest.raises(ShadowServiceAlreadyRunning, match="already running"):
            with ShadowServiceProcessLock(lock_path):
                pytest.fail("a second process acquired the singleton lock")
    finally:
        process.terminate()
        process.wait(timeout=5)

    # A stale lock file remains, but the kernel ownership is gone.
    with ShadowServiceProcessLock(lock_path):
        assert lock_path.exists()
