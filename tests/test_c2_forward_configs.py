from __future__ import annotations

import importlib.util
import json
import os
import shutil
import stat
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import coinpilot
import pytest

from coinpilot.config import load_config


def _load_script() -> ModuleType:
    path = Path(__file__).parents[1] / "scripts" / "manage_c2_forward_configs.py"
    spec = importlib.util.spec_from_file_location("manage_c2_forward_configs", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


c2 = _load_script()
REPO_ROOT = Path(__file__).parents[1]
TEMPLATE_DIR = REPO_ROOT / "configs" / "forward-paper-c2"


def test_tracked_suite_is_exact_public_only_c2_policy() -> None:
    result = c2.validate_template_suite(TEMPLATE_DIR)
    assert result["markets"] == list(c2.EXPECTED_MARKETS)
    assert result["public_only"] is True
    assert result["live_order_routing"] is False
    assert result["orders_sent"] == 0

    accounts: set[str] = set()
    databases: set[str] = set()
    for market in c2.EXPECTED_MARKETS:
        path = TEMPLATE_DIR / f"{c2.MARKET_SLUGS[market]}.toml"
        config = load_config(path)
        assert config.data.market == market
        assert config.data.interval_minutes == 60
        assert config.data.candle_count == 2500
        assert config.data.api_base_url == "https://api.upbit.com"
        assert config.model.signal_mode == "trend_breakout"
        assert config.model.horizon_bars == 336
        assert config.model.breakout_entry_window == 336
        assert config.model.breakout_exit_window == 168
        assert config.risk.initial_cash == 2_500_000
        assert config.risk.risk_per_trade == 0.0075
        assert config.risk.max_position_fraction == 0.5
        assert config.risk.fee_rate == 0.0005
        assert config.risk.slippage_bps == 5.0
        assert config.paper.history_candles == 2500
        assert config.paper.poll_seconds == 30
        assert config.paper.max_signal_delay_seconds == 120
        assert config.paper.max_ticker_age_seconds == 60
        assert config.shadow.mode == "observe"
        assert "research-v2-candles.db" not in config.data.database_path
        accounts.add(config.paper.account_name)
        databases.add(config.data.database_path)
    assert len(accounts) == 4
    assert len(databases) == 4

    manifest = json.loads((TEMPLATE_DIR / "manifest.json").read_text())
    assert manifest["strategy_policy_sha256"] == c2.strategy_policy_sha256()
    assert manifest["strategy_policy"]["paper_schema_version"] == 8
    assert manifest["strategy_policy"]["paper_strategy_version"] == 5
    assert manifest["activation"]["t0"] is None
    assert (
        manifest["activation"]["t0_policy"]
        == "first_successful_initialization_is_t0"
    )


def _app_home(tmp_path: Path, name: str = "c2-btc") -> Path:
    home = tmp_path / name
    home.mkdir()
    return home


def test_installed_render_is_immutable_and_copy_safe_verify_works(
    tmp_path: Path,
) -> None:
    home = _app_home(tmp_path)
    output = home / "config" / "paper.toml"
    created = c2.render_installed_bundle("KRW-BTC", home, output)
    assert created["config_result"] == "created"
    assert created["manifest_result"] == "created"
    assert created["started_services"] == 0
    assert stat.S_IMODE(output.stat().st_mode) == 0o600
    manifest_path = output.with_name("paper.manifest.json")
    assert stat.S_IMODE(manifest_path.stat().st_mode) == 0o600

    unchanged = c2.render_installed_bundle("KRW-BTC", home, output)
    assert unchanged["config_result"] == "unchanged"
    assert unchanged["manifest_result"] == "unchanged"

    package_root = Path(next(iter(coinpilot.__path__)))
    verified = c2.verify_installed_runtime(
        config_path=output,
        manifest_path=manifest_path,
        installed_package_root=package_root,
    )
    assert verified["valid"] is True
    assert verified["paper_account_key"].startswith("paper-v8:c2-forward-v1-")
    assert verified["orders_sent"] == 0

    copied = home / "bin" / "coinpilot-c2-config"
    copied.parent.mkdir()
    shutil.copy2(Path(c2.__file__), copied)
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(REPO_ROOT / "src")
    completed = subprocess.run(
        [
            sys.executable,
            str(copied),
            "verify-installed",
            "--config",
            str(output),
            "--manifest",
            str(manifest_path),
            "--installed-package-root",
            str(package_root),
        ],
        cwd=home,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout)["valid"] is True


def test_render_refuses_escape_symlink_and_existing_drift(tmp_path: Path) -> None:
    home = _app_home(tmp_path)
    with pytest.raises(c2.C2ConfigError, match="output must be exactly"):
        c2.render_installed_bundle(
            "KRW-BTC",
            home,
            tmp_path / "outside" / "paper.toml",
        )

    config_dir = home / "config"
    outside = tmp_path / "outside"
    outside.mkdir()
    config_dir.symlink_to(outside, target_is_directory=True)
    with pytest.raises(c2.C2ConfigError, match="symlinked"):
        c2.render_installed_bundle(
            "KRW-BTC",
            home,
            config_dir / "paper.toml",
        )

    config_dir.unlink()
    c2.render_installed_bundle("KRW-BTC", home, config_dir / "paper.toml")
    config = config_dir / "paper.toml"
    config.write_text(config.read_text() + "# drift\n")
    with pytest.raises(c2.C2ConfigError, match="overwrite drifted"):
        c2.render_installed_bundle("KRW-BTC", home, config)


def test_suite_status_is_read_only_and_reports_uninitialized_cash(
    tmp_path: Path,
) -> None:
    root = tmp_path / "instances"
    root.mkdir()
    database_paths = []
    for market in c2.EXPECTED_MARKETS:
        home = _app_home(root, c2.INSTANCE_SLUGS[market])
        output = home / "config" / "paper.toml"
        c2.render_installed_bundle(market, home, output)
        database_paths.append(home / "data" / "coinpilot-c2.db")

    result = c2.suite_status(root)
    assert result["read_only"] is True
    assert result["portfolio"] == {
        "market_count": 4,
        "initial_cash": 10_000_000.0,
        "equity": 10_000_000.0,
        "pnl": 0.0,
        "return_pct": 0.0,
        "open_positions": 0,
        "fills": 0,
        "closed_trades": 0,
        "fees": 0.0,
        "slippage": 0.0,
    }
    assert all(
        account["account_status"] == "NOT_INITIALIZED"
        for account in result["accounts"]
    )
    assert result["live_order_routing"] is False
    assert result["orders_sent"] == 0
    assert all(not path.exists() for path in database_paths)
