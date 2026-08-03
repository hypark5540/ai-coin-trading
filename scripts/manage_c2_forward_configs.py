#!/usr/bin/env python3
"""Render, validate, and inspect the frozen four-market C2 paper suite.

This tool only manages Coinpilot's public-feed paper configuration.  It has no
exchange credential fields, no private API client, and no live-order action.
Rendering is immutable: an existing file is accepted only when its bytes are
already identical to the frozen bundle.
"""

from __future__ import annotations

import argparse
import dataclasses
import fcntl
import hashlib
import importlib.util
import json
import math
import os
import sqlite3
import stat
import tempfile
import tomllib
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

from coinpilot.config import (
    AppConfig,
    DataConfig,
    ModelConfig,
    OperationsConfig,
    OutputConfig,
    PaperConfig,
    RiskConfig,
    ShadowConfig,
    load_config,
)
from coinpilot.paper import (
    PAPER_SCHEMA_VERSION,
    PAPER_STRATEGY_VERSION,
    paper_account_key,
    paper_config_fingerprint,
)


SUITE_SCHEMA_VERSION = 1
SUITE_ID = "c2-forward-paper-v1"
STRATEGY_ID = "C2_TREND_336_168"
REQUESTED_AT = "2026-07-21T19:16:56+09:00"
EXPECTED_MARKETS = ("KRW-BTC", "KRW-ETH", "KRW-XRP", "KRW-SOL")
MARKET_SLUGS = {
    "KRW-BTC": "btc",
    "KRW-ETH": "eth",
    "KRW-XRP": "xrp",
    "KRW-SOL": "sol",
}
INSTANCE_SLUGS = {
    "KRW-BTC": "c2-btc",
    "KRW-ETH": "c2-eth",
    "KRW-XRP": "c2-xrp",
    "KRW-SOL": "c2-sol",
}
OPERATIONS_PORTS = {
    "KRW-BTC": 8768,
    "KRW-ETH": 8769,
    "KRW-XRP": 8770,
    "KRW-SOL": 8771,
}
SECTION_ORDER = (
    "data",
    "model",
    "risk",
    "paper",
    "output",
    "shadow",
    "operations",
)
SOURCE_TREE_EXTRA_FILES = ("pyproject.toml",)
FORBIDDEN_CONFIG_KEYS = frozenset(
    {
        "access_key",
        "access_key_id",
        "api_key",
        "api_secret",
        "private_key",
        "secret_key",
        "webhook_url",
    }
)


class C2ConfigError(ValueError):
    """Raised when a C2 forward-paper bundle is unsafe or has drifted."""


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _json_document(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    ) + "\n"


def _strategy_policy() -> dict[str, Any]:
    """Return the market/path-independent frozen policy."""

    return {
        "suite_id": SUITE_ID,
        "strategy_id": STRATEGY_ID,
        "universe": list(EXPECTED_MARKETS),
        "sleeve_weight": 0.25,
        "data": {
            "interval_minutes": 60,
            "candle_count": 2500,
            "api_base_url": "https://api.upbit.com",
            "request_timeout_seconds": 10.0,
            "source": "public_upbit_quotation_api",
        },
        "model": dataclasses.asdict(
            ModelConfig(
                signal_mode="trend_breakout",
                horizon_bars=336,
                min_train_samples=500,
                train_window=1500,
                retrain_every=24,
                entry_probability=0.58,
                exit_probability=0.48,
                minimum_edge_pct=0.001,
                l2=0.01,
                learning_rate=0.08,
                max_iterations=250,
                calibration_window=240,
                calibration_min_samples=120,
                calibration_uncertainty_z=1.0,
                target_clip_quantile=0.01,
                regime_sma_window=168,
                regime_sma_gap_min=-1.0,
                breakout_entry_window=336,
                breakout_exit_window=168,
            )
        ),
        "risk": dataclasses.asdict(
            RiskConfig(
                initial_cash=2_500_000.0,
                risk_per_trade=0.0075,
                max_position_fraction=0.50,
                atr_stop_multiple=2.0,
                minimum_stop_pct=0.01,
                maximum_stop_pct=0.08,
                trailing_stop_pct=0.08,
                max_strategy_drawdown_pct=0.15,
                fee_rate=0.0005,
                slippage_bps=5.0,
                cooldown_bars=6,
                minimum_order_quote=5_000.0,
            )
        ),
        "paper": {
            "history_candles": 2500,
            "poll_seconds": 30,
            "max_signal_delay_seconds": 120,
            "max_ticker_age_seconds": 60,
            "execution": "fresh_public_ticker_after_closed_hourly_bar",
            "initialization": "prime_without_trade",
        },
        "costs": {
            "fee_rate_per_side": 0.0005,
            "slippage_bps_per_side": 5.0,
            "modeled_round_trip_cost": 0.002,
        },
        "safety": {
            "mode": "forward_paper_simulation",
            "public_only": True,
            "live_order_routing": False,
            "orders_sent": 0,
            "credentials_required": False,
        },
        "paper_schema_version": PAPER_SCHEMA_VERSION,
        "paper_strategy_version": PAPER_STRATEGY_VERSION,
        "t0_policy": "first_successful_initialization_is_t0",
    }


def strategy_policy_sha256() -> str:
    return _sha256_bytes(_canonical_json(_strategy_policy()))


def _activation_policy() -> dict[str, Any]:
    return {
        "requested_at": REQUESTED_AT,
        "t0": None,
        "t0_policy": "first_successful_initialization_is_t0",
        "first_cycle_policy": "prime_without_trade",
        "first_cycle_required_fills": 0,
        "activation_receipt_file": "paper.activation.json",
        "activation_receipt_required": True,
    }


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _source_tree_files() -> tuple[Path, ...]:
    root = _repo_root()
    source_files = tuple(sorted((root / "src" / "coinpilot").rglob("*.py")))
    extras = tuple(root / name for name in SOURCE_TREE_EXTRA_FILES)
    files = source_files + extras
    if not files or any(not path.is_file() for path in files):
        raise C2ConfigError("Cannot fingerprint the complete Coinpilot source tree")
    return files


def source_tree_manifest() -> dict[str, Any]:
    root = _repo_root()
    aggregate = hashlib.sha256()
    files = _source_tree_files()
    for path in files:
        relative = path.relative_to(root).as_posix()
        content = path.read_bytes()
        aggregate.update(relative.encode("utf-8"))
        aggregate.update(b"\0")
        aggregate.update(content)
        aggregate.update(b"\0")
    return {
        "aggregate_sha256": aggregate.hexdigest(),
        "file_count": len(files),
        "scope": "pyproject.toml + sorted src/coinpilot/**/*.py",
    }


def _default_installed_package_root() -> Path:
    spec = importlib.util.find_spec("coinpilot")
    if spec is None or not spec.submodule_search_locations:
        raise C2ConfigError("Cannot locate the installed coinpilot package")
    return _absolute_path(Path(next(iter(spec.submodule_search_locations))))


def installed_package_manifest(package_root: Path | None = None) -> dict[str, Any]:
    root = _absolute_path(
        _default_installed_package_root() if package_root is None else package_root
    )
    _reject_symlink_components(root)
    if not root.is_dir() or root.name != "coinpilot":
        raise C2ConfigError(
            "installed package root must be an existing coinpilot directory"
        )
    files = tuple(sorted(root.rglob("*.py")))
    if not files:
        raise C2ConfigError("installed coinpilot package has no Python source files")
    aggregate = hashlib.sha256()
    for path in files:
        if path.is_symlink() or not path.is_file():
            raise C2ConfigError(f"unsafe installed package source: {path}")
        relative = path.relative_to(root).as_posix()
        aggregate.update(relative.encode("utf-8"))
        aggregate.update(b"\0")
        aggregate.update(path.read_bytes())
        aggregate.update(b"\0")
    return {
        "aggregate_sha256": aggregate.hexdigest(),
        "file_count": len(files),
        "scope": "sorted installed coinpilot/**/*.py",
    }


def _account_name(market: str) -> str:
    return f"c2-forward-v1-{market.lower()}"


def _validate_market(market: str) -> str:
    if market not in EXPECTED_MARKETS:
        raise C2ConfigError(
            f"market must be one of {', '.join(EXPECTED_MARKETS)}"
        )
    return market


def _config_for_paths(
    market: str,
    *,
    database_path: str,
    artifacts_dir: str,
    shadow_database_path: str,
    archive_root: str,
    backup_dir: str,
) -> AppConfig:
    _validate_market(market)
    policy = _strategy_policy()
    model = ModelConfig(**policy["model"])
    risk = RiskConfig(**policy["risk"])
    paper_policy = policy["paper"]
    config = AppConfig(
        data=DataConfig(
            market=market,
            interval_minutes=policy["data"]["interval_minutes"],
            candle_count=policy["data"]["candle_count"],
            api_base_url=policy["data"]["api_base_url"],
            request_timeout_seconds=policy["data"][
                "request_timeout_seconds"
            ],
            database_path=database_path,
        ),
        model=model,
        risk=risk,
        paper=PaperConfig(
            history_candles=paper_policy["history_candles"],
            poll_seconds=paper_policy["poll_seconds"],
            max_signal_delay_seconds=paper_policy[
                "max_signal_delay_seconds"
            ],
            max_ticker_age_seconds=paper_policy["max_ticker_age_seconds"],
            account_name=_account_name(market),
        ),
        output=OutputConfig(artifacts_dir=artifacts_dir),
        # This section is unused by the paper service.  Keeping it explicitly
        # observe-only makes accidental shadow invocation fail safe.
        shadow=ShadowConfig(
            mode="observe",
            database_path=shadow_database_path,
            archive_root=archive_root,
            initial_cash=2_500_000.0,
            order_quote=125_000.0,
            fee_rate=0.0005,
            latency_ms=100.0,
            max_book_gap_ms=1_000.0,
            warmup_books=100,
            trade_flow_window_ms=1_000,
            entry_threshold=0.45,
            exit_threshold=0.0,
            max_holding_seconds=60.0,
            max_spread_bps=10.0,
            max_daily_loss_pct=0.01,
            max_drawdown_pct=0.05,
            equity_sample_seconds=5,
            health_sample_seconds=10,
            partition_seconds=300,
            session_seconds=31_536_000.0,
            model_version="c2-paper-config-shadow-disabled-v1",
        ),
        operations=OperationsConfig(
            bind_host="127.0.0.1",
            port=OPERATIONS_PORTS[market],
            heartbeat_seconds=10,
            stale_after_seconds=60,
            notifier_poll_seconds=2,
            keychain_service="coinpilot-c2-paper-no-webhook",
            keychain_account=_account_name(market),
            backup_dir=backup_dir,
            archive_retention_days=90,
            disk_warning_pct=0.15,
            disk_halt_pct=0.05,
        ),
    )
    return config.validate()


def installed_config(market: str, app_home: Path) -> AppConfig:
    app_home = _absolute_path(app_home)
    return _config_for_paths(
        market,
        database_path=str(app_home / "data" / "coinpilot-c2.db"),
        artifacts_dir=str(app_home / "artifacts" / "c2"),
        shadow_database_path=str(
            app_home / "data" / "c2-paper-shadow-disabled.db"
        ),
        archive_root=str(app_home / "data" / "c2-paper-shadow-disabled"),
        backup_dir=str(app_home / "backups" / "c2-paper"),
    )


def template_config(market: str) -> AppConfig:
    slug = MARKET_SLUGS[_validate_market(market)]
    root = f"../../var/forward-paper-c2/{slug}"
    return _config_for_paths(
        market,
        database_path=f"{root}/coinpilot-c2.db",
        artifacts_dir=f"../../artifacts/forward-paper-c2/{slug}",
        shadow_database_path=f"{root}/c2-paper-shadow-disabled.db",
        archive_root=f"{root}/c2-paper-shadow-disabled",
        backup_dir=f"{root}/backups",
    )


def _toml_value(value: object) -> str:
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float) and math.isfinite(value):
        return repr(value)
    raise C2ConfigError(f"Unsupported TOML value: {value!r}")


def render_config(config: AppConfig) -> str:
    values = config.as_dict()
    lines = [
        "# Frozen C2 forward-paper configuration. No credentials or live orders.",
        f"# strategy_policy_sha256 = {strategy_policy_sha256()}",
        "",
    ]
    for section in SECTION_ORDER:
        lines.append(f"[{section}]")
        section_values = values[section]
        for key, value in section_values.items():
            lines.append(f"{key} = {_toml_value(value)}")
        lines.append("")
    return "\n".join(lines)


def _runtime_paths(config: AppConfig) -> dict[str, str]:
    return {
        "database_path": config.data.database_path,
        "artifacts_dir": config.output.artifacts_dir,
        "shadow_database_path": config.shadow.database_path,
        "shadow_archive_root": config.shadow.archive_root,
        "backup_dir": config.operations.backup_dir,
    }


def _market_manifest(
    config: AppConfig,
    config_text: str,
    *,
    config_file: str,
) -> dict[str, Any]:
    return {
        "market": config.data.market,
        "sleeve_weight": 0.25,
        "initial_cash": config.risk.initial_cash,
        "config_file": config_file,
        "config_sha256": _sha256_bytes(config_text.encode("utf-8")),
        "paper_account_name": config.paper.account_name,
        "paper_account_key": paper_account_key(config),
        "paper_config_fingerprint": paper_config_fingerprint(config),
        "runtime_paths": _runtime_paths(config),
    }


def installed_manifest(
    config: AppConfig,
    config_text: str,
    *,
    config_file: str = "paper.toml",
) -> dict[str, Any]:
    return {
        "schema_version": SUITE_SCHEMA_VERSION,
        "suite_id": SUITE_ID,
        "strategy_id": STRATEGY_ID,
        "strategy_policy_sha256": strategy_policy_sha256(),
        "strategy_policy": _strategy_policy(),
        "source_tree": source_tree_manifest(),
        "installed_package": installed_package_manifest(),
        "market_config": _market_manifest(
            config,
            config_text,
            config_file=config_file,
        ),
        "activation": _activation_policy(),
        "safety": {
            "public_only": True,
            "live_order_routing": False,
            "orders_sent": 0,
            "credential_fields": [],
        },
    }


def template_suite_manifest() -> dict[str, Any]:
    markets = []
    for market in EXPECTED_MARKETS:
        config = template_config(market)
        text = render_config(config)
        markets.append(
            _market_manifest(
                config,
                text,
                config_file=f"{MARKET_SLUGS[market]}.toml",
            )
        )
    return {
        "schema_version": SUITE_SCHEMA_VERSION,
        "suite_id": SUITE_ID,
        "strategy_id": STRATEGY_ID,
        "strategy_policy_sha256": strategy_policy_sha256(),
        "strategy_policy": _strategy_policy(),
        "source_tree": source_tree_manifest(),
        "markets": markets,
        "activation": _activation_policy(),
        "safety": {
            "public_only": True,
            "live_order_routing": False,
            "orders_sent": 0,
            "credential_fields": [],
        },
    }


def _absolute_path(path: Path) -> Path:
    return Path(os.path.abspath(path.expanduser()))


def _reject_symlink_components(path: Path) -> None:
    current = _absolute_path(path)
    while True:
        if current.is_symlink():
            raise C2ConfigError(f"symlinked managed path is not allowed: {current}")
        if current == current.parent:
            return
        current = current.parent


def _require_within(path: Path, root: Path) -> None:
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise C2ConfigError(f"managed path escapes app home: {path}") from exc


def _validate_app_home(app_home: Path) -> Path:
    absolute = _absolute_path(app_home)
    _reject_symlink_components(absolute)
    if not absolute.is_dir():
        raise C2ConfigError(f"app home must already exist: {absolute}")
    return absolute


def _installed_paths(app_home: Path, output: Path) -> tuple[Path, Path]:
    home = _validate_app_home(app_home)
    config_path = _absolute_path(output)
    expected = home / "config" / "paper.toml"
    if config_path != expected:
        raise C2ConfigError(f"output must be exactly {expected}")
    manifest_path = config_path.with_name("paper.manifest.json")
    for path in (config_path, manifest_path, config_path.parent):
        _require_within(path, home)
        _reject_symlink_components(path)
    return config_path, manifest_path


def _scan_forbidden_keys(value: object, *, prefix: str = "") -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            normalized = str(key).strip().lower()
            name = f"{prefix}.{key}" if prefix else str(key)
            if normalized in FORBIDDEN_CONFIG_KEYS:
                raise C2ConfigError(f"credential field is forbidden: {name}")
            _scan_forbidden_keys(item, prefix=name)
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _scan_forbidden_keys(item, prefix=f"{prefix}[{index}]")


def _check_owner_only(path: Path) -> None:
    mode = stat.S_IMODE(path.stat().st_mode)
    if mode & 0o077:
        raise C2ConfigError(f"managed file must be owner-only: {path}")


def _validate_loaded_config(actual: AppConfig, expected: AppConfig) -> None:
    if actual.as_dict() != expected.as_dict():
        raise C2ConfigError("loaded C2 configuration differs from frozen policy")
    if actual.data.api_base_url != "https://api.upbit.com":
        raise C2ConfigError("C2 paper must use the public Upbit API endpoint")
    if actual.shadow.mode != "observe":
        raise C2ConfigError("paper companion shadow section must stay observe-only")
    if paper_config_fingerprint(actual) != paper_config_fingerprint(expected):
        raise C2ConfigError("paper configuration fingerprint drift")


def validate_installed_bundle(
    market: str,
    *,
    app_home: Path,
    output: Path,
    require_owner_only: bool = True,
) -> tuple[AppConfig, dict[str, Any]]:
    config_path, manifest_path = _installed_paths(app_home, output)
    if not config_path.is_file() or not manifest_path.is_file():
        raise C2ConfigError(
            f"C2 config bundle is incomplete: {config_path}, {manifest_path}"
        )
    if require_owner_only:
        _check_owner_only(config_path)
        _check_owner_only(manifest_path)
    expected_config = installed_config(market, _absolute_path(app_home))
    expected_text = render_config(expected_config)
    actual_text = config_path.read_text(encoding="utf-8")
    if actual_text != expected_text:
        raise C2ConfigError(f"immutable C2 config drift: {config_path}")
    raw = tomllib.loads(actual_text)
    _scan_forbidden_keys(raw)
    loaded = load_config(config_path)
    _validate_loaded_config(loaded, expected_config)
    expected_manifest = installed_manifest(
        expected_config,
        expected_text,
        config_file=config_path.name,
    )
    try:
        actual_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise C2ConfigError(f"invalid C2 manifest: {manifest_path}") from exc
    if actual_manifest != expected_manifest:
        raise C2ConfigError(f"immutable C2 manifest drift: {manifest_path}")
    return loaded, actual_manifest


def _validate_source_tree_receipt(value: object) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {
        "aggregate_sha256",
        "file_count",
        "scope",
    }:
        raise C2ConfigError("source_tree deployment receipt has invalid fields")
    digest = value["aggregate_sha256"]
    if (
        not isinstance(digest, str)
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
    ):
        raise C2ConfigError("source_tree aggregate_sha256 is invalid")
    if (
        isinstance(value["file_count"], bool)
        or not isinstance(value["file_count"], int)
        or value["file_count"] < 1
    ):
        raise C2ConfigError("source_tree file_count is invalid")
    if value["scope"] != "pyproject.toml + sorted src/coinpilot/**/*.py":
        raise C2ConfigError("source_tree fingerprint scope is invalid")
    return value


def verify_installed_runtime(
    *,
    config_path: Path,
    manifest_path: Path,
    installed_package_root: Path,
) -> dict[str, Any]:
    """Verify an installed bundle without depending on the repository tree."""

    config_path = _absolute_path(config_path)
    manifest_path = _absolute_path(manifest_path)
    if config_path.name != "paper.toml" or manifest_path != config_path.with_name(
        "paper.manifest.json"
    ):
        raise C2ConfigError(
            "verify-installed requires adjacent paper.toml and paper.manifest.json"
        )
    app_home = config_path.parent.parent
    if config_path != app_home / "config" / "paper.toml":
        raise C2ConfigError("paper config must be APP_HOME/config/paper.toml")
    _installed_paths(app_home, config_path)
    for path in (config_path, manifest_path):
        if not path.is_file():
            raise C2ConfigError(f"installed C2 file is missing: {path}")
        _check_owner_only(path)
    config_text = config_path.read_text(encoding="utf-8")
    raw = tomllib.loads(config_text)
    _scan_forbidden_keys(raw)
    data = raw.get("data")
    market = data.get("market") if isinstance(data, dict) else None
    if not isinstance(market, str):
        raise C2ConfigError("installed config has no data.market")
    _validate_market(market)
    expected_config = installed_config(market, app_home)
    if config_text != render_config(expected_config):
        raise C2ConfigError("installed C2 config bytes differ from frozen policy")
    loaded = load_config(config_path)
    _validate_loaded_config(loaded, expected_config)
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise C2ConfigError("installed C2 manifest is invalid JSON") from exc
    if not isinstance(manifest, dict):
        raise C2ConfigError("installed C2 manifest must be one JSON object")
    source_receipt = _validate_source_tree_receipt(manifest.get("source_tree"))
    expected_manifest = {
        "schema_version": SUITE_SCHEMA_VERSION,
        "suite_id": SUITE_ID,
        "strategy_id": STRATEGY_ID,
        "strategy_policy_sha256": strategy_policy_sha256(),
        "strategy_policy": _strategy_policy(),
        "source_tree": source_receipt,
        "installed_package": installed_package_manifest(installed_package_root),
        "market_config": _market_manifest(
            expected_config,
            config_text,
            config_file=config_path.name,
        ),
        "activation": _activation_policy(),
        "safety": {
            "public_only": True,
            "live_order_routing": False,
            "orders_sent": 0,
            "credential_fields": [],
        },
    }
    if manifest != expected_manifest:
        raise C2ConfigError("installed C2 manifest or package fingerprint drift")
    return {
        "market": market,
        "config": str(config_path),
        "manifest": str(manifest_path),
        "strategy_policy_sha256": strategy_policy_sha256(),
        "source_tree_sha256": source_receipt["aggregate_sha256"],
        "installed_package_sha256": expected_manifest["installed_package"][
            "aggregate_sha256"
        ],
        "paper_account_key": paper_account_key(loaded),
        "paper_config_fingerprint": paper_config_fingerprint(loaded),
        "public_only": True,
        "live_order_routing": False,
        "orders_sent": 0,
        "valid": True,
    }


@contextmanager
def _bundle_lock(directory: Path) -> Iterator[None]:
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    _reject_symlink_components(directory)
    lock_path = directory / ".c2-forward-config.lock"
    descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    os.chmod(lock_path, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _write_immutable(path: Path, text: str) -> str:
    encoded = text.encode("utf-8")
    if path.exists():
        if path.is_symlink() or not path.is_file():
            raise C2ConfigError(f"managed output is not a regular file: {path}")
        if path.read_bytes() != encoded:
            raise C2ConfigError(f"refusing to overwrite drifted file: {path}")
        _check_owner_only(path)
        return "unchanged"
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()
    return "created"


def render_installed_bundle(market: str, app_home: Path, output: Path) -> dict[str, Any]:
    config_path, manifest_path = _installed_paths(app_home, output)
    config = installed_config(market, _absolute_path(app_home))
    config_text = render_config(config)
    manifest_text = _json_document(
        installed_manifest(config, config_text, config_file=config_path.name)
    )
    with _bundle_lock(config_path.parent):
        config_result = _write_immutable(config_path, config_text)
        manifest_result = _write_immutable(manifest_path, manifest_text)
    validate_installed_bundle(
        market,
        app_home=app_home,
        output=output,
    )
    return {
        "market": market,
        "config": str(config_path),
        "config_result": config_result,
        "manifest": str(manifest_path),
        "manifest_result": manifest_result,
        "strategy_policy_sha256": strategy_policy_sha256(),
        "paper_account_key": paper_account_key(config),
        "paper_config_fingerprint": paper_config_fingerprint(config),
        "public_only": True,
        "live_order_routing": False,
        "orders_sent": 0,
        "started_services": 0,
    }


def _template_documents() -> dict[str, str]:
    documents: dict[str, str] = {}
    for market in EXPECTED_MARKETS:
        documents[f"{MARKET_SLUGS[market]}.toml"] = render_config(
            template_config(market)
        )
    documents["manifest.json"] = _json_document(template_suite_manifest())
    return documents


def render_template_suite(directory: Path) -> dict[str, Any]:
    output_dir = _absolute_path(directory)
    _reject_symlink_components(output_dir)
    documents = _template_documents()
    results: dict[str, str] = {}
    with _bundle_lock(output_dir):
        for name, text in documents.items():
            results[name] = _write_immutable(output_dir / name, text)
    validate_template_suite(output_dir, require_owner_only=True)
    return {
        "directory": str(output_dir),
        "files": results,
        "strategy_policy_sha256": strategy_policy_sha256(),
        "public_only": True,
        "live_order_routing": False,
        "orders_sent": 0,
    }


def validate_template_suite(
    directory: Path,
    *,
    require_owner_only: bool = False,
) -> dict[str, Any]:
    output_dir = _absolute_path(directory)
    _reject_symlink_components(output_dir)
    expected = _template_documents()
    for name, text in expected.items():
        path = output_dir / name
        _reject_symlink_components(path)
        if not path.is_file():
            raise C2ConfigError(f"C2 template file is missing: {path}")
        if require_owner_only:
            _check_owner_only(path)
        if path.read_text(encoding="utf-8") != text:
            raise C2ConfigError(f"immutable C2 template drift: {path}")
    for market in EXPECTED_MARKETS:
        name = f"{MARKET_SLUGS[market]}.toml"
        path = output_dir / name
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
        _scan_forbidden_keys(raw)
        loaded = load_config(path)
        expected_config = template_config(market)
        # Relative paths become absolute during loading.  Policy and account
        # fingerprints deliberately exclude local runtime paths.
        if paper_config_fingerprint(loaded) != paper_config_fingerprint(
            expected_config
        ):
            raise C2ConfigError(f"template paper fingerprint drift: {path}")
        comparable = loaded.as_dict()
        expected_values = expected_config.as_dict()
        for section in ("model", "risk", "paper"):
            if comparable[section] != expected_values[section]:
                raise C2ConfigError(f"template policy drift in [{section}]: {path}")
        if loaded.data.market != market or loaded.data.api_base_url != (
            "https://api.upbit.com"
        ):
            raise C2ConfigError(f"template market/API drift: {path}")
        if loaded.shadow.mode != "observe":
            raise C2ConfigError(f"template shadow mode drift: {path}")
    return {
        "directory": str(output_dir),
        "markets": list(EXPECTED_MARKETS),
        "strategy_policy_sha256": strategy_policy_sha256(),
        "source_tree_sha256": source_tree_manifest()["aggregate_sha256"],
        "public_only": True,
        "live_order_routing": False,
        "orders_sent": 0,
    }


def _read_paper_database(config: AppConfig) -> dict[str, Any]:
    path = Path(config.data.database_path)
    _reject_symlink_components(path)
    initial_cash = float(config.risk.initial_cash)
    empty = {
        "account_status": "NOT_INITIALIZED",
        "cash": initial_cash,
        "quantity": 0.0,
        "position": "CASH",
        "mark_price": None,
        "mark_source": None,
        "equity": initial_cash,
        "pnl": 0.0,
        "return_pct": 0.0,
        "realized_pnl": 0.0,
        "fill_count": 0,
        "closed_trade_count": 0,
        "total_fees": 0.0,
        "total_slippage": 0.0,
        "last_bar_time": None,
        "updated_at": None,
        "latest_event": None,
    }
    if not path.exists():
        return empty
    if not path.is_file():
        raise C2ConfigError(f"paper database is not a regular file: {path}")
    connection = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA query_only = ON")
        tables = {
            str(row["name"])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        required = {"paper_state", "paper_events", "candles"}
        if not required.issubset(tables):
            raise C2ConfigError(f"paper database schema is incomplete: {path}")
        account_key = paper_account_key(config)
        row = connection.execute(
            "SELECT state_json FROM paper_state WHERE account_key = ?",
            (account_key,),
        ).fetchone()
        if row is None:
            return empty
        state = json.loads(row["state_json"])
        if state.get("market") != config.data.market:
            raise C2ConfigError("persisted paper market does not match configuration")
        if state.get("config_fingerprint") != paper_config_fingerprint(config):
            raise C2ConfigError("persisted paper fingerprint does not match configuration")
        if int(state.get("schema_version", -1)) != PAPER_SCHEMA_VERSION:
            raise C2ConfigError("persisted paper schema version is unsupported")
        mark_row = connection.execute(
            """
            SELECT timestamp, close
            FROM candles
            WHERE market = ? AND interval_minutes = ?
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (config.data.market, config.data.interval_minutes),
        ).fetchone()
        quantity = float(state.get("quantity", 0.0))
        cash = float(state.get("cash", 0.0))
        mark_price: float | None = None
        mark_source: str | None = None
        if mark_row is not None:
            mark_price = float(mark_row["close"])
            mark_source = "latest_cached_closed_candle"
        elif quantity > 0:
            mark_price = float(state.get("entry_price", 0.0))
            mark_source = "entry_fill_fallback"
        if quantity > 0 and mark_price is not None:
            liquidation_price = mark_price * (1 - config.risk.slippage_bps / 10_000)
            equity = cash + quantity * liquidation_price * (1 - config.risk.fee_rate)
        else:
            equity = cash
        fill_rows = connection.execute(
            """
            SELECT payload_json
            FROM paper_events
            WHERE account_key = ? AND event_type = 'fill'
            ORDER BY timestamp ASC, rowid ASC
            """,
            (account_key,),
        ).fetchall()
        fill_payloads = [json.loads(item["payload_json"]) for item in fill_rows]
        latest = connection.execute(
            """
            SELECT timestamp, event_type, payload_json
            FROM paper_events
            WHERE account_key = ?
            ORDER BY timestamp DESC, rowid DESC
            LIMIT 1
            """,
            (account_key,),
        ).fetchone()
        latest_event = (
            {
                "timestamp": latest["timestamp"],
                "event_type": latest["event_type"],
                "payload": json.loads(latest["payload_json"]),
            }
            if latest is not None
            else None
        )
        pnl = equity - initial_cash
        return {
            "account_status": str(state.get("halt_state", "UNKNOWN")),
            "cash": cash,
            "quantity": quantity,
            "position": "LONG" if quantity > 0 else "CASH",
            "mark_price": mark_price,
            "mark_source": mark_source,
            "equity": equity,
            "pnl": pnl,
            "return_pct": pnl / initial_cash,
            "realized_pnl": float(state.get("realized_pnl", 0.0)),
            "fill_count": len(fill_payloads),
            "closed_trade_count": sum(
                str(item.get("side")) == "sell" for item in fill_payloads
            ),
            "total_fees": sum(float(item.get("fee", 0.0)) for item in fill_payloads),
            "total_slippage": sum(
                float(item.get("slippage_cost", 0.0)) for item in fill_payloads
            ),
            "last_bar_time": state.get("last_bar_time"),
            "updated_at": state.get("updated_at"),
            "latest_event": latest_event,
        }
    finally:
        connection.close()


def suite_status(app_home_root: Path) -> dict[str, Any]:
    root = _validate_app_home(app_home_root)
    accounts: list[dict[str, Any]] = []
    for market in EXPECTED_MARKETS:
        app_home = root / INSTANCE_SLUGS[market]
        output = app_home / "config" / "paper.toml"
        config, manifest = validate_installed_bundle(
            market,
            app_home=app_home,
            output=output,
        )
        status = _read_paper_database(config)
        accounts.append(
            {
                "market": market,
                "app_home": str(app_home),
                "account_key": paper_account_key(config),
                "paper_config_fingerprint": paper_config_fingerprint(config),
                "config_sha256": manifest["market_config"]["config_sha256"],
                "initial_cash": config.risk.initial_cash,
                **status,
                "public_only": True,
                "live_order_routing": False,
                "orders_sent": 0,
            }
        )
    initial_cash = sum(float(item["initial_cash"]) for item in accounts)
    equity = sum(float(item["equity"]) for item in accounts)
    pnl = equity - initial_cash
    return {
        "schema_version": 1,
        "suite_id": SUITE_ID,
        "strategy_id": STRATEGY_ID,
        "strategy_policy_sha256": strategy_policy_sha256(),
        "requested_at": REQUESTED_AT,
        "t0_policy": "first_successful_initialization_is_t0",
        "accounts": accounts,
        "portfolio": {
            "market_count": len(accounts),
            "initial_cash": initial_cash,
            "equity": equity,
            "pnl": pnl,
            "return_pct": pnl / initial_cash,
            "open_positions": sum(item["position"] == "LONG" for item in accounts),
            "fills": sum(int(item["fill_count"]) for item in accounts),
            "closed_trades": sum(
                int(item["closed_trade_count"]) for item in accounts
            ),
            "fees": sum(float(item["total_fees"]) for item in accounts),
            "slippage": sum(
                float(item["total_slippage"]) for item in accounts
            ),
        },
        "public_only": True,
        "live_order_routing": False,
        "orders_sent": 0,
        "read_only": True,
    }


def _add_installed_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--market", required=True, choices=EXPECTED_MARKETS)
    parser.add_argument("--app-home", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Manage frozen, public-only C2 forward-paper configurations"
    )
    commands = parser.add_subparsers(dest="command", required=True)

    render = commands.add_parser("render", help="immutably render one installed config")
    _add_installed_arguments(render)

    check = commands.add_parser("check", help="validate one installed config")
    _add_installed_arguments(check)

    verify_installed = commands.add_parser(
        "verify-installed",
        help="verify config, manifest, and installed package without repo access",
    )
    verify_installed.add_argument("--config", type=Path, required=True)
    verify_installed.add_argument("--manifest", type=Path, required=True)
    verify_installed.add_argument(
        "--installed-package-root",
        type=Path,
        required=True,
        help="installed site-packages/coinpilot directory",
    )

    render_templates = commands.add_parser(
        "render-templates", help="immutably render the four tracked templates"
    )
    render_templates.add_argument("--output-dir", type=Path, required=True)

    check_templates = commands.add_parser(
        "check-templates", help="validate the four tracked templates"
    )
    check_templates.add_argument("--output-dir", type=Path, required=True)

    status = commands.add_parser(
        "status", help="read four instance paper ledgers without modifying them"
    )
    status.add_argument(
        "--app-home-root",
        type=Path,
        required=True,
        help="directory containing c2-btc, c2-eth, c2-xrp, and c2-sol app homes",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "render":
            payload = render_installed_bundle(args.market, args.app_home, args.output)
        elif args.command == "check":
            config, manifest = validate_installed_bundle(
                args.market,
                app_home=args.app_home,
                output=args.output,
            )
            payload = {
                "market": args.market,
                "config": str(_absolute_path(args.output)),
                "strategy_policy_sha256": manifest["strategy_policy_sha256"],
                "source_tree_sha256": manifest["source_tree"]["aggregate_sha256"],
                "paper_account_key": paper_account_key(config),
                "paper_config_fingerprint": paper_config_fingerprint(config),
                "public_only": True,
                "live_order_routing": False,
                "orders_sent": 0,
                "valid": True,
            }
        elif args.command == "verify-installed":
            payload = verify_installed_runtime(
                config_path=args.config,
                manifest_path=args.manifest,
                installed_package_root=args.installed_package_root,
            )
        elif args.command == "render-templates":
            payload = render_template_suite(args.output_dir)
        elif args.command == "check-templates":
            payload = validate_template_suite(args.output_dir)
        elif args.command == "status":
            payload = suite_status(args.app_home_root)
        else:  # pragma: no cover - argparse guarantees a known command
            raise C2ConfigError(f"unsupported command: {args.command}")
    except (C2ConfigError, OSError, sqlite3.Error, ValueError) as exc:
        print(
            _json_document(
                {
                    "error": type(exc).__name__,
                    "message": str(exc),
                    "public_only": True,
                    "live_order_routing": False,
                    "orders_sent": 0,
                }
            ),
            end="",
            file=os.sys.stderr,
        )
        return 2
    print(_json_document(payload), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
