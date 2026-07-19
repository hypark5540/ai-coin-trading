from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest

from coinpilot.backtest import with_scaled_costs
from coinpilot.config import AppConfig, load_config


def test_default_config_is_valid() -> None:
    config = load_config()
    assert config.data.market == "KRW-BTC"
    assert config.round_trip_cost == pytest.approx(0.002)
    assert config.shadow.mode == "observe"
    assert config.operations.bind_host == "127.0.0.1"


def test_unknown_configuration_key_is_rejected(tmp_path) -> None:
    path = tmp_path / "bad.toml"
    path.write_text("[data]\nunknown = 1\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Unknown keys"):
        load_config(path)


def test_configuration_section_must_be_a_table(tmp_path) -> None:
    path = tmp_path / "bad-section.toml"
    path.write_text("data = 123\n", encoding="utf-8")
    with pytest.raises(ValueError, match="must be a table"):
        load_config(path)


def test_live_risk_bounds_cannot_be_invalid() -> None:
    config = AppConfig()
    invalid = dataclasses.replace(
        config, risk=dataclasses.replace(config.risk, max_position_fraction=1.5)
    )
    with pytest.raises(ValueError, match="max_position_fraction"):
        invalid.validate()


@pytest.mark.parametrize(
    ("section", "field", "value"),
    [
        ("data", "interval_minutes", True),
        ("model", "max_iterations", 10.5),
        ("risk", "fee_rate", float("nan")),
        ("risk", "slippage_bps", 10_000),
        ("risk", "initial_cash", float("inf")),
        ("output", "artifacts_dir", ""),
    ],
)
def test_nonfinite_or_wrongly_typed_values_are_rejected(
    section: str, field: str, value: object
) -> None:
    config = AppConfig()
    changed_section = dataclasses.replace(
        getattr(config, section), **{field: value}
    )
    invalid = dataclasses.replace(config, **{section: changed_section})
    with pytest.raises(ValueError):
        invalid.validate()


def test_relative_runtime_paths_are_resolved_from_config_directory(
    tmp_path,
) -> None:
    config_dir = tmp_path / "settings"
    config_dir.mkdir()
    path = config_dir / "coinpilot.toml"
    path.write_text(
        '[data]\ndatabase_path = "state/paper.db"\n'
        '[output]\nartifacts_dir = "reports"\n'
        '[shadow]\ndatabase_path = "state/shadow.db"\n'
        'archive_root = "state/archive"\n'
        '[operations]\nbackup_dir = "state/backups"\n',
        encoding="utf-8",
    )
    config = load_config(path)
    assert Path(config.data.database_path) == (
        config_dir / "state/paper.db"
    ).resolve()
    assert Path(config.output.artifacts_dir) == (config_dir / "reports").resolve()
    assert Path(config.shadow.database_path) == (
        config_dir / "state/shadow.db"
    ).resolve()
    assert Path(config.shadow.archive_root) == (
        config_dir / "state/archive"
    ).resolve()
    assert Path(config.operations.backup_dir) == (
        config_dir / "state/backups"
    ).resolve()


def test_shadow_cannot_bind_publicly_or_enable_live_mode() -> None:
    config = AppConfig()
    public = dataclasses.replace(
        config,
        operations=dataclasses.replace(config.operations, bind_host="0.0.0.0"),
    )
    with pytest.raises(ValueError, match="127.0.0.1"):
        public.validate()

    live = dataclasses.replace(
        config,
        shadow=dataclasses.replace(config.shadow, mode="live"),
    )
    with pytest.raises(ValueError, match="observe or diagnostic"):
        live.validate()


@pytest.mark.parametrize("database_path", [None, 123])
def test_database_path_must_be_path_like(database_path: object) -> None:
    config = AppConfig()
    invalid = dataclasses.replace(
        config,
        data=dataclasses.replace(config.data, database_path=database_path),
    )

    with pytest.raises(ValueError, match="database_path"):
        invalid.validate()


@pytest.mark.parametrize(
    "contents",
    [
        "[data]\ndatabase_path = 123\n",
        "[output]\nartifacts_dir = 123\n",
    ],
)
def test_toml_path_type_errors_are_reported_as_configuration_errors(
    tmp_path, contents: str
) -> None:
    path = tmp_path / "bad-path.toml"
    path.write_text(contents, encoding="utf-8")

    with pytest.raises(ValueError, match="paths must be strings"):
        load_config(path)


@pytest.mark.parametrize(
    "multiplier",
    [True, False, float("nan"), float("inf"), float("-inf")],
)
def test_scaled_cost_multiplier_rejects_bool_and_nonfinite_values(
    multiplier: object,
) -> None:
    with pytest.raises(ValueError):
        with_scaled_costs(AppConfig(), multiplier)
