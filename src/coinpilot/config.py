from __future__ import annotations

import dataclasses
import math
import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TypeVar


SUPPORTED_MINUTE_INTERVALS = (1, 3, 5, 10, 15, 30, 60, 240)
MARKET_PATTERN = re.compile(r"^[A-Z0-9]+-[A-Z0-9]+$")
FEATURE_WARMUP_BARS = 168
PROBABILITY_FEATURE_WARMUP_BARS = 72
PROBABILITY_SIGNAL_MODES = frozenset({"probability", "trend_breakout"})
SUPPORTED_SIGNAL_MODES = frozenset(
    {*PROBABILITY_SIGNAL_MODES, "expected_return"}
)


@dataclass(frozen=True, slots=True)
class DataConfig:
    market: str = "KRW-BTC"
    interval_minutes: int = 60
    candle_count: int = 2500
    api_base_url: str = "https://api.upbit.com"
    request_timeout_seconds: float = 10.0
    database_path: str = "var/coinpilot.db"


@dataclass(frozen=True, slots=True)
class ModelConfig:
    signal_mode: str = "probability"
    horizon_bars: int = 3
    min_train_samples: int = 500
    train_window: int = 1500
    retrain_every: int = 24
    entry_probability: float = 0.58
    exit_probability: float = 0.48
    minimum_edge_pct: float = 0.001
    l2: float = 0.01
    learning_rate: float = 0.08
    max_iterations: int = 250
    calibration_window: int = 240
    calibration_min_samples: int = 120
    calibration_uncertainty_z: float = 1.0
    target_clip_quantile: float = 0.01
    regime_sma_window: int = 168
    regime_sma_gap_min: float = -1.0
    breakout_entry_window: int = 336
    breakout_exit_window: int = 168


@dataclass(frozen=True, slots=True)
class RiskConfig:
    initial_cash: float = 10_000_000.0
    risk_per_trade: float = 0.005
    max_position_fraction: float = 0.25
    atr_stop_multiple: float = 2.0
    minimum_stop_pct: float = 0.01
    maximum_stop_pct: float = 0.08
    trailing_stop_pct: float = 0.06
    max_strategy_drawdown_pct: float = 0.10
    fee_rate: float = 0.0005
    slippage_bps: float = 5.0
    cooldown_bars: int = 6
    minimum_order_quote: float = 5_000.0


@dataclass(frozen=True, slots=True)
class PaperConfig:
    history_candles: int = 2500
    poll_seconds: int = 30
    max_signal_delay_seconds: int = 120
    max_ticker_age_seconds: int = 60
    account_name: str = "default"


@dataclass(frozen=True, slots=True)
class OutputConfig:
    artifacts_dir: str = "artifacts"


@dataclass(frozen=True, slots=True)
class ShadowConfig:
    """Public-feed-only shadow execution and risk limits."""

    mode: str = "observe"
    database_path: str = "var/shadow.db"
    archive_root: str = "var/hft/archive"
    initial_cash: float = 10_000_000.0
    order_quote: float = 250_000.0
    fee_rate: float = 0.0005
    latency_ms: float = 100.0
    max_book_gap_ms: float = 1_000.0
    warmup_books: int = 100
    trade_flow_window_ms: int = 1_000
    entry_threshold: float = 0.45
    exit_threshold: float = 0.0
    max_holding_seconds: float = 60.0
    max_spread_bps: float = 10.0
    max_daily_loss_pct: float = 0.01
    max_drawdown_pct: float = 0.05
    equity_sample_seconds: int = 5
    health_sample_seconds: int = 10
    partition_seconds: int = 300
    session_seconds: float = 31_536_000.0
    model_version: str = "diagnostic-imbalance-flow-v0"


@dataclass(frozen=True, slots=True)
class OperationsConfig:
    """Local-only health, Slack delivery, and retention settings."""

    bind_host: str = "127.0.0.1"
    port: int = 8765
    heartbeat_seconds: int = 10
    stale_after_seconds: int = 60
    notifier_poll_seconds: int = 2
    keychain_service: str = "coinpilot-slack-webhook"
    keychain_account: str = "coinpilot-shadow"
    backup_dir: str = "var/backups"
    archive_retention_days: int = 90
    disk_warning_pct: float = 0.15
    disk_halt_pct: float = 0.05


@dataclass(frozen=True, slots=True)
class AppConfig:
    data: DataConfig = field(default_factory=DataConfig)
    model: ModelConfig = field(default_factory=ModelConfig)
    risk: RiskConfig = field(default_factory=RiskConfig)
    paper: PaperConfig = field(default_factory=PaperConfig)
    output: OutputConfig = field(default_factory=OutputConfig)
    shadow: ShadowConfig = field(default_factory=ShadowConfig)
    operations: OperationsConfig = field(default_factory=OperationsConfig)

    def validate(self) -> "AppConfig":
        d, m, r, p = self.data, self.model, self.risk, self.paper
        shadow, operations = self.shadow, self.operations
        errors: list[str] = []

        def finite_number(value: object) -> bool:
            return (
                isinstance(value, (int, float))
                and not isinstance(value, bool)
                and math.isfinite(value)
            )

        if not isinstance(d.market, str) or not MARKET_PATTERN.fullmatch(d.market):
            errors.append("data.market must look like KRW-BTC")
        if (
            isinstance(d.interval_minutes, bool)
            or not isinstance(d.interval_minutes, int)
            or d.interval_minutes not in SUPPORTED_MINUTE_INTERVALS
        ):
            errors.append(
                f"data.interval_minutes must be one of {SUPPORTED_MINUTE_INTERVALS}"
            )
        if (
            isinstance(d.candle_count, bool)
            or not isinstance(d.candle_count, int)
            or d.candle_count < 200
        ):
            errors.append("data.candle_count must be at least 200")
        if (
            isinstance(d.request_timeout_seconds, bool)
            or not isinstance(d.request_timeout_seconds, (int, float))
            or not math.isfinite(d.request_timeout_seconds)
            or d.request_timeout_seconds <= 0
        ):
            errors.append("data.request_timeout_seconds must be positive")
        if not isinstance(d.api_base_url, str) or not d.api_base_url.startswith(
            "https://"
        ):
            errors.append("data.api_base_url must use https")
        if not isinstance(d.database_path, str) or not d.database_path.strip():
            errors.append("data.database_path must be a non-empty string")

        integer_model_fields = {
            "horizon_bars": m.horizon_bars,
            "min_train_samples": m.min_train_samples,
            "train_window": m.train_window,
            "retrain_every": m.retrain_every,
            "max_iterations": m.max_iterations,
            "calibration_window": m.calibration_window,
            "calibration_min_samples": m.calibration_min_samples,
            "regime_sma_window": m.regime_sma_window,
            "breakout_entry_window": m.breakout_entry_window,
            "breakout_exit_window": m.breakout_exit_window,
        }
        for name, value in integer_model_fields.items():
            if isinstance(value, bool) or not isinstance(value, int):
                errors.append(f"model.{name} must be an integer")
        if isinstance(m.horizon_bars, int) and m.horizon_bars < 1:
            errors.append("model.horizon_bars must be at least 1")
        if isinstance(m.min_train_samples, int) and m.min_train_samples < 100:
            errors.append("model.min_train_samples must be at least 100")
        if (
            isinstance(m.train_window, int)
            and isinstance(m.min_train_samples, int)
            and m.train_window < m.min_train_samples
        ):
            errors.append("model.train_window must be >= model.min_train_samples")
        if isinstance(m.retrain_every, int) and m.retrain_every < 1:
            errors.append("model.retrain_every must be at least 1")
        if m.signal_mode not in SUPPORTED_SIGNAL_MODES:
            errors.append(
                "model.signal_mode must be probability, expected_return, "
                "or trend_breakout"
            )
        if m.signal_mode == "trend_breakout" and d.interval_minutes != 60:
            errors.append(
                "trend_breakout requires data.interval_minutes to be 60"
            )
        if (
            isinstance(m.breakout_entry_window, int)
            and m.breakout_entry_window < 2
        ):
            errors.append("model.breakout_entry_window must be at least 2")
        if (
            isinstance(m.breakout_exit_window, int)
            and m.breakout_exit_window < 2
        ):
            errors.append("model.breakout_exit_window must be at least 2")
        if (
            isinstance(m.breakout_entry_window, int)
            and isinstance(m.breakout_exit_window, int)
            and m.breakout_exit_window >= m.breakout_entry_window
        ):
            errors.append(
                "model.breakout_exit_window must be below "
                "breakout_entry_window"
            )
        if (
            isinstance(m.calibration_window, int)
            and m.calibration_window < 60
        ):
            errors.append("model.calibration_window must be at least 60")
        if (
            isinstance(m.calibration_min_samples, int)
            and m.calibration_min_samples < 30
        ):
            errors.append("model.calibration_min_samples must be at least 30")
        if (
            isinstance(m.calibration_window, int)
            and isinstance(m.calibration_min_samples, int)
            and m.calibration_min_samples > m.calibration_window
        ):
            errors.append(
                "model.calibration_min_samples cannot exceed calibration_window"
            )
        if (
            isinstance(m.regime_sma_window, int)
            and m.regime_sma_window not in {168, 336}
        ):
            errors.append("model.regime_sma_window must be 168 or 336")
        if (
            m.signal_mode == "expected_return"
            and isinstance(m.train_window, int)
            and isinstance(m.min_train_samples, int)
            and isinstance(m.calibration_window, int)
            and isinstance(m.horizon_bars, int)
            and m.train_window
            < (
                m.min_train_samples
                + m.calibration_window
                + m.horizon_bars
            )
        ):
            errors.append(
                "expected_return model.train_window must cover min_train_samples, "
                "calibration_window, and horizon_bars"
            )
        finite_model_fields = {
            "entry_probability": m.entry_probability,
            "exit_probability": m.exit_probability,
            "minimum_edge_pct": m.minimum_edge_pct,
            "l2": m.l2,
            "learning_rate": m.learning_rate,
            "calibration_uncertainty_z": m.calibration_uncertainty_z,
            "target_clip_quantile": m.target_clip_quantile,
            "regime_sma_gap_min": m.regime_sma_gap_min,
        }
        for name, value in finite_model_fields.items():
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
            ):
                errors.append(f"model.{name} must be a finite number")
        if finite_number(m.entry_probability) and not 0.5 < m.entry_probability < 1.0:
            errors.append("model.entry_probability must be between 0.5 and 1")
        if (
            finite_number(m.exit_probability)
            and finite_number(m.entry_probability)
            and not 0.0 < m.exit_probability < m.entry_probability
        ):
            errors.append(
                "model.exit_probability must be positive and below entry_probability"
            )
        if finite_number(m.minimum_edge_pct) and m.minimum_edge_pct < 0:
            errors.append("model.minimum_edge_pct cannot be negative")
        if (
            finite_number(m.calibration_uncertainty_z)
            and m.calibration_uncertainty_z < 0
        ):
            errors.append("model.calibration_uncertainty_z cannot be negative")
        if (
            finite_number(m.target_clip_quantile)
            and not 0 <= m.target_clip_quantile < 0.10
        ):
            errors.append(
                "model.target_clip_quantile must be in [0, 0.10)"
            )
        if (
            (finite_number(m.l2) and m.l2 < 0)
            or (finite_number(m.learning_rate) and m.learning_rate <= 0)
            or (isinstance(m.max_iterations, int) and m.max_iterations < 10)
        ):
            errors.append("model optimizer settings are invalid")

        bounded_rates = {
            "risk_per_trade": r.risk_per_trade,
            "max_position_fraction": r.max_position_fraction,
            "minimum_stop_pct": r.minimum_stop_pct,
            "maximum_stop_pct": r.maximum_stop_pct,
            "trailing_stop_pct": r.trailing_stop_pct,
            "max_strategy_drawdown_pct": r.max_strategy_drawdown_pct,
        }
        for name, value in bounded_rates.items():
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or not 0 < value < 1
            ):
                errors.append(f"risk.{name} must be between 0 and 1")
        if (
            finite_number(r.minimum_stop_pct)
            and finite_number(r.maximum_stop_pct)
            and r.minimum_stop_pct > r.maximum_stop_pct
        ):
            errors.append("risk.minimum_stop_pct cannot exceed maximum_stop_pct")
        if (
            not isinstance(r.atr_stop_multiple, (int, float))
            or isinstance(r.atr_stop_multiple, bool)
            or not math.isfinite(r.atr_stop_multiple)
            or r.atr_stop_multiple <= 0
        ):
            errors.append("risk.atr_stop_multiple must be positive")
        if (
            not isinstance(r.fee_rate, (int, float))
            or isinstance(r.fee_rate, bool)
            or not math.isfinite(r.fee_rate)
            or not 0 <= r.fee_rate < 0.1
        ):
            errors.append("risk.fee_rate must be finite and between 0 and 0.1")
        if (
            not isinstance(r.slippage_bps, (int, float))
            or isinstance(r.slippage_bps, bool)
            or not math.isfinite(r.slippage_bps)
            or not 0 <= r.slippage_bps < 10_000
        ):
            errors.append(
                "risk.slippage_bps must be finite and below 10000"
            )
        if (
            isinstance(r.cooldown_bars, bool)
            or not isinstance(r.cooldown_bars, int)
            or r.cooldown_bars < 0
        ):
            errors.append("risk.cooldown_bars cannot be negative")
        if (
            not isinstance(r.initial_cash, (int, float))
            or isinstance(r.initial_cash, bool)
            or not math.isfinite(r.initial_cash)
            or r.initial_cash <= 0
            or not isinstance(r.minimum_order_quote, (int, float))
            or isinstance(r.minimum_order_quote, bool)
            or not math.isfinite(r.minimum_order_quote)
            or r.minimum_order_quote <= 0
        ):
            errors.append("risk cash values must be positive")

        if m.signal_mode == "trend_breakout":
            feature_warmup = (
                max(m.breakout_entry_window, m.breakout_exit_window)
                if isinstance(m.breakout_entry_window, int)
                and isinstance(m.breakout_exit_window, int)
                else 0
            )
            required_history = feature_warmup + 1
        else:
            feature_warmup = (
                max(FEATURE_WARMUP_BARS, m.regime_sma_window)
                if m.signal_mode == "expected_return"
                else PROBABILITY_FEATURE_WARMUP_BARS
            )
            required_history = (
                max(
                    m.min_train_samples + m.horizon_bars + 80,
                    m.train_window
                    + feature_warmup
                    + m.horizon_bars
                    + m.retrain_every
                    + 1,
                )
                if isinstance(m.min_train_samples, int)
                and isinstance(m.horizon_bars, int)
                and isinstance(m.train_window, int)
                and isinstance(m.retrain_every, int)
                else 0
            )
        if isinstance(d.candle_count, int) and d.candle_count < required_history:
            errors.append(
                f"data.candle_count must be at least {required_history} for this model"
            )
        if (
            isinstance(p.history_candles, bool)
            or not isinstance(p.history_candles, int)
            or p.history_candles < required_history
        ):
            errors.append(
                f"paper.history_candles must be at least {required_history}"
            )
        if (
            isinstance(p.poll_seconds, bool)
            or not isinstance(p.poll_seconds, int)
            or p.poll_seconds < 1
        ):
            errors.append("paper.poll_seconds must be positive")
        if (
            isinstance(p.max_signal_delay_seconds, bool)
            or not isinstance(p.max_signal_delay_seconds, int)
            or (
                isinstance(p.poll_seconds, int)
                and not isinstance(p.poll_seconds, bool)
                and p.max_signal_delay_seconds < p.poll_seconds
            )
        ):
            errors.append(
                "paper.max_signal_delay_seconds must be an integer "
                "greater than or equal to poll_seconds"
            )
        if (
            isinstance(p.max_ticker_age_seconds, bool)
            or not isinstance(p.max_ticker_age_seconds, int)
            or p.max_ticker_age_seconds < 1
        ):
            errors.append("paper.max_ticker_age_seconds must be a positive integer")
        if not isinstance(p.account_name, str) or not p.account_name.strip():
            errors.append("paper.account_name cannot be empty")
        if (
            not isinstance(self.output.artifacts_dir, str)
            or not self.output.artifacts_dir.strip()
        ):
            errors.append("output.artifacts_dir cannot be empty")

        if shadow.mode not in {"observe", "diagnostic"}:
            errors.append("shadow.mode must be observe or diagnostic")
        for name, value in (
            ("database_path", shadow.database_path),
            ("archive_root", shadow.archive_root),
            ("model_version", shadow.model_version),
        ):
            if not isinstance(value, str) or not value.strip():
                errors.append(f"shadow.{name} must be a non-empty string")
        for name, value in (
            ("initial_cash", shadow.initial_cash),
            ("order_quote", shadow.order_quote),
            ("max_book_gap_ms", shadow.max_book_gap_ms),
            ("max_holding_seconds", shadow.max_holding_seconds),
            ("max_spread_bps", shadow.max_spread_bps),
            ("session_seconds", shadow.session_seconds),
        ):
            if not finite_number(value) or float(value) <= 0:
                errors.append(f"shadow.{name} must be positive and finite")
        if (
            finite_number(shadow.order_quote)
            and finite_number(shadow.initial_cash)
            and shadow.order_quote > shadow.initial_cash
        ):
            errors.append("shadow.order_quote cannot exceed initial_cash")
        if not finite_number(shadow.latency_ms) or shadow.latency_ms < 0:
            errors.append("shadow.latency_ms must be non-negative and finite")
        if (
            not finite_number(shadow.fee_rate)
            or not 0 <= shadow.fee_rate < 0.1
        ):
            errors.append("shadow.fee_rate must be in [0, 0.1)")
        for name, value in (
            ("warmup_books", shadow.warmup_books),
            ("trade_flow_window_ms", shadow.trade_flow_window_ms),
            ("equity_sample_seconds", shadow.equity_sample_seconds),
            ("health_sample_seconds", shadow.health_sample_seconds),
            ("partition_seconds", shadow.partition_seconds),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                errors.append(f"shadow.{name} must be a positive integer")
        for name, value in (
            ("entry_threshold", shadow.entry_threshold),
            ("exit_threshold", shadow.exit_threshold),
        ):
            if not finite_number(value) or not -1 <= float(value) <= 1:
                errors.append(f"shadow.{name} must be in [-1, 1]")
        if (
            finite_number(shadow.entry_threshold)
            and finite_number(shadow.exit_threshold)
            and shadow.exit_threshold >= shadow.entry_threshold
        ):
            errors.append("shadow.exit_threshold must be below entry_threshold")
        for name, value in (
            ("max_daily_loss_pct", shadow.max_daily_loss_pct),
            ("max_drawdown_pct", shadow.max_drawdown_pct),
        ):
            if not finite_number(value) or not 0 < float(value) < 1:
                errors.append(f"shadow.{name} must be between 0 and 1")

        if operations.bind_host != "127.0.0.1":
            errors.append("operations.bind_host must be exactly 127.0.0.1")
        if (
            isinstance(operations.port, bool)
            or not isinstance(operations.port, int)
            or not 1024 <= operations.port <= 65_535
        ):
            errors.append("operations.port must be between 1024 and 65535")
        for name, value in (
            ("heartbeat_seconds", operations.heartbeat_seconds),
            ("stale_after_seconds", operations.stale_after_seconds),
            ("notifier_poll_seconds", operations.notifier_poll_seconds),
            ("archive_retention_days", operations.archive_retention_days),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                errors.append(f"operations.{name} must be a positive integer")
        if (
            isinstance(operations.heartbeat_seconds, int)
            and isinstance(operations.stale_after_seconds, int)
            and operations.stale_after_seconds
            < operations.heartbeat_seconds * 2
        ):
            errors.append(
                "operations.stale_after_seconds must be at least two heartbeats"
            )
        for name, value in (
            ("keychain_service", operations.keychain_service),
            ("keychain_account", operations.keychain_account),
            ("backup_dir", operations.backup_dir),
        ):
            if not isinstance(value, str) or not value.strip():
                errors.append(f"operations.{name} must be a non-empty string")
        for name, value in (
            ("disk_warning_pct", operations.disk_warning_pct),
            ("disk_halt_pct", operations.disk_halt_pct),
        ):
            if not finite_number(value) or not 0 < float(value) < 1:
                errors.append(f"operations.{name} must be between 0 and 1")
        if (
            finite_number(operations.disk_warning_pct)
            and finite_number(operations.disk_halt_pct)
            and operations.disk_halt_pct >= operations.disk_warning_pct
        ):
            errors.append("operations.disk_halt_pct must be below disk_warning_pct")

        if errors:
            raise ValueError("Invalid configuration:\n- " + "\n- ".join(errors))
        return self

    @property
    def round_trip_cost(self) -> float:
        per_side = self.risk.fee_rate + self.risk.slippage_bps / 10_000
        return 2 * per_side

    @property
    def feature_warmup_bars(self) -> int:
        if self.model.signal_mode == "trend_breakout":
            return max(
                self.model.breakout_entry_window,
                self.model.breakout_exit_window,
            )
        return (
            max(FEATURE_WARMUP_BARS, self.model.regime_sma_window)
            if self.model.signal_mode == "expected_return"
            else PROBABILITY_FEATURE_WARMUP_BARS
        )

    @property
    def minimum_history_bars(self) -> int:
        if self.model.signal_mode == "trend_breakout":
            return self.feature_warmup_bars + 1
        return max(
            self.model.min_train_samples + self.model.horizon_bars + 80,
            self.model.train_window
            + self.feature_warmup_bars
            + self.model.horizon_bars
            + self.model.retrain_every
            + 1,
        )

    def as_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


T = TypeVar("T")


def _read_section(cls: type[T], name: str, raw: object) -> T:
    if not isinstance(raw, dict):
        raise ValueError(f"Configuration section [{name}] must be a table")
    allowed = {item.name for item in dataclasses.fields(cls)}
    unknown = set(raw) - allowed
    if unknown:
        formatted = ", ".join(sorted(unknown))
        raise ValueError(f"Unknown keys in [{name}]: {formatted}")
    return cls(**raw)


def load_config(path: str | Path | None = None) -> AppConfig:
    if path is None:
        return AppConfig().validate()

    config_path = Path(path).expanduser().resolve()
    try:
        with config_path.open("rb") as handle:
            raw = tomllib.load(handle)
    except OSError as exc:
        raise ValueError(f"Cannot read configuration: {config_path}") from exc

    allowed_sections = {
        "data",
        "model",
        "risk",
        "paper",
        "output",
        "shadow",
        "operations",
    }
    unknown_sections = set(raw) - allowed_sections
    if unknown_sections:
        formatted = ", ".join(sorted(unknown_sections))
        raise ValueError(f"Unknown configuration sections: {formatted}")

    data = _read_section(DataConfig, "data", raw.get("data", {}))
    output = _read_section(OutputConfig, "output", raw.get("output", {}))
    shadow = _read_section(ShadowConfig, "shadow", raw.get("shadow", {}))
    operations = _read_section(
        OperationsConfig,
        "operations",
        raw.get("operations", {}),
    )
    if (
        not isinstance(data.database_path, str)
        or not isinstance(output.artifacts_dir, str)
        or not isinstance(shadow.database_path, str)
        or not isinstance(shadow.archive_root, str)
        or not isinstance(operations.backup_dir, str)
    ):
        raise ValueError("Runtime database and artifact paths must be strings")
    database_path = Path(data.database_path).expanduser()
    artifacts_dir = Path(output.artifacts_dir).expanduser()
    shadow_database_path = Path(shadow.database_path).expanduser()
    archive_root = Path(shadow.archive_root).expanduser()
    backup_dir = Path(operations.backup_dir).expanduser()
    if not database_path.is_absolute():
        database_path = config_path.parent / database_path
    if not artifacts_dir.is_absolute():
        artifacts_dir = config_path.parent / artifacts_dir
    if not shadow_database_path.is_absolute():
        shadow_database_path = config_path.parent / shadow_database_path
    if not archive_root.is_absolute():
        archive_root = config_path.parent / archive_root
    if not backup_dir.is_absolute():
        backup_dir = config_path.parent / backup_dir

    config = AppConfig(
        data=dataclasses.replace(data, database_path=str(database_path.resolve())),
        model=_read_section(ModelConfig, "model", raw.get("model", {})),
        risk=_read_section(RiskConfig, "risk", raw.get("risk", {})),
        paper=_read_section(PaperConfig, "paper", raw.get("paper", {})),
        output=dataclasses.replace(
            output, artifacts_dir=str(artifacts_dir.resolve())
        ),
        shadow=dataclasses.replace(
            shadow,
            database_path=str(shadow_database_path.resolve()),
            archive_root=str(archive_root.resolve()),
        ),
        operations=dataclasses.replace(
            operations,
            backup_dir=str(backup_dir.resolve()),
        ),
    )
    return config.validate()
