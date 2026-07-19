"""Always-on public-feed archive plus stateful shadow execution service."""

from __future__ import annotations

import dataclasses
import errno
import fcntl
import hashlib
import os
import shutil
import stat
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

from coinpilot.config import AppConfig
from coinpilot.hft_archive import (
    HFTArchiveResult,
    record_upbit_public_archive,
    validate_hft_archive_request,
)
from coinpilot.hft_shadow import (
    ShadowConfig as RuntimeShadowConfig,
    ShadowEngine,
    ShadowIntent,
)
from coinpilot.hft_shadow_signal import (
    DiagnosticSnapshot,
    OnlineDiagnosticSignal,
)
from coinpilot.hft_shadow_store import ShadowStore, canonical_json
from coinpilot.hft_workflow import (
    write_artifact_completion,
    write_json_atomic,
)


class ShadowServiceError(RuntimeError):
    """Raised when the shadow service cannot continue without guessing."""


class ShadowServiceAlreadyRunning(ShadowServiceError):
    """Raised when this shadow database already has a runtime owner."""


class ShadowServiceProcessLock:
    """Non-blocking kernel lock held for one complete shadow service lifetime.

    ``flock`` is released by the kernel when a process exits, including an
    unclean exit.  The in-process registry closes the small platform difference
    where multiple descriptors opened by one process may otherwise appear to
    share lock ownership.
    """

    _registry_guard = threading.Lock()
    _held_paths: set[str] = set()

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path).expanduser().resolve(strict=False)
        self._descriptor: int | None = None
        self._registry_key = os.path.normcase(str(self.path))

    def acquire(self) -> None:
        if self._descriptor is not None:
            raise ShadowServiceError("shadow process lock is already acquired")
        with self._registry_guard:
            if self._registry_key in self._held_paths:
                raise ShadowServiceAlreadyRunning(
                    f"shadow service is already running for {self.path}"
                )
            self._held_paths.add(self._registry_key)

        descriptor: int | None = None
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            flags = os.O_CREAT | os.O_RDWR
            flags |= getattr(os, "O_CLOEXEC", 0)
            flags |= getattr(os, "O_NOFOLLOW", 0)
            descriptor = os.open(self.path, flags, 0o600)
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise ShadowServiceError(
                    f"shadow process lock is not a regular file: {self.path}"
                )
            try:
                fcntl.flock(
                    descriptor,
                    fcntl.LOCK_EX | fcntl.LOCK_NB,
                )
            except OSError as exc:
                if exc.errno in (errno.EACCES, errno.EAGAIN):
                    raise ShadowServiceAlreadyRunning(
                        f"shadow service is already running for {self.path}"
                    ) from exc
                raise
            os.fchmod(descriptor, 0o600)
            self._descriptor = descriptor
            descriptor = None
        except BaseException:
            if descriptor is not None:
                os.close(descriptor)
            with self._registry_guard:
                self._held_paths.discard(self._registry_key)
            raise

    def release(self) -> None:
        descriptor = self._descriptor
        if descriptor is None:
            return
        self._descriptor = None
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            try:
                os.close(descriptor)
            finally:
                with self._registry_guard:
                    self._held_paths.discard(self._registry_key)

    def __enter__(self) -> ShadowServiceProcessLock:
        self.acquire()
        return self

    def __exit__(
        self,
        exc_type: object,
        exc_value: object,
        traceback: object,
    ) -> None:
        self.release()


@dataclass(frozen=True, slots=True)
class ShadowServiceResult:
    run_id: str
    capture_id: str
    strategy_mode: str
    stop_reason: str
    archive: HFTArchiveResult
    status: dict[str, Any]
    run_summary_path: Path
    run_completion_path: Path
    orders_sent: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "capture_id": self.capture_id,
            "strategy_mode": self.strategy_mode,
            "stop_reason": self.stop_reason,
            "archive": self.archive.to_dict(),
            "status": self.status,
            "run_summary_path": str(self.run_summary_path),
            "run_completion_path": str(self.run_completion_path),
            "simulated": True,
            "orders_sent": 0,
            "live_order_routing": False,
        }


def runtime_shadow_config(config: AppConfig) -> RuntimeShadowConfig:
    selected = config.shadow
    return RuntimeShadowConfig(
        market=config.data.market,
        initial_cash_quote=selected.initial_cash,
        fee_rate=selected.fee_rate,
        latency_ns=int(selected.latency_ms * 1_000_000),
        max_book_gap_ns=int(selected.max_book_gap_ms * 1_000_000),
        warmup_books=selected.warmup_books,
        max_order_quote=selected.order_quote,
        equity_sample_interval_ns=selected.equity_sample_seconds * 1_000_000_000,
        health_sample_interval_ns=selected.health_sample_seconds * 1_000_000_000,
        one_position_only=True,
        mode="shadow",
    ).validate()


def shadow_process_lock_path(config: AppConfig) -> Path:
    """Return the singleton-lock path derived only from the shadow database."""

    database = Path(config.shadow.database_path).expanduser().resolve(strict=False)
    return database.parent / f".{database.name}.shadow-run.lock"


def shadow_runtime_manifest(
    config: AppConfig,
    *,
    duration_seconds: float | None = None,
    max_events: int | None = None,
) -> dict[str, Any]:
    """Canonical behavior manifest used by the restart fail-closed gate."""

    selected = config.validate()
    selected_duration = (
        selected.shadow.session_seconds
        if duration_seconds is None
        else duration_seconds
    )
    return {
        "schema_version": 1,
        "public_feed_only": True,
        "live_order_routing": False,
        "market": selected.data.market,
        # Include the complete section, not a hand-picked subset that can fall
        # out of sync when a new diagnostic or guardrail field is added.
        "shadow": dataclasses.asdict(selected.shadow),
        "execution_engine": runtime_shadow_config(selected).to_dict(),
        "operations": {
            "heartbeat_seconds": selected.operations.heartbeat_seconds,
            "disk_warning_pct": selected.operations.disk_warning_pct,
            "disk_halt_pct": selected.operations.disk_halt_pct,
        },
        "invocation": {
            "duration_seconds": selected_duration,
            "max_events": max_events,
            "max_depth": 30,
        },
        "policy_invariants": {
            "daily_loss_timezone": "Asia/Seoul",
            "one_position_only": True,
            "diagnostic_is_validated_alpha": False,
        },
    }


def shadow_runtime_fingerprint(
    config: AppConfig,
    *,
    duration_seconds: float | None = None,
    max_events: int | None = None,
) -> str:
    payload = shadow_runtime_manifest(
        config,
        duration_seconds=duration_seconds,
        max_events=max_events,
    )
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def _source_content_hash(source_root: Path) -> str:
    """Hash the actual editable Python source, including path names."""

    root = source_root.expanduser().resolve(strict=True)
    if (root / "src" / "coinpilot").is_dir():
        package_root = root / "src" / "coinpilot"
        prefix = root
    elif (root / "coinpilot").is_dir():
        package_root = root / "coinpilot"
        prefix = root
    elif root.name == "coinpilot" and root.is_dir():
        package_root = root
        prefix = root.parent
    else:
        raise ShadowServiceError(
            f"cannot locate coinpilot source below {source_root}"
        )

    python_paths = tuple(package_root.rglob("*.py"))
    symlinks = tuple(path for path in python_paths if path.is_symlink())
    if symlinks:
        raise ShadowServiceError(
            "coinpilot source fingerprint refuses symbolic-link source files"
        )
    candidates = sorted(
        (path for path in python_paths if path.is_file()),
        key=lambda path: path.relative_to(prefix).as_posix(),
    )
    pyproject = prefix / "pyproject.toml"
    if pyproject.is_symlink():
        raise ShadowServiceError(
            "coinpilot source fingerprint refuses a symbolic-link pyproject"
        )
    if pyproject.is_file():
        candidates.append(pyproject)
        candidates.sort(key=lambda path: path.relative_to(prefix).as_posix())
    if not candidates:
        raise ShadowServiceError("coinpilot source fingerprint has no files")

    digest = hashlib.sha256()
    digest.update(b"coinpilot-source-v1\0")
    for path in candidates:
        relative = path.relative_to(prefix).as_posix().encode("utf-8")
        content = path.read_bytes()
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return digest.hexdigest()


def _default_source_root() -> Path:
    current = Path(__file__).resolve()
    if (
        len(current.parents) >= 3
        and current.parents[1].name == "src"
        and (current.parents[2] / "pyproject.toml").is_file()
    ):
        return current.parents[2]
    return current.parent


def _git_identity(source_root: Path) -> tuple[str | None, bool]:
    """Return HEAD and scoped dirtiness without invoking a shell."""

    try:
        top = subprocess.run(
            ["git", "-C", str(source_root), "rev-parse", "--show-toplevel"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=5,
        ).stdout.strip()
        commit = subprocess.run(
            ["git", "-C", top, "rev-parse", "HEAD"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=5,
        ).stdout.strip()
        relative = source_root.resolve().relative_to(Path(top).resolve())
        status_result = subprocess.run(
            [
                "git",
                "-C",
                top,
                "status",
                "--porcelain=v1",
                "--untracked-files=all",
                "--",
                str(relative / "src" / "coinpilot"),
                str(relative / "pyproject.toml"),
            ],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=5,
        )
        return commit, bool(status_result.stdout.strip())
    except (
        FileNotFoundError,
        subprocess.SubprocessError,
        ValueError,
    ):
        return None, True


def installed_code_version(source_root: str | Path | None = None) -> str:
    """Fingerprint deployed code and actual editable content deterministically."""

    root = (
        _default_source_root()
        if source_root is None
        else Path(source_root).expanduser().resolve(strict=True)
    )
    source_hash = _source_content_hash(root)
    commit, dirty = _git_identity(root)
    identity = {
        "schema_version": 1,
        "git_commit": commit,
        "dirty": dirty,
        # Keeping the content hash even for a clean checkout verifies what the
        # interpreter can actually load rather than trusting index metadata.
        "source_sha256": source_hash,
    }
    override = os.environ.get("COINPILOT_CODE_VERSION", "").strip()
    if override:
        identity["deployment_label_sha256"] = hashlib.sha256(
            override.encode("utf-8")
        ).hexdigest()
    digest = hashlib.sha256(canonical_json(identity).encode("utf-8")).hexdigest()
    return f"coinpilot-code-v1:{digest}"


def shadow_deployment_version(
    config: AppConfig,
    *,
    duration_seconds: float | None = None,
    max_events: int | None = None,
    source_root: str | Path | None = None,
) -> str:
    """Combine strategy/runtime and code identities for restart comparison."""

    identity = {
        "schema_version": 1,
        "runtime_fingerprint": shadow_runtime_fingerprint(
            config,
            duration_seconds=duration_seconds,
            max_events=max_events,
        ),
        "code_fingerprint": installed_code_version(source_root),
    }
    digest = hashlib.sha256(canonical_json(identity).encode("utf-8")).hexdigest()
    return f"shadow-deployment-v1:{digest}"


class DiagnosticPolicy:
    """Bounded plumbing policy; this is not a validated alpha model."""

    def __init__(self, config: AppConfig, store: ShadowStore) -> None:
        self.config = config
        self.store = store
        self._position_started_ns: int | None = None
        self._risk_halt_reason: str | None = None
        self._day: str | None = None
        self._day_start_equity: float | None = None
        self._timezone = ZoneInfo("Asia/Seoul")

    def _daily_loss(
        self,
        snapshot: DiagnosticSnapshot,
        status: dict[str, Any],
    ) -> float:
        wall_ns = snapshot.book.received_wall_ns
        moment = (
            datetime.now(self._timezone)
            if wall_ns is None
            else datetime.fromtimestamp(
                wall_ns / 1e9,
                tz=self._timezone,
            )
        )
        day = moment.date().isoformat()
        equity = float(status["last_equity_quote"])
        if day != self._day or self._day_start_equity is None:
            self._day = day
            day_start = moment.replace(
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
            )
            persisted = self.store.first_equity_since(
                market=self.config.data.market,
                wall_ns=int(day_start.timestamp() * 1e9),
            )
            self._day_start_equity = max(
                equity if persisted is None else persisted,
                1e-12,
            )
        return max(0.0, 1.0 - equity / self._day_start_equity)

    def intent(
        self,
        snapshot: DiagnosticSnapshot,
        status: dict[str, Any],
    ) -> ShadowIntent | None:
        base = float(status["base_quantity"])
        pending = status.get("pending_order")
        if base <= 1e-12:
            self._position_started_ns = None
        elif self._position_started_ns is None:
            self._position_started_ns = snapshot.book.received_monotonic_ns

        daily_loss = self._daily_loss(snapshot, status)
        if float(status["max_drawdown"]) >= self.config.shadow.max_drawdown_pct:
            self._risk_halt_reason = "max_drawdown"
        elif daily_loss >= self.config.shadow.max_daily_loss_pct:
            self._risk_halt_reason = "daily_loss"

        if (
            self._risk_halt_reason is not None
            and base > 1e-12
            and pending is None
            and status["lifecycle_status"] == "running"
        ):
            return self._intent(
                snapshot,
                action="sell",
                reason=f"risk_exit:{self._risk_halt_reason}",
                base_quantity=base,
                daily_loss=daily_loss,
            )
        if self._risk_halt_reason is not None:
            return None
        if (
            self.config.shadow.mode != "diagnostic"
            or not snapshot.ready
            or pending is not None
            or status["lifecycle_status"] != "running"
            or snapshot.spread_bps > self.config.shadow.max_spread_bps
        ):
            return None

        if base <= 1e-12 and snapshot.signal >= self.config.shadow.entry_threshold:
            return self._intent(
                snapshot,
                action="buy",
                reason="diagnostic_entry",
                quote_notional=self.config.shadow.order_quote,
                daily_loss=daily_loss,
            )
        holding_ns = (
            0
            if self._position_started_ns is None
            else max(
                0,
                snapshot.book.received_monotonic_ns
                - self._position_started_ns,
            )
        )
        if base > 1e-12 and (
            snapshot.signal <= self.config.shadow.exit_threshold
            or holding_ns
            >= int(self.config.shadow.max_holding_seconds * 1_000_000_000)
        ):
            reason = (
                "diagnostic_signal_exit"
                if snapshot.signal <= self.config.shadow.exit_threshold
                else "diagnostic_max_holding"
            )
            return self._intent(
                snapshot,
                action="sell",
                reason=reason,
                base_quantity=base,
                daily_loss=daily_loss,
            )
        return None

    def _intent(
        self,
        snapshot: DiagnosticSnapshot,
        *,
        action: str,
        reason: str,
        daily_loss: float,
        quote_notional: float | None = None,
        base_quantity: float | None = None,
    ) -> ShadowIntent:
        return ShadowIntent(
            action=action,
            reason=reason,
            policy_version=self.config.shadow.model_version,
            signal=snapshot.signal,
            quote_notional=quote_notional,
            base_quantity=base_quantity,
            features={
                "book_imbalance_l5": snapshot.book_imbalance_l5,
                "trade_flow": snapshot.trade_flow,
                "spread_bps": snapshot.spread_bps,
                "warmup_books_seen": snapshot.warmup_books_seen,
                "daily_loss": daily_loss,
                "diagnostic_only": True,
            },
        )

    def halt_if_flat(self, engine: ShadowEngine) -> bool:
        if self._risk_halt_reason is None:
            return False
        status = engine.status()
        if (
            float(status["base_quantity"]) <= 1e-12
            and status.get("pending_order") is None
            and status["lifecycle_status"] != "halted_recovery"
        ):
            engine.halt(
                self._risk_halt_reason,
                observed_wall_ns=time.time_ns(),
            )
            return True
        return False


def _capture_paths(root: Path, capture_id: str) -> tuple[Path, Path, Path]:
    return (
        root / f"{capture_id}.reservation.json",
        root / f"{capture_id}.run.json",
        root / f"{capture_id}.complete.json",
    )


def run_shadow_service(
    config: AppConfig,
    *,
    duration_seconds: float | None = None,
    max_events: int | None = None,
    capture_id: str | None = None,
    external_stop_requested: Callable[[], bool] | None = None,
) -> ShadowServiceResult:
    """Run exactly one shadow owner for the configured durable database."""

    with ShadowServiceProcessLock(shadow_process_lock_path(config)):
        return _run_shadow_service_locked(
            config,
            duration_seconds=duration_seconds,
            max_events=max_events,
            capture_id=capture_id,
            external_stop_requested=external_stop_requested,
        )


def _run_shadow_service_locked(
    config: AppConfig,
    *,
    duration_seconds: float | None = None,
    max_events: int | None = None,
    capture_id: str | None = None,
    external_stop_requested: Callable[[], bool] | None = None,
) -> ShadowServiceResult:
    selected_duration = (
        config.shadow.session_seconds
        if duration_seconds is None
        else duration_seconds
    )
    selected_capture = validate_hft_archive_request(
        market=config.data.market,
        duration_seconds=selected_duration,
        max_events=max_events,
        partition_seconds=config.shadow.partition_seconds,
        max_depth=30,
        capture_id=capture_id or f"shadow-{uuid.uuid4().hex}",
    )
    archive_root = Path(config.shadow.archive_root)
    archive_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    reservation_path, run_summary_path, run_completion_path = _capture_paths(
        archive_root,
        selected_capture,
    )
    for path in (reservation_path, run_summary_path, run_completion_path):
        if path.exists() or path.is_symlink():
            raise ShadowServiceError(
                f"shadow capture artifact already exists: {path}"
            )
    write_json_atomic(
        {
            "schema_version": 1,
            "status": "capture_id_reserved",
            "capture_id": selected_capture,
            "mode": "hft_shadow_public_feed",
            "simulated": True,
            "orders_sent": 0,
        },
        reservation_path,
    )

    store = ShadowStore(config.shadow.database_path)
    engine, _ = ShadowEngine.start(
        store,
        runtime_shadow_config(config),
        code_version=shadow_deployment_version(
            config,
            duration_seconds=selected_duration,
            max_events=max_events,
        ),
    )
    signal = OnlineDiagnosticSignal(
        warmup_books=config.shadow.warmup_books,
        trade_flow_window_ms=config.shadow.trade_flow_window_ms,
        model_version=config.shadow.model_version,
    )
    policy = DiagnosticPolicy(config, store)
    internal_stop = threading.Event()
    heartbeat_error: list[BaseException] = []
    stop_reason = {"value": "session_complete"}
    last_health_status = {"value": None}
    intervening_boundary: dict[str, Any] = {
        "gap_reason": None,
        "monotonic_regression": False,
    }

    def stopped() -> bool:
        return internal_stop.is_set() or (
            external_stop_requested is not None and external_stop_requested()
        )

    def heartbeat() -> None:
        while not internal_stop.is_set():
            now = time.time_ns()
            try:
                usage = shutil.disk_usage(archive_root)
                free_pct = usage.free / usage.total if usage.total else 0.0
                health_status = "ok"
                if free_pct <= config.operations.disk_halt_pct:
                    health_status = "critical"
                elif free_pct <= config.operations.disk_warning_pct:
                    health_status = "degraded"
                store.update_heartbeat(
                    run_id=engine.run_id,
                    component="shadow_engine",
                    status=health_status,
                    observed_wall_ns=now,
                    event_key=f"heartbeat:{now}",
                    details={
                        "strategy_mode": config.shadow.mode,
                        "disk_free_pct": round(free_pct, 6),
                        "live_order_routing": False,
                        "orders_sent": 0,
                    },
                    alert_on_unhealthy=(
                        health_status != last_health_status["value"]
                    ),
                )
                last_health_status["value"] = health_status
                if health_status == "critical":
                    engine.halt("disk_low", observed_wall_ns=now)
                    stop_reason["value"] = "disk_low"
                    internal_stop.set()
                    return
            except BaseException as exc:
                heartbeat_error.append(exc)
                stop_reason["value"] = "heartbeat_failure"
                internal_stop.set()
                return
            internal_stop.wait(config.operations.heartbeat_seconds)

    def consume(record: dict[str, Any]) -> None:
        event = record.get("event")
        event_type = event.get("event_type") if isinstance(event, dict) else None
        if event_type != "orderbook":
            if record.get("gap_before") is True:
                intervening_boundary["gap_reason"] = (
                    record.get("gap_reason") or "intervening_event_gap"
                )
            if record.get("monotonic_regression") is True:
                intervening_boundary["monotonic_regression"] = True
        snapshot = signal.feed(record)
        if snapshot is None:
            return
        if (
            intervening_boundary["gap_reason"] is not None
            or intervening_boundary["monotonic_regression"]
        ):
            book = replace(
                snapshot.book,
                gap_before=intervening_boundary["gap_reason"] is not None,
                gap_reason=intervening_boundary["gap_reason"],
                monotonic_regression=bool(
                    intervening_boundary["monotonic_regression"]
                ),
            )
            snapshot = replace(snapshot, book=book)
            intervening_boundary["gap_reason"] = None
            intervening_boundary["monotonic_regression"] = False
        status = engine.status()
        intent = policy.intent(snapshot, status)
        engine.process_book(snapshot.book, intent)
        policy.halt_if_flat(engine)

    heartbeat_thread = threading.Thread(
        target=heartbeat,
        name="coinpilot-shadow-heartbeat",
        daemon=True,
    )
    heartbeat_thread.start()
    failed = False
    try:
        archive = record_upbit_public_archive(
            market=config.data.market,
            duration_seconds=selected_duration,
            output_root=archive_root,
            max_events=max_events,
            partition_seconds=config.shadow.partition_seconds,
            max_depth=30,
            capture_id=selected_capture,
            stop_requested=stopped,
            on_record=consume,
        )
        if heartbeat_error:
            raise ShadowServiceError("shadow heartbeat failed") from heartbeat_error[0]
        if external_stop_requested is not None and external_stop_requested():
            stop_reason["value"] = "sigterm"
    except BaseException:
        failed = True
        raise
    finally:
        internal_stop.set()
        heartbeat_thread.join(timeout=max(2, config.operations.heartbeat_seconds + 1))
        engine.stop(
            reason="runtime_error" if failed else stop_reason["value"],
            ended_wall_ns=time.time_ns(),
        )

    status = store.read_status(engine.run_id)
    run_payload = {
        "schema_version": 1,
        "mode": "hft_shadow_public_feed",
        "run_id": engine.run_id,
        "strategy_mode": config.shadow.mode,
        "stop_reason": stop_reason["value"],
        "archive": archive.to_dict(),
        "status": status,
        "simulated": True,
        "diagnostic_is_validated_alpha": False,
        "orders_sent": 0,
        "live_order_routing": False,
    }
    write_json_atomic(run_payload, run_summary_path)
    members: dict[str, Path] = {
        "capture_id_reservation": reservation_path,
        "run_summary": run_summary_path,
    }
    for index, (data_path, manifest_path) in enumerate(
        zip(archive.data_paths, archive.manifest_paths, strict=True),
        start=1,
    ):
        members[f"data_{index:04d}"] = data_path
        members[f"manifest_{index:04d}"] = manifest_path
    write_artifact_completion(
        members,
        run_completion_path,
        metadata={
            "mode": run_payload["mode"],
            "run_id": engine.run_id,
            "capture_id": selected_capture,
            "strategy_mode": config.shadow.mode,
            "simulated": True,
            "orders_sent": 0,
        },
    )
    return ShadowServiceResult(
        run_id=engine.run_id,
        capture_id=selected_capture,
        strategy_mode=config.shadow.mode,
        stop_reason=stop_reason["value"],
        archive=archive,
        status=status,
        run_summary_path=run_summary_path,
        run_completion_path=run_completion_path,
    )
