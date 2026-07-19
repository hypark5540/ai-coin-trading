from __future__ import annotations

import fcntl
import hashlib
import json
import os
import sqlite3
from collections.abc import Iterable, Mapping
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from coinpilot.data import CANDLE_COLUMNS, validate_candles


class ConcurrentPaperUpdate(ValueError):
    """Raised when another paper runner committed a newer account revision."""


def _utc_timestamp(value: str | datetime | pd.Timestamp) -> pd.Timestamp:
    """Normalize a database query boundary to a timezone-aware UTC timestamp."""
    timestamp = pd.Timestamp(value)
    if pd.isna(timestamp):
        raise ValueError("Candle range boundaries cannot be NaT")
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def _json_default(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, np.generic):
        return value.item()
    raise TypeError(f"Cannot JSON encode {type(value).__name__}")


class SQLiteStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.is_symlink():
            raise ValueError("SQLite database path cannot be a symbolic link")
        try:
            descriptor = os.open(
                self.path,
                os.O_CREAT | os.O_EXCL | os.O_RDWR,
                0o600,
            )
        except FileExistsError:
            pass
        else:
            os.close(descriptor)
        self._secure_database_files()
        self._initialize()
        self._secure_database_files()

    def _secure_database_files(self) -> None:
        for path in (
            self.path,
            Path(f"{self.path}-wal"),
            Path(f"{self.path}-shm"),
        ):
            if path.exists():
                os.chmod(path, 0o600)

    @contextmanager
    def paper_account_lock(self, account_key: str):
        lock_id = hashlib.sha256(account_key.encode("utf-8")).hexdigest()[:16]
        lock_path = Path(f"{self.path}.paper-{lock_id}.lock")
        descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        os.chmod(lock_path, 0o600)
        try:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise ConcurrentPaperUpdate(
                    f"Paper account {account_key!r} is already running"
                ) from exc
            yield
        finally:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS candles (
                    market TEXT NOT NULL,
                    interval_minutes INTEGER NOT NULL,
                    timestamp TEXT NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume REAL NOT NULL,
                    quote_volume REAL NOT NULL,
                    PRIMARY KEY (market, interval_minutes, timestamp)
                );

                CREATE TABLE IF NOT EXISTS paper_state (
                    account_key TEXT PRIMARY KEY,
                    state_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS paper_events (
                    event_id TEXT PRIMARY KEY,
                    account_key TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_paper_events_account_time
                    ON paper_events(account_key, timestamp DESC);
                """
            )
        self._secure_database_files()

    def upsert_candles(self, frame: pd.DataFrame, interval_minutes: int) -> int:
        candles = validate_candles(frame)
        rows = [
            (
                row.market,
                interval_minutes,
                pd.Timestamp(row.timestamp).isoformat(),
                float(row.open),
                float(row.high),
                float(row.low),
                float(row.close),
                float(row.volume),
                float(row.quote_volume),
            )
            for row in candles.itertuples(index=False)
        ]
        with self._connect() as connection:
            before = connection.total_changes
            connection.executemany(
                """
                INSERT INTO candles (
                    market, interval_minutes, timestamp, open, high, low,
                    close, volume, quote_volume
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(market, interval_minutes, timestamp) DO UPDATE SET
                    open = excluded.open,
                    high = excluded.high,
                    low = excluded.low,
                    close = excluded.close,
                    volume = excluded.volume,
                    quote_volume = excluded.quote_volume
                """,
                rows,
            )
            return connection.total_changes - before

    def load_candles(
        self, market: str, interval_minutes: int, *, limit: int | None = None
    ) -> pd.DataFrame:
        limit_sql = ""
        params: list[Any] = [market, interval_minutes]
        if limit is not None:
            limit_sql = " LIMIT ?"
            params.append(limit)
        query = f"""
            SELECT timestamp, market, open, high, low, close, volume, quote_volume
            FROM (
                SELECT timestamp, market, open, high, low, close, volume, quote_volume
                FROM candles
                WHERE market = ? AND interval_minutes = ?
                ORDER BY timestamp DESC
                {limit_sql}
            )
            ORDER BY timestamp ASC
        """
        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()
        if not rows:
            return pd.DataFrame(columns=CANDLE_COLUMNS)
        return validate_candles(pd.DataFrame([dict(row) for row in rows]))

    def candle_bounds(
        self, market: str, interval_minutes: int
    ) -> tuple[pd.Timestamp, pd.Timestamp] | None:
        """Return the inclusive oldest/newest stored candle timestamps.

        The returned timestamps are timezone-aware UTC values. ``None`` means
        that no candles are stored for the requested market and interval.
        """
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT
                    MIN(timestamp) AS first_timestamp,
                    MAX(timestamp) AS last_timestamp
                FROM candles
                WHERE market = ? AND interval_minutes = ?
                """,
                (market, interval_minutes),
            ).fetchone()
        if row["first_timestamp"] is None:
            return None
        return (
            _utc_timestamp(row["first_timestamp"]),
            _utc_timestamp(row["last_timestamp"]),
        )

    def load_candles_range(
        self,
        market: str,
        interval_minutes: int,
        *,
        start: str | datetime | pd.Timestamp | None = None,
        end: str | datetime | pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        """Load candles chronologically from the half-open range ``[start, end)``.

        ``start`` is inclusive and ``end`` is exclusive, which lets adjacent
        research splits share a boundary without sharing a candle. Either
        boundary may be omitted. Naive timestamp inputs are interpreted as UTC;
        timezone-aware inputs are converted to UTC before querying.
        """
        start_timestamp = _utc_timestamp(start) if start is not None else None
        end_timestamp = _utc_timestamp(end) if end is not None else None
        if (
            start_timestamp is not None
            and end_timestamp is not None
            and start_timestamp > end_timestamp
        ):
            raise ValueError("Candle range start must not be after end")

        predicates = ["market = ?", "interval_minutes = ?"]
        params: list[Any] = [market, interval_minutes]
        if start_timestamp is not None:
            predicates.append("timestamp >= ?")
            params.append(start_timestamp.isoformat())
        if end_timestamp is not None:
            predicates.append("timestamp < ?")
            params.append(end_timestamp.isoformat())

        query = f"""
            SELECT timestamp, market, open, high, low, close, volume, quote_volume
            FROM candles
            WHERE {" AND ".join(predicates)}
            ORDER BY timestamp ASC
        """
        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()
        if not rows:
            return pd.DataFrame(columns=CANDLE_COLUMNS)
        return validate_candles(pd.DataFrame([dict(row) for row in rows]))

    def candle_count(self, market: str, interval_minutes: int) -> int:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT COUNT(*) AS count
                FROM candles
                WHERE market = ? AND interval_minutes = ?
                """,
                (market, interval_minutes),
            ).fetchone()
        return int(row["count"])

    def delete_candles(
        self,
        market: str,
        interval_minutes: int,
        timestamps: Iterable[pd.Timestamp],
    ) -> int:
        values = [
            (market, interval_minutes, pd.Timestamp(timestamp).isoformat())
            for timestamp in timestamps
        ]
        if not values:
            return 0
        with self._connect() as connection:
            before = connection.total_changes
            connection.executemany(
                """
                DELETE FROM candles
                WHERE market = ? AND interval_minutes = ? AND timestamp = ?
                """,
                values,
            )
            return connection.total_changes - before

    def load_paper_state(self, account_key: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT state_json FROM paper_state WHERE account_key = ?",
                (account_key,),
            ).fetchone()
        if row is None:
            return None
        return json.loads(row["state_json"])

    def save_paper_step(
        self,
        account_key: str,
        state: Mapping[str, Any],
        events: Iterable[Mapping[str, Any]],
        *,
        expected_revision: int,
    ) -> int:
        next_revision = expected_revision + 1
        state_to_save = dict(state)
        state_to_save["revision"] = next_revision
        state_json = json.dumps(
            state_to_save,
            ensure_ascii=False,
            sort_keys=True,
            default=_json_default,
        )
        updated_at = str(
            state_to_save.get("updated_at")
            or pd.Timestamp.now(tz="UTC").isoformat()
        )

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT state_json FROM paper_state WHERE account_key = ?",
                (account_key,),
            ).fetchone()
            current_revision = (
                int(json.loads(existing["state_json"]).get("revision", 0))
                if existing is not None
                else 0
            )
            if current_revision != expected_revision:
                raise ConcurrentPaperUpdate(
                    f"Paper account changed concurrently: expected revision "
                    f"{expected_revision}, found {current_revision}"
                )
            for event in events:
                payload = dict(event.get("payload", {}))
                payload_json = json.dumps(
                    payload,
                    ensure_ascii=False,
                    sort_keys=True,
                    default=_json_default,
                )
                scoped_event_id = f"{account_key}:{event['event_id']}"
                cursor = connection.execute(
                    """
                    INSERT OR IGNORE INTO paper_events (
                        event_id, account_key, timestamp, event_type, payload_json
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        scoped_event_id,
                        account_key,
                        str(event["timestamp"]),
                        str(event["event_type"]),
                        payload_json,
                    ),
                )
                if cursor.rowcount == 0:
                    prior = connection.execute(
                        """
                        SELECT account_key, timestamp, event_type, payload_json
                        FROM paper_events
                        WHERE event_id = ?
                        """,
                        (scoped_event_id,),
                    ).fetchone()
                    expected = (
                        account_key,
                        str(event["timestamp"]),
                        str(event["event_type"]),
                        payload_json,
                    )
                    actual = (
                        prior["account_key"],
                        prior["timestamp"],
                        prior["event_type"],
                        prior["payload_json"],
                    )
                    if actual != expected:
                        raise ValueError(
                            "Deterministic paper event ID collided with different data"
                        )
            connection.execute(
                """
                INSERT INTO paper_state(account_key, state_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(account_key) DO UPDATE SET
                    state_json = excluded.state_json,
                    updated_at = excluded.updated_at
                """,
                (account_key, state_json, updated_at),
            )
        return next_revision

    def recent_paper_events(
        self, account_key: str, *, limit: int = 20
    ) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT event_id, timestamp, event_type, payload_json
                FROM paper_events
                WHERE account_key = ?
                ORDER BY timestamp DESC, rowid DESC
                LIMIT ?
                """,
                (account_key, limit),
            ).fetchall()
        return [
            {
                "event_id": row["event_id"],
                "timestamp": row["timestamp"],
                "event_type": row["event_type"],
                "payload": json.loads(row["payload_json"]),
            }
            for row in rows
        ]
