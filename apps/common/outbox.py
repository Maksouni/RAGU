from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

from apps.common.models import AskExchangeEvent, OutboxRecord

STATE_PENDING_INGEST = "PENDING_INGEST"
STATE_INGEST_RETRY = "INGEST_RETRY"
STATE_INGESTED = "INGESTED"
STATE_SHEETS_SYNCED = "SHEETS_SYNCED"
STATE_DEAD_LETTER = "DEAD_LETTER"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _dt_to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _iso_to_dt(raw: str | None) -> datetime | None:
    if not raw:
        return None
    return datetime.fromisoformat(raw)


class OutboxRepository:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    @contextmanager
    def _conn(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _ensure_schema(self) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS outbox_events (
                    event_id TEXT PRIMARY KEY,
                    payload_json TEXT NOT NULL,
                    state TEXT NOT NULL,
                    attempts INTEGER NOT NULL,
                    next_attempt_at TEXT NOT NULL,
                    last_error TEXT,
                    ingest_status_code INTEGER,
                    ingest_timestamp TEXT,
                    sheets_synced_at TEXT,
                    dead_letter_reason TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_outbox_state_next_attempt "
                "ON outbox_events(state, next_attempt_at)"
            )

    def enqueue_event(self, event: AskExchangeEvent) -> bool:
        now = _utcnow()
        with self._conn() as conn:
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO outbox_events (
                    event_id, payload_json, state, attempts, next_attempt_at, created_at, updated_at
                )
                VALUES (?, ?, ?, 0, ?, ?, ?)
                """,
                (
                    event.event_id,
                    event.model_dump_json(),
                    STATE_PENDING_INGEST,
                    _dt_to_iso(now),
                    _dt_to_iso(now),
                    _dt_to_iso(now),
                ),
            )
            return cursor.rowcount == 1

    def fetch_pending_ingest(self, limit: int) -> list[OutboxRecord]:
        now = _dt_to_iso(_utcnow())
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM outbox_events
                WHERE state IN (?, ?)
                  AND next_attempt_at <= ?
                ORDER BY created_at
                LIMIT ?
                """,
                (STATE_PENDING_INGEST, STATE_INGEST_RETRY, now, limit),
            ).fetchall()
        return [self._row_to_record(row) for row in rows]

    def mark_ingested(self, event_ids: list[str], ingest_status_code: int) -> None:
        if not event_ids:
            return
        now = _utcnow()
        with self._conn() as conn:
            conn.executemany(
                """
                UPDATE outbox_events
                SET state = ?,
                    ingest_status_code = ?,
                    ingest_timestamp = ?,
                    updated_at = ?,
                    last_error = NULL
                WHERE event_id = ?
                """,
                [
                    (
                        STATE_INGESTED,
                        ingest_status_code,
                        _dt_to_iso(now),
                        _dt_to_iso(now),
                        event_id,
                    )
                    for event_id in event_ids
                ],
            )

    def mark_ingest_retry(
        self,
        event_id: str,
        error_text: str,
        backoff_seconds: int,
        max_attempts: int,
    ) -> None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT attempts FROM outbox_events WHERE event_id = ?",
                (event_id,),
            ).fetchone()
            if not row:
                return
            attempts = int(row["attempts"]) + 1
            now = _utcnow()
            if attempts >= max_attempts:
                conn.execute(
                    """
                    UPDATE outbox_events
                    SET state = ?,
                        attempts = ?,
                        dead_letter_reason = ?,
                        updated_at = ?
                    WHERE event_id = ?
                    """,
                    (STATE_DEAD_LETTER, attempts, error_text[:1000], _dt_to_iso(now), event_id),
                )
                return
            next_attempt_at = now + timedelta(seconds=backoff_seconds)
            conn.execute(
                """
                UPDATE outbox_events
                SET state = ?,
                    attempts = ?,
                    last_error = ?,
                    next_attempt_at = ?,
                    updated_at = ?
                WHERE event_id = ?
                """,
                (
                    STATE_INGEST_RETRY,
                    attempts,
                    error_text[:1000],
                    _dt_to_iso(next_attempt_at),
                    _dt_to_iso(now),
                    event_id,
                ),
            )

    def fetch_pending_sheets_sync(self, limit: int) -> list[OutboxRecord]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM outbox_events
                WHERE state = ?
                ORDER BY ingest_timestamp, created_at
                LIMIT ?
                """,
                (STATE_INGESTED, limit),
            ).fetchall()
        return [self._row_to_record(row) for row in rows]

    def mark_sheets_synced(self, event_id: str) -> None:
        now = _utcnow()
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE outbox_events
                SET state = ?,
                    sheets_synced_at = ?,
                    updated_at = ?
                WHERE event_id = ?
                """,
                (STATE_SHEETS_SYNCED, _dt_to_iso(now), _dt_to_iso(now), event_id),
            )

    def mark_dead_letter(self, event_id: str, reason: str) -> None:
        now = _utcnow()
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE outbox_events
                SET state = ?,
                    dead_letter_reason = ?,
                    updated_at = ?
                WHERE event_id = ?
                """,
                (STATE_DEAD_LETTER, reason[:1000], _dt_to_iso(now), event_id),
            )

    def find_recent_answer(self, question: str, max_scan: int = 200) -> AskExchangeEvent | None:
        question_norm = self._normalize_question(question)
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT payload_json
                FROM outbox_events
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (max_scan,),
            ).fetchall()
        for row in rows:
            event = AskExchangeEvent.model_validate(json.loads(row["payload_json"]))
            if self._normalize_question(event.question) == question_norm:
                return event
        return None

    @staticmethod
    def _normalize_question(question: str) -> str:
        return " ".join((question or "").strip().lower().split())

    def _row_to_record(self, row: sqlite3.Row) -> OutboxRecord:
        event = AskExchangeEvent.model_validate(json.loads(row["payload_json"]))
        next_attempt = _iso_to_dt(row["next_attempt_at"])
        assert next_attempt is not None
        return OutboxRecord(
            event=event,
            state=row["state"],
            attempts=int(row["attempts"]),
            next_attempt_at=next_attempt,
            last_error=row["last_error"],
            ingest_status_code=row["ingest_status_code"],
            ingest_timestamp=_iso_to_dt(row["ingest_timestamp"]),
            sheets_synced_at=_iso_to_dt(row["sheets_synced_at"]),
            dead_letter_reason=row["dead_letter_reason"],
        )
