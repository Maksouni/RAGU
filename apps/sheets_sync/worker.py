from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol
from zoneinfo import ZoneInfo

from apps.common.outbox import OutboxRepository
from apps.common.settings import IntegrationSettings

logger = logging.getLogger(__name__)
NSK_TZ = ZoneInfo("Asia/Novosibirsk")


class SheetsBackend(Protocol):
    def upsert(self, worksheet_name: str, key_field: str, row: dict[str, Any]) -> None:
        ...

    def append_error(self, row: dict[str, Any]) -> None:
        ...


@dataclass
class GoogleSheetsBackend:
    settings: IntegrationSettings
    _client: Any = None
    _spreadsheet: Any = None

    _headers: dict[str, list[str]] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        self._headers = {
            "overview": [
                "event_id",
                "user_id",
                "question",
                "answer",
                "timestamp_nsk",
                "response_time_ms",
                "response_mode",
                "cache_hit",
            ],
            "queries": ["event_id", "timestamp", "user_id", "chat_id", "mode", "question", "correlation_id"],
            "answers": ["event_id", "timestamp", "mode", "answer", "correlation_id"],
            "ingest_jobs": ["event_id", "ingest_timestamp", "ingest_status_code", "attempts"],
            "errors_audit": ["event_id", "timestamp", "stage", "error"],
        }

    def _connect(self) -> None:
        if self._spreadsheet is not None:
            return
        import gspread
        from google.oauth2.service_account import Credentials

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        credentials = Credentials.from_service_account_file(
            self.settings.google_service_account_json_path,
            scopes=scopes,
        )
        self._client = gspread.authorize(credentials)
        self._spreadsheet = self._client.open_by_key(self.settings.google_sheets_spreadsheet_id)
        self._ensure_overview_first()

    def _ensure_overview_first(self) -> None:
        assert self._spreadsheet is not None
        try:
            overview = self._spreadsheet.worksheet("overview")
        except Exception:
            overview = self._spreadsheet.add_worksheet(title="overview", rows=1000, cols=26)
            overview.append_row(self._headers["overview"])
        worksheets = self._spreadsheet.worksheets()
        ordered = [overview] + [ws for ws in worksheets if ws.id != overview.id]
        self._spreadsheet.reorder_worksheets(ordered)

    def _ensure_worksheet(self, worksheet_name: str) -> Any:
        self._connect()
        assert self._spreadsheet is not None
        try:
            worksheet = self._spreadsheet.worksheet(worksheet_name)
        except Exception:
            worksheet = self._spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=26)
            worksheet.append_row(self._headers[worksheet_name])
            return worksheet
        current_header = worksheet.row_values(1)
        expected_header = self._headers[worksheet_name]
        if current_header != expected_header:
            worksheet.update(f"A1:{chr(64 + len(expected_header))}1", [expected_header])
        return worksheet

    def _sheet_to_dict_rows(self, rows: Iterable[list[str]]) -> list[dict[str, str]]:
        rows = list(rows)
        if not rows:
            return []
        headers = rows[0]
        result: list[dict[str, str]] = []
        for row in rows[1:]:
            row_data: dict[str, str] = {}
            for i, value in enumerate(row):
                if i < len(headers):
                    row_data[headers[i]] = value
            result.append(row_data)
        return result

    def upsert(self, worksheet_name: str, key_field: str, row: dict[str, Any]) -> None:
        worksheet = self._ensure_worksheet(worksheet_name)
        records = self._sheet_to_dict_rows(worksheet.get_all_values())
        key_value = str(row[key_field])
        headers = self._headers[worksheet_name]
        values = [str(row.get(header, "")) for header in headers]

        for idx, record in enumerate(records, start=2):
            if record.get(key_field) == key_value:
                worksheet.update(f"A{idx}:{chr(64 + len(headers))}{idx}", [values])
                return
        worksheet.append_row(values)

    def append_error(self, row: dict[str, Any]) -> None:
        headers = self._headers["errors_audit"]
        worksheet = self._ensure_worksheet("errors_audit")
        values = [str(row.get(header, "")) for header in headers]
        worksheet.append_row(values)


class SheetsSyncWorker:
    def __init__(
        self,
        settings: IntegrationSettings,
        outbox: OutboxRepository,
        backend: SheetsBackend,
        batch_size: int = 100,
    ) -> None:
        self._settings = settings
        self._outbox = outbox
        self._backend = backend
        self._batch_size = batch_size

    def _write_error(self, event_id: str, stage: str, error: str) -> None:
        self._backend.append_error(
            {
                "event_id": event_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "stage": stage,
                "error": error[:1000],
            }
        )

    def sync_once(self) -> int:
        if not self._settings.sheets_sync_enabled:
            return 0
        if not self._settings.google_sheets_spreadsheet_id or not self._settings.google_service_account_json_path:
            logger.warning("Sheets sync is enabled, but Google config is missing. Skipping.")
            return 0

        records = self._outbox.fetch_pending_sheets_sync(limit=self._batch_size)
        if not records:
            return 0

        synced = 0
        for record in records:
            event = record.event
            if not event.question or not event.answer:
                reason = "invalid event: empty question or answer"
                self._outbox.mark_dead_letter(event.event_id, reason)
                self._write_error(event.event_id, "validation", reason)
                continue

            try:
                nsk_timestamp = event.timestamp.astimezone(NSK_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
                response_time_ms = event.metadata.get("response_time_ms", "")
                response_mode = event.metadata.get("response_mode", "")
                cache_hit = event.metadata.get("cache_hit", "")
                self._backend.upsert(
                    "overview",
                    "event_id",
                    {
                        "event_id": event.event_id,
                        "user_id": event.user_id,
                        "question": event.question,
                        "answer": event.answer,
                        "timestamp_nsk": nsk_timestamp,
                        "response_time_ms": response_time_ms,
                        "response_mode": response_mode,
                        "cache_hit": cache_hit,
                    },
                )
                self._backend.upsert(
                    "queries",
                    "event_id",
                    {
                        "event_id": event.event_id,
                        "timestamp": event.timestamp.isoformat(),
                        "user_id": event.user_id,
                        "chat_id": event.chat_id,
                        "mode": event.mode,
                        "question": event.question,
                        "correlation_id": event.correlation_id,
                    },
                )
                self._backend.upsert(
                    "answers",
                    "event_id",
                    {
                        "event_id": event.event_id,
                        "timestamp": event.timestamp.isoformat(),
                        "mode": event.mode,
                        "answer": event.answer,
                        "correlation_id": event.correlation_id,
                    },
                )
                self._backend.upsert(
                    "ingest_jobs",
                    "event_id",
                    {
                        "event_id": event.event_id,
                        "ingest_timestamp": record.ingest_timestamp.isoformat() if record.ingest_timestamp else "",
                        "ingest_status_code": record.ingest_status_code or "",
                        "attempts": record.attempts,
                    },
                )
                self._outbox.mark_sheets_synced(event.event_id)
                synced += 1
            except Exception as exc:
                logger.exception("Sheets sync failure event_id=%s", event.event_id)
                self._write_error(event.event_id, "sync", str(exc))

        return synced
