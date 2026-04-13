from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from apps.common.models import AskExchangeEvent
from apps.common.outbox import OutboxRepository
from apps.common.settings import IntegrationSettings
from apps.sheets_sync.worker import SheetsSyncWorker


class FakeSheetsBackend:
    def __init__(self) -> None:
        self.tables: dict[str, dict[str, dict[str, Any]]] = {}
        self.errors: list[dict[str, Any]] = []

    def upsert(self, worksheet_name: str, key_field: str, row: dict[str, Any]) -> None:
        table = self.tables.setdefault(worksheet_name, {})
        table[str(row[key_field])] = row

    def append_error(self, row: dict[str, Any]) -> None:
        self.errors.append(row)


def test_sheets_sync_idempotent() -> None:
    tmp_dir = Path("ragu_working_dir") / "pytest_tmp" / f"sheets-{uuid4().hex}"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    outbox = OutboxRepository(tmp_dir / "outbox.sqlite")
    event = AskExchangeEvent(
        event_id="evt-1",
        question="postgresql versions",
        answer="17.6",
        mode="global",
        user_id="u1",
        chat_id="c1",
        correlation_id="corr-1",
        timestamp=datetime.now(timezone.utc),
    )
    outbox.enqueue_event(event)
    outbox.mark_ingested(["evt-1"], ingest_status_code=202)

    settings = IntegrationSettings(
        SHEETS_SYNC_ENABLED=True,
        GOOGLE_SHEETS_SPREADSHEET_ID="dummy",
        GOOGLE_SERVICE_ACCOUNT_JSON_PATH="dummy.json",
    )
    backend = FakeSheetsBackend()
    worker = SheetsSyncWorker(settings=settings, outbox=outbox, backend=backend)

    assert worker.sync_once() == 1
    assert worker.sync_once() == 0

    assert len(backend.tables["overview"]) == 1
    row = next(iter(backend.tables["overview"].values()))
    assert "response_time_ms" in row
    assert "cache_hit" in row
    assert len(backend.tables["queries"]) == 1
    assert len(backend.tables["answers"]) == 1
    assert len(backend.tables["ingest_jobs"]) == 1
    assert backend.errors == []
