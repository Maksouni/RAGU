from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from apps.common.models import AskExchangeEvent
from apps.common.outbox import OutboxRepository


def test_outbox_roundtrip() -> None:
    tmp_dir = Path("ragu_working_dir") / "pytest_tmp" / f"outbox-{uuid4().hex}"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    repo = OutboxRepository(tmp_dir / "outbox.sqlite")
    event = AskExchangeEvent(
        event_id="evt-1",
        question="q",
        answer="a",
        mode="local",
        user_id="u1",
        chat_id="c1",
        correlation_id="corr-1",
        timestamp=datetime.now(timezone.utc),
    )
    assert repo.enqueue_event(event) is True
    records = repo.fetch_pending_ingest(limit=10)
    assert len(records) == 1
    assert records[0].event.event_id == "evt-1"
    assert records[0].event.question == "q"
    assert records[0].event.answer == "a"
    cached = repo.find_recent_answer("q")
    assert cached is not None
    assert cached.answer == "a"
