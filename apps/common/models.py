from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field

AskMode = Literal["local", "global"]
AnswerMode = Literal["auto", "llm", "no_llm"]


class AskResult(BaseModel):
    question: str
    answer: str
    requested_mode: AskMode
    answer_mode: AnswerMode = "auto"
    response_mode: str
    response_time_ms: int | None = None


class AskExchangeEvent(BaseModel):
    event_id: str
    question: str
    answer: str
    mode: AskMode
    user_id: str
    chat_id: str
    correlation_id: str
    source: str = "telegram"
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: dict[str, Any] = Field(default_factory=dict)

    def to_ingest_document(self) -> dict[str, Any]:
        payload = self.model_dump(mode="json")
        payload["event_type"] = "ask_exchange"
        return payload


class OutboxRecord(BaseModel):
    event: AskExchangeEvent
    state: str
    attempts: int
    next_attempt_at: datetime
    last_error: str | None = None
    ingest_status_code: int | None = None
    ingest_timestamp: datetime | None = None
    sheets_synced_at: datetime | None = None
    dead_letter_reason: str | None = None
