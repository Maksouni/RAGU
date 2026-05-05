from __future__ import annotations

from dataclasses import dataclass

import pytest

from apps.common.bot_messages import START_MESSAGE
from apps.vk_bot.main import _handle_message


class FakeVkApi:
    def __init__(self) -> None:
        self.sent: list[tuple[int, str]] = []

    async def send_message(self, peer_id: int, text: str) -> None:
        self.sent.append((peer_id, text))


@dataclass
class FakeResult:
    answer: str


class FakeOrchestrator:
    def __init__(self) -> None:
        self.calls: list[dict[str, str]] = []

    async def handle_user_message(
        self,
        *,
        raw_text: str,
        chat_id: str,
        user_id: str,
        correlation_id: str,
    ) -> FakeResult:
        self.calls.append(
            {
                "raw_text": raw_text,
                "chat_id": chat_id,
                "user_id": user_id,
                "correlation_id": correlation_id,
            }
        )
        return FakeResult(answer="готовый ответ")


@pytest.mark.asyncio
async def test_vk_start_command_returns_shared_start_message() -> None:
    vk_api = FakeVkApi()
    orchestrator = FakeOrchestrator()

    await _handle_message(
        update={"type": "message_new", "object": {"message": {"text": "Начать", "peer_id": 123, "from_id": 456}}},
        vk_api=vk_api,
        orchestrator=orchestrator,
    )

    assert vk_api.sent == [(123, START_MESSAGE)]
    assert orchestrator.calls == []


@pytest.mark.asyncio
async def test_vk_message_uses_same_orchestrator_flow() -> None:
    vk_api = FakeVkApi()
    orchestrator = FakeOrchestrator()

    await _handle_message(
        update={
            "type": "message_new",
            "object": {"message": {"text": "postgresql debian 13", "peer_id": 123, "from_id": 456}},
        },
        vk_api=vk_api,
        orchestrator=orchestrator,
    )

    assert vk_api.sent == [(123, "готовый ответ")]
    assert orchestrator.calls[0]["raw_text"] == "postgresql debian 13"
    assert orchestrator.calls[0]["chat_id"] == "vk:123"
    assert orchestrator.calls[0]["user_id"] == "vk:456"
