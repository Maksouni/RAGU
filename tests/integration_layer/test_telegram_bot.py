from __future__ import annotations

import pytest

from apps.bot.main import cmd_start
from apps.common.bot_messages import START_MESSAGE


class FakeTelegramMessage:
    def __init__(self) -> None:
        self.answers: list[str] = []

    async def answer(self, text: str) -> None:
        self.answers.append(text)


@pytest.mark.asyncio
async def test_telegram_start_help_handler_returns_shared_start_message() -> None:
    message = FakeTelegramMessage()

    await cmd_start(message)  # type: ignore[arg-type]

    assert message.answers == [START_MESSAGE]
