from __future__ import annotations

import asyncio
import logging
import uuid

from aiogram import Bot, Dispatcher, Router
from aiogram.filters import Command
from aiogram.types import Message

from apps.common.api_client import ApiClientError, RaguApiClient
from apps.common.bot_messages import EMPTY_QUERY_MESSAGE, INIT_MESSAGE, START_MESSAGE, TEMP_ERROR_MESSAGE
from apps.common.context import correlation_scope
from apps.common.logging import configure_logging
from apps.common.outbox import OutboxRepository
from apps.common.settings import get_settings
from apps.common.text_limits import truncate_for_telegram
from apps.orchestrator.service import AskOrchestrator

logger = logging.getLogger(__name__)
router = Router()

_orchestrator: AskOrchestrator | None = None


@router.message(Command("start"))
async def cmd_start(message: Message) -> None:
    await message.answer(START_MESSAGE)


@router.message()
async def on_text_message(message: Message) -> None:
    global _orchestrator
    if _orchestrator is None:
        await message.answer(INIT_MESSAGE)
        return

    text = message.text or ""
    correlation_id = str(uuid.uuid4())
    user_id = str(message.from_user.id) if message.from_user else "unknown"
    chat_id = str(message.chat.id)

    with correlation_scope(correlation_id):
        try:
            result = await _orchestrator.handle_user_message(
                raw_text=text,
                chat_id=chat_id,
                user_id=user_id,
                correlation_id=correlation_id,
            )
        except ValueError:
            await message.answer(EMPTY_QUERY_MESSAGE)
            return
        except ApiClientError as exc:
            logger.exception("API error while handling telegram message")
            await message.answer(f"Ошибка API: {exc}")
            return
        except Exception:
            logger.exception("Unexpected bot handler error")
            await message.answer(TEMP_ERROR_MESSAGE)
            return

        await message.answer(truncate_for_telegram(result.answer))


async def run_bot() -> None:
    global _orchestrator
    settings = get_settings()
    configure_logging(settings.log_level)

    if not settings.telegram_bot_token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is required")

    outbox = OutboxRepository(settings.resolve_outbox_db_path())
    api_client = RaguApiClient(settings)
    _orchestrator = AskOrchestrator(settings=settings, api_client=api_client, outbox=outbox)

    dp = Dispatcher()
    dp.include_router(router)
    bot = Bot(token=settings.telegram_bot_token)

    logger.info("Telegram bot polling started")
    try:
        await dp.start_polling(bot, polling_timeout=settings.bot_polling_timeout_sec)
    finally:
        if _orchestrator is not None:
            await _orchestrator.aclose()
        await api_client.aclose()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(run_bot())
