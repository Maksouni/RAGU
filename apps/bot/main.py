from __future__ import annotations

import asyncio
import logging
import uuid

from aiogram import Bot, Dispatcher, Router
from aiogram.filters import Command
from aiogram.types import Message

from apps.common.api_client import ApiClientError, RaguApiClient
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
    await message.answer(
        "Привет! Я ищу пакеты по источникам (registry + scraper), а не только через LLM.\n\n"
        "Как использовать:\n"
        "- Просто пиши запрос без /local и /global.\n"
        "- Если похожий запрос уже был, сначала верну ответ из кэша.\n"
        "- Если данных нет, автоматически пойду в local-поиск.\n"
        "- Команды /local и /global можно использовать вручную.\n\n"
        "Примеры:\n"
        "- дай список всех версий PostgreSQL для debian 13\n"
        "- дай список всех пакетов PostgreSQL 17.6\n"
        "- Python 3.12 для Ubuntu limit=10\n\n"
        "Фильтры:\n"
        "- format=deb|rpm|apk|exe\n"
        "- source=<часть имени источника>\n"
        "- sort=newest|oldest|name\n"
        "- limit=10\n"
        "- show=5"
    )


@router.message()
async def on_text_message(message: Message) -> None:
    global _orchestrator
    if _orchestrator is None:
        await message.answer("Бот еще инициализируется, попробуйте снова через несколько секунд.")
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
            await message.answer("Пустой запрос. Добавьте текст вопроса.")
            return
        except ApiClientError as exc:
            logger.exception("API error while handling telegram message")
            await message.answer(f"Ошибка API: {exc}")
            return
        except Exception:
            logger.exception("Unexpected bot handler error")
            await message.answer("Временная ошибка обработки запроса. Попробуйте снова.")
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
