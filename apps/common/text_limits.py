from __future__ import annotations

TELEGRAM_MESSAGE_LIMIT = 4096


def truncate_for_message(text: str, limit: int = TELEGRAM_MESSAGE_LIMIT) -> str:
    value = (text or "").strip()
    if len(value) <= limit:
        return value
    suffix = "\n\n... ответ сокращен под лимит сообщения."
    head_limit = max(0, limit - len(suffix))
    return value[:head_limit].rstrip() + suffix


def truncate_for_telegram(text: str, limit: int = TELEGRAM_MESSAGE_LIMIT) -> str:
    return truncate_for_message(text, limit=limit)
