from __future__ import annotations

MESSAGE_LIMIT = 4096


def truncate_for_message(text: str, limit: int = MESSAGE_LIMIT) -> str:
    value = (text or "").strip()
    if len(value) <= limit:
        return value
    suffix = "\n\n... ответ сокращен под лимит сообщения."
    head_limit = max(0, limit - len(suffix))
    return value[:head_limit].rstrip() + suffix
