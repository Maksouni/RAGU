from apps.common.text_limits import TELEGRAM_MESSAGE_LIMIT, truncate_for_telegram


def test_truncate_for_telegram_keeps_limit() -> None:
    text = "x" * (TELEGRAM_MESSAGE_LIMIT + 500)
    result = truncate_for_telegram(text)
    assert len(result) <= TELEGRAM_MESSAGE_LIMIT
    assert "ответ сокращен под лимит Telegram" in result

