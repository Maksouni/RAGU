from apps.common.text_limits import MESSAGE_LIMIT, truncate_for_message


def test_truncate_for_message_keeps_limit() -> None:
    text = "x" * (MESSAGE_LIMIT + 500)
    result = truncate_for_message(text)
    assert len(result) <= MESSAGE_LIMIT
    assert "ответ сокращен под лимит сообщения" in result
