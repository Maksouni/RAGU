from apps.common.routing import route_mode_and_question


def test_route_global_command() -> None:
    routed = route_mode_and_question("/global find postgresql packages")
    assert routed.mode == "global"
    assert routed.question == "find postgresql packages"
    assert routed.mode_explicit is True


def test_route_local_flag() -> None:
    routed = route_mode_and_question("local: versions for debian 13", default_mode="global")
    assert routed.mode == "local"
    assert routed.question == "versions for debian 13"
    assert routed.mode_explicit is True


def test_route_default_mode() -> None:
    routed = route_mode_and_question("plain question", default_mode="local")
    assert routed.mode == "local"
    assert routed.question == "plain question"
    assert routed.mode_explicit is False


def test_route_keeps_plain_mode_word_as_question_text() -> None:
    routed = route_mode_and_question("global package links")
    assert routed.mode == "local"
    assert routed.question == "global package links"
    assert routed.mode_explicit is False


def test_route_answer_mode_command_combo() -> None:
    routed = route_mode_and_question("/nollm /global postgresql packages")
    assert routed.mode == "global"
    assert routed.answer_mode == "no_llm"
    assert routed.question == "postgresql packages"
    assert routed.mode_explicit is True
    assert routed.answer_mode_explicit is True


def test_route_llm_word_prefix() -> None:
    routed = route_mode_and_question("llm local: versions for debian 13")
    assert routed.mode == "local"
    assert routed.answer_mode == "llm"
    assert routed.question == "versions for debian 13"
