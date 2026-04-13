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


def test_route_word_prefix() -> None:
    routed = route_mode_and_question("global package links")
    assert routed.mode == "global"
    assert routed.question == "package links"
    assert routed.mode_explicit is True

