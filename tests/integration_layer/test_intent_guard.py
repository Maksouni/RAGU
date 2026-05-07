import pytest

from apps.common.settings import IntegrationSettings
from apps.orchestrator.intent_guard import INVALID_QUERY_MESSAGE, is_supported_package_query
from apps.orchestrator.service import AskOrchestrator


def test_intent_guard_accepts_package_queries() -> None:
    assert is_supported_package_query("дай список всех версий PostgreSQL для debian 13")
    assert is_supported_package_query("Python 3.12 для Ubuntu limit=10")
    assert is_supported_package_query("дай мне Geometry Dash Lite для Android format=apk")


def test_intent_guard_rejects_meta_or_chat_queries() -> None:
    assert not is_supported_package_query("если не написал llm nollm что по умолчанию работает")
    assert not is_supported_package_query("как дела")


class FailingApiClient:
    async def ask(self, *args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("API should not be called for invalid queries")

    async def beautify_answer(self, *args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("LLM formatter should not be called for invalid queries")


class FakeOutbox:
    def find_recent_answer(self, question: str):  # noqa: ANN201
        return None

    def enqueue_event(self, event):  # noqa: ANN001, ANN201
        raise AssertionError("Invalid queries should not be persisted")


@pytest.mark.asyncio
async def test_orchestrator_returns_guidance_for_invalid_query_without_persisting() -> None:
    orchestrator = AskOrchestrator(
        settings=IntegrationSettings(),
        api_client=FailingApiClient(),  # type: ignore[arg-type]
        outbox=FakeOutbox(),  # type: ignore[arg-type]
    )

    result = await orchestrator.handle_user_message(
        raw_text="если не написал llm nollm что по умолчанию работает",
        chat_id="chat",
        user_id="user",
        correlation_id="corr",
    )

    assert result.response_mode == "invalid_query"
    assert result.answer == INVALID_QUERY_MESSAGE


@pytest.mark.asyncio
async def test_orchestrator_returns_fast_unsupported_answer_for_android_apk() -> None:
    orchestrator = AskOrchestrator(
        settings=IntegrationSettings(),
        api_client=FailingApiClient(),  # type: ignore[arg-type]
        outbox=FakeOutbox(),  # type: ignore[arg-type]
    )

    result = await orchestrator.handle_user_message(
        raw_text="/llm дай мне Geometry Dash Lite для Android format=apk",
        chat_id="chat",
        user_id="user",
        correlation_id="corr",
    )

    assert result.response_mode == "registry_scrape_local"
    assert "Android APK-источники" in result.answer
