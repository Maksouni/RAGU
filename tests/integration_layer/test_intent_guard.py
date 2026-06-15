import pytest

from apps.common.models import AskExchangeEvent
from apps.common.settings import IntegrationSettings
from apps.orchestrator.intent_guard import (
    INVALID_QUERY_MESSAGE,
    is_supported_general_it_query,
    is_supported_package_query,
)
from apps.orchestrator.service import AskOrchestrator, _build_contextual_question


def test_intent_guard_accepts_package_queries() -> None:
    assert is_supported_package_query("дай список всех версий PostgreSQL для debian 13")
    assert is_supported_package_query("Python 3.12 для Ubuntu limit=10")
    assert is_supported_package_query("скачай Foo Package для Solaris format=deb")


def test_intent_guard_accepts_general_it_questions() -> None:
    assert is_supported_general_it_query("what is the difference between Docker image and container?")
    assert is_supported_general_it_query("как работает индекс в PostgreSQL")
    assert is_supported_general_it_query("объясни зачем нужны embeddings в RAG")
    assert is_supported_general_it_query("что такое endpoint в REST API?")


def test_intent_guard_accepts_broad_questions_and_rejects_tiny_noise() -> None:
    assert not is_supported_package_query("как дела")
    assert is_supported_general_it_query("как дела")
    assert is_supported_general_it_query("почему не работает сервис")
    assert not is_supported_general_it_query("ok")


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
        raw_text="ok",
        chat_id="chat",
        user_id="user",
        correlation_id="corr",
    )

    assert result.response_mode == "invalid_query"
    assert result.answer == INVALID_QUERY_MESSAGE


@pytest.mark.asyncio
async def test_orchestrator_handles_start_command_without_api_or_persisting() -> None:
    orchestrator = AskOrchestrator(
        settings=IntegrationSettings(),
        api_client=FailingApiClient(),  # type: ignore[arg-type]
        outbox=FakeOutbox(),  # type: ignore[arg-type]
    )

    result = await orchestrator.handle_user_message(
        raw_text="/start",
        chat_id="chat",
        user_id="user",
        correlation_id="corr",
    )

    assert result.response_mode == "start"
    assert result.answer


@pytest.mark.asyncio
async def test_orchestrator_returns_fast_unsupported_answer_for_unconfigured_source() -> None:
    orchestrator = AskOrchestrator(
        settings=IntegrationSettings(),
        api_client=FailingApiClient(),  # type: ignore[arg-type]
        outbox=FakeOutbox(),  # type: ignore[arg-type]
    )

    result = await orchestrator.handle_user_message(
        raw_text="/llm скачай Foo Package для Solaris format=deb",
        chat_id="chat",
        user_id="user",
        correlation_id="corr",
    )

    assert result.response_mode == "registry_scrape_local"
    assert "Источник данных не настроен" in result.answer
    assert "похожие старые ответы" in result.answer


@pytest.mark.asyncio
async def test_orchestrator_treats_context_rewritten_redis_latest_as_package_request() -> None:
    orchestrator = AskOrchestrator(
        settings=IntegrationSettings(),
        api_client=FailingApiClient(),  # type: ignore[arg-type]
        outbox=FakeOutbox(),  # type: ignore[arg-type]
    )

    result = await orchestrator.handle_user_message(
        raw_text="/llm product=redis latest version дай мне его последнюю версию",
        chat_id="chat",
        user_id="user",
        correlation_id="corr",
    )

    assert result.response_mode == "registry_scrape_local"
    assert "product=redis" in result.answer
    assert "Источник данных не настроен" in result.answer


def test_latest_followup_reuses_previous_package_context() -> None:
    previous = AskExchangeEvent(
        event_id="evt-previous",
        question="дай список всех версий PostgreSQL для debian 13",
        answer="17.6",
        mode="local",
        user_id="user",
        chat_id="chat",
        correlation_id="corr",
        metadata={"product": "postgresql", "os": "debian", "os_version": "13"},
    )

    rewritten = _build_contextual_question("а последнюю покажи", previous)

    assert rewritten == "дай список всех версий PostgreSQL для debian 13. а последнюю покажи show=1"
