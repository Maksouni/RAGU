from __future__ import annotations

import re

from apps.orchestrator.query_parser import parse_scenario_query


INVALID_QUERY_MESSAGE = (
    "Не понял запрос как задачу поиска пакетов.\n\n"
    "Сформулируйте вопрос про пакет, версию, ОС или формат сборки.\n\n"
    "Примеры:\n"
    "- дай список всех версий PostgreSQL для debian 13\n"
    "- дай список всех пакетов PostgreSQL 17.6\n"
    "- Python 3.12 для Ubuntu limit=10\n\n"
    "Можно добавить фильтры: format=deb|rpm|exe, source=<часть имени>, "
    "sort=newest|oldest|name, limit=10, show=5. Для строгого поиска только по базе используйте /db."
)

_VERSION_RE = re.compile(r"\b\d+(?:\.\d+)+\b")
_DOMAIN_TOKEN_RE = re.compile(
    r"\b("
    r"postgresql|postgres|python|ubuntu|debian|rhel|windows|win|"
    r"deb|rpm|exe|package|packages|version|versions|"
    r"пакет|пакеты|пакетов|версия|версии|версий|сборка|сборки|"
    r"дебиан|убунту|виндовс"
    r")\b",
    re.IGNORECASE,
)
_FILTER_TOKEN_RE = re.compile(r"\b(format|source|sort|limit|show|os|os_version|product)\s*=", re.IGNORECASE)
_GENERAL_IT_TOKEN_RE = re.compile(
    r"\b("
    r"docker|container|image|volume|compose|kubernetes|k8s|linux|ubuntu|debian|"
    r"postgresql|postgres|sql|database|index|transaction|python|pip|venv|fastapi|"
    r"api|endpoint|http|rest|git|branch|commit|merge|rebase|ssh|tls|ssl|certificate|jwt|oauth|"
    r"rag|memgraph|graph|embedding|embeddings|vector|llm|ollama|vk|bot|"
    r"redis|cache|nginx|proxy|json|yaml|dockerfile|ci|cd"
    r")\b",
    re.IGNORECASE,
)
_GENERAL_IT_RU_TOKEN_RE = re.compile(
    r"\b("
    r"докер|контейнер|образ|том|кубернетес|линукс|база|данных|индекс|"
    r"транзакц|питон|пайтон|апи|запрос|сервер|клиент|гит|ветк|коммит|"
    r"мердж|ребейз|токен|сертификат|шифрован|эндпоинт|бот|граф|эмбеддинг|вектор|модель|кэш|прокси"
    r")",
    re.IGNORECASE,
)
_QUESTION_WORD_RE = re.compile(
    r"\b(как|что|чем|почему|зачем|объясни|расскажи|покажи|how|what|why|explain|describe)\b",
    re.IGNORECASE,
)
_CHAT_ONLY_RE = re.compile(
    r"^(привет|здравствуй|как дела|спасибо|ок|окей|hello|hi|thanks|thank you)[!?.\s]*$",
    re.IGNORECASE,
)


def is_supported_package_query(text: str) -> bool:
    value = (text or "").strip()
    if len(value) < 3:
        return False
    if parse_scenario_query(value) is not None:
        return True
    return bool(_VERSION_RE.search(value) and re.search(r"\b[a-zA-Z][a-zA-Z0-9+_.-]{1,30}\b", value))


def is_supported_general_it_query(text: str) -> bool:
    value = (text or "").strip()
    if len(value) < 3:
        return False
    if is_supported_package_query(value):
        return False

    has_it_terms = bool(_GENERAL_IT_TOKEN_RE.search(value) or _GENERAL_IT_RU_TOKEN_RE.search(value))
    if has_it_terms:
        return True
    return bool(_QUESTION_WORD_RE.search(value) or value.endswith("?") or len(value.split()) >= 2)
