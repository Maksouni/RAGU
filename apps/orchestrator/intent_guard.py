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
    "Можно добавить фильтры: format=deb|rpm|apk|exe, source=<часть имени>, "
    "sort=newest|oldest|name, limit=10, show=5."
)

_VERSION_RE = re.compile(r"\b\d+(?:\.\d+)+\b")
_DOMAIN_TOKEN_RE = re.compile(
    r"\b("
    r"postgresql|postgres|python|ubuntu|debian|alpine|rhel|windows|win|"
    r"deb|rpm|apk|exe|package|packages|version|versions|"
    r"пакет|пакеты|пакетов|версия|версии|версий|сборка|сборки|"
    r"дебиан|убунту|виндовс"
    r")\b",
    re.IGNORECASE,
)
_FILTER_TOKEN_RE = re.compile(r"\b(format|source|sort|limit|show|os|os_version|product)\s*=", re.IGNORECASE)


def is_supported_package_query(text: str) -> bool:
    value = (text or "").strip()
    if len(value) < 3:
        return False
    if parse_scenario_query(value) is not None:
        return True
    return bool(_VERSION_RE.search(value) and re.search(r"\b[a-zA-Z][a-zA-Z0-9+_.-]{1,30}\b", value))
