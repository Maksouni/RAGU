from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel

ScenarioType = Literal["versions_by_os", "formats_by_version"]
PackageFormat = Literal["deb", "rpm", "exe"]
_PRODUCT_ALIASES = {
    "postgres": "postgresql",
    "postgresql": "postgresql",
    "python": "python",
    "redis": "redis",
    "mysql": "mysql",
    "mariadb": "mariadb",
    "sqlite": "sqlite3",
    "sqlite3": "sqlite3",
    "mongodb": "mongodb",
    "mongo": "mongodb",
    "go": "golang",
    "golang": "golang",
    "node": "nodejs",
    "nodejs": "nodejs",
    "javascript": "nodejs",
    "js": "nodejs",
    "ruby": "ruby",
    "php": "php",
    "java": "openjdk",
    "jdk": "openjdk",
    "openjdk": "openjdk",
    "rust": "rustc",
    "rustc": "rustc",
}
_KNOWN_PRODUCTS_PATTERN = "|".join(
    sorted((re.escape(product) for product in _PRODUCT_ALIASES), key=len, reverse=True)
)

_PRODUCT_VERSION_OS_RE = re.compile(
    r"(?P<product>[a-zA-Zа-яА-Я0-9+_.-]+)\s+"
    r"(?P<version>\d+(?:\.\d+)+)\s+"
    r"(?:для|for|on)\s+"
    r"(?P<os>[a-zA-Zа-яА-Я]+)"
    r"(?:\s+(?P<osv>[0-9][0-9.]*))?",
    re.IGNORECASE,
)
_VERSIONS_FOR_OS_RE = re.compile(
    r"(?:все\s+)?верс\w+.*?"
    rf"(?P<product>{_KNOWN_PRODUCTS_PATTERN})\s+"
    r"(?:для|for)\s+"
    r"(?P<os>[a-zA-Zа-яА-Я]+)\s*"
    r"(?P<osv>[0-9][0-9.]*)?",
    re.IGNORECASE,
)
_PACKAGES_BY_VERSION_RE = re.compile(
    r"(?:все\s+)?пакет\w+.*?"
    rf"(?P<product>{_KNOWN_PRODUCTS_PATTERN})\s+"
    r"(?P<version>\d+(?:\.\d+)+)",
    re.IGNORECASE,
)
_PRODUCT_FOR_OS_RE = re.compile(
    r"(?P<product>[a-zA-Zа-яА-Я0-9+_.-][a-zA-Zа-яА-Я0-9+_.\-\s]{1,80}?)\s+"
    r"(?:для|for|on)\s+"
    r"(?P<os>[^\s=]+)"
    r"(?:\s+(?P<osv>[0-9][0-9.]*))?",
    re.IGNORECASE,
)
_ANY_VERSION_RE = re.compile(r"(?P<version>\d+(?:\.\d+)+)")
_VERSION_HINT_RE = re.compile(r"\b(верс\w*|version|versions)\b", re.IGNORECASE)
_INSTALL_HINT_RE = re.compile(r"\b(install|installer|download|file)\b|установ\w*|файл\w*", re.IGNORECASE)
_KNOWN_PRODUCT_RE = re.compile(rf"\b({_KNOWN_PRODUCTS_PATTERN})\b", re.IGNORECASE)
_LEADING_REQUEST_WORDS_RE = re.compile(
    r"^(?:дай|дайте|мне|найди|найдите|покажи|покажите|скачай|скачать|нужен|нужна|нужно|"
    r"список|все|всех|дай список|"
    r"give|me|find|show|get|need|list|download|РґР°Р№|РґР°Р№С‚Рµ|РјРЅРµ|РЅР°Р№РґРё|РїРѕРєР°Р¶Рё)\s+",
    re.IGNORECASE,
)
_ONLY_REQUEST_WORD_RE = re.compile(
    r"^(?:дай|дайте|мне|найди|найдите|покажи|покажите|скачай|скачать|нужен|нужна|нужно|"
    r"список|все|всех|give|me|find|show|get|need|list|download)$",
    re.IGNORECASE,
)
_OS_PHRASE_RE = re.compile(
    r"(?:для|for|on|под|на)\s+"
    r"(?P<os>[a-zA-Zа-яА-Я0-9+_.-]+)"
    r"(?:\s+(?P<osv>[0-9][0-9.]*))?",
    re.IGNORECASE,
)
_KNOWN_OS_RE = re.compile(
    r"\b(?P<os>ubuntu|debian|android|windows|win|rhel|solaris|freebsd|"
    r"убунту|дебиан|андроид|виндовс)\b"
    r"(?:\s+(?P<osv>[0-9][0-9.]*))?",
    re.IGNORECASE,
)


class ScenarioQuery(BaseModel):
    scenario_type: ScenarioType
    product: str
    os: str | None = None
    os_version: str | None = None
    package_version: str | None = None
    package_format: PackageFormat | None = None
    source_name: str | None = None
    sort_by: Literal["newest", "oldest", "name"] = "newest"
    limit: int = 30
    show: int = 5
    raw_query: str


def _extract_filter_params(raw: str) -> dict[str, str]:
    params: dict[str, str] = {}
    for key, value in re.findall(r"\b([a-z_]+)\s*=\s*([a-z0-9._-]+)\b", raw.lower()):
        params[key] = value
    return params


def _normalize_product(value: str | None) -> str:
    if not value:
        return ""
    key = value.strip().lower()
    return _PRODUCT_ALIASES.get(key, key)


def _normalize_os(value: str | None) -> str | None:
    if not value:
        return None
    mapping = {
        "ubuntu": "ubuntu",
        "убунту": "ubuntu",
        "debian": "debian",
        "дебиан": "debian",
        "android": "android",
        "андроид": "android",
        "windows": "windows",
        "виндовс": "windows",
        "win": "windows",
        "rhel": "rhel",
        "solaris": "solaris",
        "freebsd": "freebsd",
    }
    key = value.strip().lower()
    return mapping.get(key)


def _extract_os(raw: str, params: dict[str, str]) -> tuple[str | None, str | None]:
    os_name = _normalize_os(params.get("os"))
    os_version = params.get("os_version")
    if os_name:
        return os_name, os_version

    for regex in (_OS_PHRASE_RE, _KNOWN_OS_RE):
        for match in regex.finditer(raw):
            candidate = _normalize_os(match.group("os"))
            if candidate:
                return candidate, match.groupdict().get("osv")
    return None, None


def _parse_sort(raw: str, params: dict[str, str]) -> Literal["newest", "oldest", "name"]:
    if params.get("sort") in {"newest", "oldest", "name"}:
        return params["sort"]  # type: ignore[return-value]
    low = raw.lower()
    if any(marker in low for marker in ("по новизне", "сначала новые", "newest", "последн", "новейш", "свеж")):
        return "newest"
    if any(marker in low for marker in ("сначала старые", "старые", "старейш", "oldest")):
        return "oldest"
    if "по имени" in low or "sort=name" in low:
        return "name"
    return "newest"


def _parse_limit(raw_text: str, params: dict[str, str]) -> int:
    raw = params.get("limit")
    if raw:
        try:
            value = int(raw)
        except ValueError:
            return 30
        return max(1, min(100, value))

    match = re.search(
        r"(?:в\s+количестве|количеством|количество|limit)\s*(?:=|:)?\s*(?P<count>\d{1,3})|(?P<pieces>\d{1,3})\s*(?:штук|шт\b)",
        raw_text.lower(),
        re.IGNORECASE,
    )
    if not match:
        return 30
    value = int(match.group("count") or match.group("pieces"))
    return max(1, min(100, value))


def _parse_show(raw_text: str, params: dict[str, str]) -> int:
    raw = params.get("show")
    if raw:
        try:
            value = int(raw)
        except ValueError:
            return 5
        return max(1, min(50, value))

    match = re.search(r"(?:show|покажи|выведи|отобрази)\s*(?:=|:)?\s*(?P<count>\d{1,2})", raw_text.lower(), re.IGNORECASE)
    if not match:
        if re.search(r"\bпоследн(?:юю|ую|яя|ее|ий)\b", raw_text.lower(), re.IGNORECASE):
            return 1
        return 5
    value = int(match.group("count"))
    return max(1, min(50, value))


def _parse_source_name(raw_text: str, params: dict[str, str]) -> str | None:
    if params.get("source"):
        return params["source"]
    match = re.search(
        r"(?:с|из)\s+(?:ресурса|источника|source)\s+(?P<source>[a-zA-Z0-9_.-]+)",
        raw_text.lower(),
        re.IGNORECASE,
    )
    return match.group("source") if match else None


def _parse_format(raw: str, params: dict[str, str]) -> PackageFormat | None:
    fmt = params.get("format")
    if fmt in {"deb", "rpm", "exe"}:
        return fmt  # type: ignore[return-value]
    low = raw.lower()
    for candidate in ("deb", "rpm", "exe"):
        if re.search(rf"\b{candidate}\b", low):
            return candidate  # type: ignore[return-value]
    return None


def _parse_product(raw: str, params: dict[str, str]) -> str:
    if params.get("product"):
        return _normalize_product(params["product"])
    low = raw.lower()
    product_match = _KNOWN_PRODUCT_RE.search(low)
    if product_match:
        return _normalize_product(product_match.group(1))
    cleaned = re.sub(r"\b[a-z_]+\s*=\s*[a-z0-9._-]+\b", " ", low, flags=re.IGNORECASE)
    cleaned = _OS_PHRASE_RE.sub(" ", cleaned)
    cleaned = _KNOWN_OS_RE.sub(" ", cleaned)
    cleaned = _ANY_VERSION_RE.sub(" ", cleaned)
    cleaned = re.sub(
        r"\b(дай|дайте|мне|найди|найдите|покажи|покажите|скачай|скачать|нужен|нужна|нужно|"
        r"список|все|всех|пакет|пакеты|пакетов|версии|версий|версия|package|packages|version|versions|"
        r"give|me|find|show|get|need|list|download|for)\b",
        " ",
        cleaned,
        flags=re.IGNORECASE,
    )
    token_match = re.search(r"\b([a-z0-9][a-z0-9+_.-]{1,30}(?:\s+[a-z0-9][a-z0-9+_.-]{1,30}){0,4})\b", cleaned)
    return _normalize_product(token_match.group(1)) if token_match else "postgresql"


def _clean_product(value: str) -> str:
    cleaned = re.sub(r"\b[a-z_]+\s*=\s*[a-z0-9._-]+\b", " ", value, flags=re.IGNORECASE)
    cleaned = " ".join(cleaned.strip().split())
    while True:
        updated = _LEADING_REQUEST_WORDS_RE.sub("", cleaned).strip()
        if updated == cleaned:
            break
        cleaned = updated
    if _ONLY_REQUEST_WORD_RE.fullmatch(cleaned):
        return ""
    product_match = _KNOWN_PRODUCT_RE.search(cleaned)
    if product_match:
        return _normalize_product(product_match.group(1))
    return _normalize_product(cleaned)


def parse_scenario_query(text: str) -> ScenarioQuery | None:
    raw = (text or "").strip()
    if not raw:
        return None

    params = _extract_filter_params(raw)
    sort_by = _parse_sort(raw, params)
    limit = _parse_limit(raw, params)
    show = _parse_show(raw, params)
    package_format = _parse_format(raw, params)
    source_name = _parse_source_name(raw, params)
    product = _parse_product(raw, params)
    os_name, os_version = _extract_os(raw, params)

    m_specific = _PRODUCT_VERSION_OS_RE.search(raw)
    if m_specific:
        return ScenarioQuery(
            scenario_type="formats_by_version",
            product=_normalize_product(m_specific.group("product")),
            package_version=m_specific.group("version"),
            os=_normalize_os(m_specific.group("os")),
            os_version=m_specific.group("osv"),
            package_format=package_format,
            source_name=source_name,
            sort_by=sort_by,
            limit=limit,
            show=show,
            raw_query=raw,
        )

    m_versions_os = _VERSIONS_FOR_OS_RE.search(raw)
    if m_versions_os:
        return ScenarioQuery(
            scenario_type="versions_by_os",
            product=_normalize_product(m_versions_os.group("product")),
            os=_normalize_os(m_versions_os.group("os")),
            os_version=m_versions_os.group("osv"),
            package_format=package_format,
            source_name=source_name,
            sort_by=sort_by,
            limit=limit,
            show=show,
            raw_query=raw,
        )

    if _VERSION_HINT_RE.search(raw) and os_name:
        return ScenarioQuery(
            scenario_type="versions_by_os",
            product=product,
            os=os_name,
            os_version=os_version,
            package_format=package_format,
            source_name=source_name,
            sort_by=sort_by,
            limit=limit,
            show=show,
            raw_query=raw,
        )

    if _VERSION_HINT_RE.search(raw) and (params.get("product") or _KNOWN_PRODUCT_RE.search(raw)):
        return ScenarioQuery(
            scenario_type="formats_by_version",
            product=product,
            os=os_name,
            os_version=os_version,
            package_format=package_format,
            source_name=source_name,
            sort_by=sort_by,
            limit=limit,
            show=show,
            raw_query=raw,
        )

    if _INSTALL_HINT_RE.search(raw) and os_name and (params.get("product") or _KNOWN_PRODUCT_RE.search(raw)):
        return ScenarioQuery(
            scenario_type="versions_by_os",
            product=product,
            os=os_name,
            os_version=os_version,
            package_format=package_format,
            source_name=source_name,
            sort_by=sort_by,
            limit=limit,
            show=show,
            raw_query=raw,
        )

    m_packages_version = _PACKAGES_BY_VERSION_RE.search(raw)
    if m_packages_version:
        return ScenarioQuery(
            scenario_type="formats_by_version",
            product=_normalize_product(m_packages_version.group("product")),
            package_version=m_packages_version.group("version"),
            os=os_name,
            os_version=os_version,
            package_format=package_format,
            source_name=source_name,
            sort_by=sort_by,
            limit=limit,
            show=show,
            raw_query=raw,
        )

    for m_product_os in _PRODUCT_FOR_OS_RE.finditer(raw):
        normalized_os = _normalize_os(m_product_os.group("os"))
        if not normalized_os:
            continue
        product_by_os = _clean_product(m_product_os.group("product"))
        if product_by_os and (package_format or _KNOWN_PRODUCT_RE.search(product_by_os)):
            return ScenarioQuery(
                scenario_type="formats_by_version" if package_format else "versions_by_os",
                product=product_by_os,
                os=normalized_os,
                os_version=m_product_os.group("osv"),
                package_format=package_format,
                source_name=source_name,
                sort_by=sort_by,
                limit=limit,
                show=show,
                raw_query=raw,
            )

    any_version = _ANY_VERSION_RE.search(raw)
    if any_version:
        return ScenarioQuery(
            scenario_type="formats_by_version",
            product=product,
            package_version=any_version.group("version"),
            os=os_name,
            os_version=os_version,
            package_format=package_format,
            source_name=source_name,
            sort_by=sort_by,
            limit=limit,
            show=show,
            raw_query=raw,
        )

    return None
