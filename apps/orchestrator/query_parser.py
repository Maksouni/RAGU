from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel

ScenarioType = Literal["versions_by_os", "formats_by_version"]
PackageFormat = Literal["deb", "rpm", "apk", "exe"]

_PRODUCT_VERSION_OS_RE = re.compile(
    r"(?P<product>[a-zA-Zа-яА-Я0-9+_.-]+)\s+"
    r"(?P<version>\d+(?:\.\d+)+)\s+"
    r"(?:для|for)\s+"
    r"(?P<os>[a-zA-Zа-яА-Я]+)"
    r"(?:\s+(?P<osv>[0-9][0-9.]*))?",
    re.IGNORECASE,
)
_VERSIONS_FOR_OS_RE = re.compile(
    r"(?:все\s+)?верс\w+.*?"
    r"(?P<product>postgresql|python)\s+"
    r"(?:для|for)\s+"
    r"(?P<os>[a-zA-Zа-яА-Я]+)\s*"
    r"(?P<osv>[0-9][0-9.]*)?",
    re.IGNORECASE,
)
_PACKAGES_BY_VERSION_RE = re.compile(
    r"(?:все\s+)?пакет\w+.*?"
    r"(?P<product>postgresql|python)\s+"
    r"(?P<version>\d+(?:\.\d+)+)",
    re.IGNORECASE,
)
_ANY_VERSION_RE = re.compile(r"(?P<version>\d+(?:\.\d+)+)")


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


def _normalize_os(value: str | None) -> str | None:
    if not value:
        return None
    mapping = {
        "ubuntu": "ubuntu",
        "убунту": "ubuntu",
        "debian": "debian",
        "дебиан": "debian",
        "windows": "windows",
        "виндовс": "windows",
        "win": "windows",
        "rhel": "rhel",
        "alpine": "alpine",
    }
    key = value.strip().lower()
    return mapping.get(key, key)


def _parse_sort(raw: str, params: dict[str, str]) -> Literal["newest", "oldest", "name"]:
    if params.get("sort") in {"newest", "oldest", "name"}:
        return params["sort"]  # type: ignore[return-value]
    low = raw.lower()
    if "по новизне" in low or "сначала новые" in low or "newest" in low:
        return "newest"
    if "сначала старые" in low or "oldest" in low:
        return "oldest"
    if "по имени" in low or "sort=name" in low:
        return "name"
    return "newest"


def _parse_limit(params: dict[str, str]) -> int:
    raw = params.get("limit")
    if not raw:
        return 30
    try:
        value = int(raw)
    except ValueError:
        return 30
    return max(1, min(100, value))


def _parse_show(params: dict[str, str]) -> int:
    raw = params.get("show")
    if not raw:
        return 5
    try:
        value = int(raw)
    except ValueError:
        return 5
    return max(1, min(50, value))


def _parse_format(raw: str, params: dict[str, str]) -> PackageFormat | None:
    fmt = params.get("format")
    if fmt in {"deb", "rpm", "apk", "exe"}:
        return fmt  # type: ignore[return-value]
    low = raw.lower()
    for candidate in ("deb", "rpm", "apk", "exe"):
        if re.search(rf"\b{candidate}\b", low):
            return candidate  # type: ignore[return-value]
    return None


def _parse_product(raw: str, params: dict[str, str]) -> str:
    if params.get("product"):
        return params["product"].lower()
    low = raw.lower()
    if "postgresql" in low:
        return "postgresql"
    if "python" in low:
        return "python"
    token_match = re.search(r"\b([a-z0-9][a-z0-9+_.-]{1,30})\b", low)
    return token_match.group(1) if token_match else "postgresql"


def parse_scenario_query(text: str) -> ScenarioQuery | None:
    raw = (text or "").strip()
    if not raw:
        return None

    params = _extract_filter_params(raw)
    sort_by = _parse_sort(raw, params)
    limit = _parse_limit(params)
    show = _parse_show(params)
    package_format = _parse_format(raw, params)
    source_name = params.get("source")
    product = _parse_product(raw, params)
    os_name = _normalize_os(params.get("os"))
    os_version = params.get("os_version")

    m_specific = _PRODUCT_VERSION_OS_RE.search(raw)
    if m_specific:
        return ScenarioQuery(
            scenario_type="formats_by_version",
            product=m_specific.group("product").lower(),
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
            product=m_versions_os.group("product").lower(),
            os=_normalize_os(m_versions_os.group("os")),
            os_version=m_versions_os.group("osv"),
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
            product=m_packages_version.group("product").lower(),
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
