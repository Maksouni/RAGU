from __future__ import annotations

import gzip
import re
from fnmatch import fnmatchcase
from urllib.parse import urljoin

from apps.scraper.models import PackageArtifact

_DEB_LINK_RE = re.compile(r'href="([^"]+\.deb)"', re.IGNORECASE)
_RPM_LINK_RE = re.compile(r'href="([^"]+\.rpm)"', re.IGNORECASE)
_EXE_LINK_RE = re.compile(r'href="([^"]+\.exe)"', re.IGNORECASE)
_VER_RE = re.compile(r"(\d+(?:\.\d+)+)")
_SKIP_BINARY_SUFFIXES = ("-dbgsym", "-dbg", "-debug", "-doc", "-docs")


def _version_match(version: str, requested_version: str | None) -> bool:
    if not requested_version:
        return True
    cleaned = version.split(":")[-1]
    return cleaned.startswith(requested_version)


def parse_deb_html_listing(
    *,
    html: str,
    base_url: str,
    source_name: str,
    source_url: str,
    product: str,
    os_name: str | None,
    os_version: str | None,
    requested_version: str | None = None,
) -> list[PackageArtifact]:
    artifacts: list[PackageArtifact] = []
    product_norm = product.lower()
    for filename in sorted(set(_DEB_LINK_RE.findall(html))):
        if product_norm not in filename.lower():
            continue
        if "_" not in filename:
            continue
        name, version, _rest = filename.split("_", 2)
        if not _version_match(version, requested_version):
            continue
        artifacts.append(
            PackageArtifact(
                source_name=source_name,
                package_name=name,
                package_version=version,
                package_format="deb",
                artifact_url=urljoin(base_url, filename),
                source_url=source_url,
                os=os_name,
                os_version=os_version,
            )
        )
    return artifacts


def _decode_packages_index(data: bytes | str) -> str:
    if isinstance(data, str):
        return data
    if data.startswith(b"\x1f\x8b"):
        data = gzip.decompress(data)
    return data.decode("utf-8", errors="replace")


def _parse_control_paragraphs(text: str) -> list[dict[str, str]]:
    paragraphs: list[dict[str, str]] = []
    current: dict[str, str] = {}
    current_key: str | None = None
    for raw_line in text.splitlines():
        if not raw_line.strip():
            if current:
                paragraphs.append(current)
                current = {}
                current_key = None
            continue
        if raw_line.startswith((" ", "\t")) and current_key:
            current[current_key] = f"{current[current_key]}\n{raw_line.strip()}"
            continue
        if ":" not in raw_line:
            continue
        key, value = raw_line.split(":", 1)
        current_key = key.strip()
        current[current_key] = value.strip()
    if current:
        paragraphs.append(current)
    return paragraphs


def _package_name_allowed(package_name: str, allowed_names: list[str] | None) -> bool:
    if allowed_names:
        return any(fnmatchcase(package_name, allowed_name) for allowed_name in allowed_names)
    return not package_name.endswith(_SKIP_BINARY_SUFFIXES)


def parse_deb_packages_index(
    *,
    data: bytes | str,
    base_url: str,
    source_name: str,
    source_url: str,
    product: str,
    os_name: str | None,
    os_version: str | None,
    requested_version: str | None = None,
    package_names: list[str] | None = None,
) -> list[PackageArtifact]:
    artifacts: list[PackageArtifact] = []
    product_norm = product.lower()
    for paragraph in _parse_control_paragraphs(_decode_packages_index(data)):
        package_name = (paragraph.get("Package") or "").strip()
        source_package = (paragraph.get("Source") or "").strip().split(" ", 1)[0]
        version = (paragraph.get("Version") or "").strip()
        filename = (paragraph.get("Filename") or "").strip()
        if not package_name or not version or not filename:
            continue
        if product_norm not in package_name.lower() and product_norm not in source_package.lower():
            continue
        if not _package_name_allowed(package_name, package_names):
            continue
        if not _version_match(version, requested_version):
            continue
        artifacts.append(
            PackageArtifact(
                source_name=source_name,
                package_name=package_name,
                package_version=version,
                package_format="deb",
                artifact_url=urljoin(base_url.rstrip("/") + "/", filename),
                source_url=source_url,
                os=os_name,
                os_version=os_version,
            )
        )
    return sorted(
        artifacts,
        key=lambda item: (0 if product_norm in item.package_name.lower() else 1, item.package_name),
    )


def parse_rpm_html_listing(
    *,
    html: str,
    base_url: str,
    source_name: str,
    source_url: str,
    product: str,
    os_name: str | None,
    os_version: str | None,
    requested_version: str | None = None,
) -> list[PackageArtifact]:
    artifacts: list[PackageArtifact] = []
    product_norm = product.lower()
    for filename in sorted(set(_RPM_LINK_RE.findall(html))):
        if product_norm not in filename.lower():
            continue
        stem = filename[:-4]
        first_dash = stem.find("-")
        if first_dash <= 0:
            continue
        package_name = stem[:first_dash]
        ver_match = _VER_RE.search(stem)
        version = ver_match.group(1) if ver_match else stem[first_dash + 1 :]
        if not _version_match(version, requested_version):
            continue
        artifacts.append(
            PackageArtifact(
                source_name=source_name,
                package_name=package_name,
                package_version=version,
                package_format="rpm",
                artifact_url=urljoin(base_url, filename),
                source_url=source_url,
                os=os_name,
                os_version=os_version,
            )
        )
    return artifacts


def parse_exe_html_listing(
    *,
    html: str,
    base_url: str,
    source_name: str,
    source_url: str,
    product: str,
    os_name: str | None,
    os_version: str | None,
    requested_version: str | None = None,
) -> list[PackageArtifact]:
    artifacts: list[PackageArtifact] = []
    product_norm = product.lower()
    for href in sorted(set(_EXE_LINK_RE.findall(html))):
        filename = href.split("/")[-1]
        if product_norm not in filename.lower():
            continue
        version_match = _VER_RE.search(filename)
        version = version_match.group(1) if version_match else "unknown"
        if not _version_match(version, requested_version):
            continue
        name = filename.rsplit(".", 1)[0]
        artifacts.append(
            PackageArtifact(
                source_name=source_name,
                package_name=name,
                package_version=version,
                package_format="exe",
                artifact_url=urljoin(base_url, href),
                source_url=source_url,
                os=os_name,
                os_version=os_version,
            )
        )
    return artifacts
