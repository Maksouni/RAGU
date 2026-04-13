from __future__ import annotations

import re
import tarfile
from io import BytesIO
from urllib.parse import urljoin

from apps.scraper.models import PackageArtifact

_DEB_LINK_RE = re.compile(r'href="([^"]+\.deb)"', re.IGNORECASE)
_RPM_LINK_RE = re.compile(r'href="([^"]+\.rpm)"', re.IGNORECASE)
_EXE_LINK_RE = re.compile(r'href="([^"]+\.exe)"', re.IGNORECASE)
_VER_RE = re.compile(r"(\d+(?:\.\d+)+)")


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


def parse_apk_index(
    *,
    apkindex_tar_gz: bytes,
    base_url: str,
    source_name: str,
    source_url: str,
    product: str,
    os_name: str | None,
    os_version: str | None,
    requested_version: str | None = None,
) -> list[PackageArtifact]:
    with tarfile.open(fileobj=BytesIO(apkindex_tar_gz), mode="r:gz") as tar:
        member = tar.getmember("APKINDEX")
        raw_text = tar.extractfile(member).read().decode("utf-8", errors="replace")

    artifacts: list[PackageArtifact] = []
    product_norm = product.lower()
    chunks = raw_text.split("\n\n")
    for chunk in chunks:
        fields = {}
        for line in chunk.splitlines():
            if ":" not in line:
                continue
            key, value = line.split(":", 1)
            fields[key] = value
        name = fields.get("P", "")
        version = fields.get("V", "")
        if not name or not version:
            continue
        if product_norm not in name.lower():
            continue
        if not _version_match(version, requested_version):
            continue
        artifacts.append(
            PackageArtifact(
                source_name=source_name,
                package_name=name,
                package_version=version,
                package_format="apk",
                artifact_url=urljoin(base_url, f"{name}-{version}.apk"),
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
