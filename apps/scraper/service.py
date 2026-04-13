from __future__ import annotations

import logging

import httpx

from apps.registry.models import RepositoryTemplate
from apps.scraper.models import PackageArtifact
from apps.scraper.parsers import (
    parse_apk_index,
    parse_deb_html_listing,
    parse_exe_html_listing,
    parse_rpm_html_listing,
)

logger = logging.getLogger(__name__)


class PackageScraperService:
    def __init__(self, timeout_sec: float = 30.0) -> None:
        self._timeout_sec = timeout_sec
        self._client = httpx.AsyncClient(timeout=timeout_sec, trust_env=False)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def fetch_from_template(
        self,
        template: RepositoryTemplate,
        *,
        product: str,
        requested_version: str | None = None,
    ) -> list[PackageArtifact]:
        try:
            if template.parser_type in {"deb_html", "rpm_html", "exe_html"}:
                assert template.list_url and template.base_url and template.source_url
                response = await self._client.get(template.list_url)
                response.raise_for_status()
                html = response.text
                if template.parser_type == "deb_html":
                    return parse_deb_html_listing(
                        html=html,
                        base_url=template.base_url,
                        source_name=template.source_name,
                        source_url=template.source_url,
                        product=product,
                        os_name=template.os,
                        os_version=template.os_version,
                        requested_version=requested_version,
                    )
                if template.parser_type == "rpm_html":
                    return parse_rpm_html_listing(
                        html=html,
                        base_url=template.base_url,
                        source_name=template.source_name,
                        source_url=template.source_url,
                        product=product,
                        os_name=template.os,
                        os_version=template.os_version,
                        requested_version=requested_version,
                    )
                return parse_exe_html_listing(
                    html=html,
                    base_url=template.base_url,
                    source_name=template.source_name,
                    source_url=template.source_url,
                    product=product,
                    os_name=template.os,
                    os_version=template.os_version,
                    requested_version=requested_version,
                )

            if template.parser_type == "apk_index":
                assert template.list_url and template.base_url and template.source_url
                response = await self._client.get(template.list_url)
                response.raise_for_status()
                return parse_apk_index(
                    apkindex_tar_gz=response.content,
                    base_url=template.base_url,
                    source_name=template.source_name,
                    source_url=template.source_url,
                    product=product,
                    os_name=template.os,
                    os_version=template.os_version,
                    requested_version=requested_version,
                )
        except Exception:
            logger.exception("Template scrape failed template_id=%s", template.template_id)
        return []
