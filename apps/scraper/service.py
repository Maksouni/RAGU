from __future__ import annotations

import asyncio
import logging

import httpx

from apps.registry.models import RepositoryTemplate
from apps.scraper.models import PackageArtifact
from apps.scraper.parsers import (
    parse_deb_packages_index,
    parse_deb_html_listing,
    parse_exe_html_listing,
    parse_rpm_html_listing,
)

logger = logging.getLogger(__name__)


class PackageScraperService:
    def __init__(self, timeout_sec: float = 30.0, max_attempts: int = 3, backoff_base_sec: float = 0.5) -> None:
        self._timeout_sec = timeout_sec
        self._max_attempts = max(1, max_attempts)
        self._backoff_base_sec = max(0.0, backoff_base_sec)
        self._last_errors: dict[str, str] = {}
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout_sec, connect=min(10.0, timeout_sec)),
            follow_redirects=True,
            trust_env=False,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def fetch_from_template(
        self,
        template: RepositoryTemplate,
        *,
        product: str,
        requested_version: str | None = None,
    ) -> list[PackageArtifact]:
        self._last_errors.pop(template.template_id, None)
        try:
            if template.parser_type in {"deb_html", "deb_packages_gz", "rpm_html", "exe_html"}:
                assert template.list_url and template.base_url and template.source_url
                response = await self._get_with_retry(template.list_url, template.template_id)
                if template.parser_type == "deb_packages_gz":
                    package_names = [
                        item.strip()
                        for item in template.metadata.get("package_names", "").split(",")
                        if item.strip()
                    ]
                    return parse_deb_packages_index(
                        data=response.content,
                        base_url=template.base_url,
                        source_name=template.source_name,
                        source_url=template.source_url,
                        product=product,
                        os_name=template.os,
                        os_version=template.os_version,
                        requested_version=requested_version,
                        package_names=package_names or None,
                    )
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

        except Exception as exc:
            self._last_errors[template.template_id] = str(exc)[:500]
            logger.exception(
                "Template scrape failed template_id=%s product=%s requested_version=%s error=%s",
                template.template_id,
                product,
                requested_version,
                exc,
            )
        return []

    def last_errors_for(self, template_ids: list[str]) -> dict[str, str]:
        return {template_id: self._last_errors[template_id] for template_id in template_ids if template_id in self._last_errors}

    async def _get_with_retry(self, url: str, template_id: str) -> httpx.Response:
        last_error: Exception | None = None
        for attempt in range(1, self._max_attempts + 1):
            try:
                response = await self._client.get(url)
                response.raise_for_status()
                return response
            except (httpx.TimeoutException, httpx.TransportError, httpx.HTTPStatusError) as exc:
                last_error = exc
                if attempt >= self._max_attempts:
                    break
                delay = self._backoff_base_sec * (2 ** (attempt - 1))
                logger.warning(
                    "Source fetch failed template_id=%s attempt=%s/%s url=%s error=%s; retrying in %.1fs",
                    template_id,
                    attempt,
                    self._max_attempts,
                    url,
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)
        assert last_error is not None
        raise last_error
