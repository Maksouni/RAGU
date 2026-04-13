from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

ParserType = Literal["deb_html", "rpm_html", "apk_index", "exe_html"]


class RepositoryTemplate(BaseModel):
    template_id: str
    source_name: str
    package_format: Literal["deb", "rpm", "apk", "exe"]
    parser_type: ParserType
    product_hint: str = "postgresql"
    os: str | None = None
    os_version: str | None = None
    list_url: str | None = None
    source_url: str | None = None
    base_url: str | None = None
    enabled: bool = True
    metadata: dict[str, str] = Field(default_factory=dict)
