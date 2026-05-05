from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

AskMode = Literal["local", "global"]
AnswerMode = Literal["auto", "llm", "no_llm"]
BotPlatform = Literal["telegram", "vk", "both", "none"]


class IntegrationSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    api_base_url: str = Field(default="http://127.0.0.1:8000", validation_alias=AliasChoices("API_BASE_URL"))
    default_ask_mode: AskMode = Field(default="local", validation_alias=AliasChoices("DEFAULT_ASK_MODE"))
    default_answer_mode: AnswerMode = Field(
        default="auto",
        validation_alias=AliasChoices("DEFAULT_ANSWER_MODE"),
    )
    ask_timeout_sec: float = Field(default=60.0, validation_alias=AliasChoices("ASK_TIMEOUT_SEC"))
    ingest_timeout_sec: float = Field(default=60.0, validation_alias=AliasChoices("INGEST_TIMEOUT_SEC"))
    ingest_interval_sec: int = Field(default=15, validation_alias=AliasChoices("INGEST_INTERVAL_SEC"))
    ingest_batch_size: int = Field(default=20, validation_alias=AliasChoices("INGEST_BATCH_SIZE"))
    ingest_max_attempts: int = Field(default=10, validation_alias=AliasChoices("INGEST_MAX_ATTEMPTS"))
    ingest_backoff_base_sec: int = Field(default=3, validation_alias=AliasChoices("INGEST_BACKOFF_BASE_SEC"))
    ingest_backoff_max_sec: int = Field(default=120, validation_alias=AliasChoices("INGEST_BACKOFF_MAX_SEC"))

    outbox_db_path: str = Field(
        default="ragu_working_dir/integration/outbox.sqlite",
        validation_alias=AliasChoices("OUTBOX_DB_PATH"),
    )

    bot_platform: BotPlatform = Field(default="telegram", validation_alias=AliasChoices("BOT_PLATFORM"))

    telegram_bot_token: str = Field(default="", validation_alias=AliasChoices("TELEGRAM_BOT_TOKEN"))
    bot_polling_timeout_sec: int = Field(default=30, validation_alias=AliasChoices("BOT_POLLING_TIMEOUT_SEC"))

    vk_bot_token: str = Field(default="", validation_alias=AliasChoices("VK_BOT_TOKEN"))
    vk_group_id: str = Field(default="", validation_alias=AliasChoices("VK_GROUP_ID"))
    vk_api_version: str = Field(default="5.199", validation_alias=AliasChoices("VK_API_VERSION"))
    vk_long_poll_wait_sec: int = Field(default=25, validation_alias=AliasChoices("VK_LONG_POLL_WAIT_SEC"))

    sheets_sync_enabled: bool = Field(default=True, validation_alias=AliasChoices("SHEETS_SYNC_ENABLED"))
    sheets_sync_interval_sec: int = Field(default=15, validation_alias=AliasChoices("SHEETS_SYNC_INTERVAL_SEC"))
    google_sheets_spreadsheet_id: str = Field(
        default="",
        validation_alias=AliasChoices("GOOGLE_SHEETS_SPREADSHEET_ID"),
    )
    google_service_account_json_path: str = Field(
        default="",
        validation_alias=AliasChoices("GOOGLE_SERVICE_ACCOUNT_JSON_PATH"),
    )

    log_level: str = Field(default="INFO", validation_alias=AliasChoices("LOG_LEVEL"))

    def resolve_outbox_db_path(self) -> Path:
        return Path(self.outbox_db_path).expanduser().resolve()


@lru_cache(maxsize=1)
def get_settings() -> IntegrationSettings:
    return IntegrationSettings()
