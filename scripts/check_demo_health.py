from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal

import httpx
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from apps.common.settings import IntegrationSettings

Status = Literal["OK", "WARN", "FAIL"]


@dataclass
class CheckResult:
    name: str
    status: Status
    message: str


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _check_compose_service(name: str, required: bool) -> CheckResult:
    compose_file = REPO_ROOT / "examples" / "fastapi_demo" / "docker-compose.yml"
    try:
        result = subprocess.run(
            ["docker", "compose", "-f", str(compose_file), "ps", "--status", "running", "--services"],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except Exception as exc:
        if required:
            return CheckResult(name, "FAIL", f"docker compose status is unavailable: {exc}")
        return CheckResult(name, "WARN", f"optional component is disabled or docker is unavailable: {exc}")

    if result.returncode != 0:
        message = (result.stderr or result.stdout or "").strip()[:300]
        if required:
            return CheckResult(name, "FAIL", f"docker compose status failed: {message}")
        return CheckResult(name, "WARN", f"optional component is disabled or docker status failed: {message}")

    running = {line.strip() for line in result.stdout.splitlines() if line.strip()}
    if name in running:
        return CheckResult(name, "OK", "compose service is running")
    if required:
        return CheckResult(name, "FAIL", "required compose service is not running")
    return CheckResult(name, "WARN", "optional compose service is not running")


def _check_fastapi(settings: IntegrationSettings) -> CheckResult:
    try:
        response = httpx.get(f"{settings.api_base_url.rstrip('/')}/status", timeout=3.0)
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        return CheckResult("fastapi_status", "FAIL", f"/status is unavailable: {exc}")

    embedding_dim = int(payload.get("embedding_dim") or 0)
    if embedding_dim > 20:
        return CheckResult("fastapi_status", "FAIL", f"/status OK, but embedding_dim={embedding_dim} > 20")
    return CheckResult("fastapi_status", "OK", f"/status OK, default_answer_mode={payload.get('default_answer_mode')}")


def _check_memgraph() -> CheckResult:
    try:
        from neo4j import GraphDatabase

        uri = os.getenv("MEMGRAPH_URI", "bolt://127.0.0.1:7687")
        username = os.getenv("MEMGRAPH_USERNAME") or None
        password = os.getenv("MEMGRAPH_PASSWORD") or None
        auth = (username, password) if username or password else None
        with GraphDatabase.driver(uri, auth=auth, connection_timeout=3.0) as driver:
            driver.verify_connectivity()
        return CheckResult("memgraph_bolt", "OK", f"Bolt connectivity OK: {uri}")
    except Exception as exc:
        return CheckResult("memgraph_bolt", "FAIL", f"Bolt connectivity failed: {exc}")


def _check_sheets(settings: IntegrationSettings) -> CheckResult:
    if not settings.sheets_sync_enabled:
        return CheckResult("sheets_sync", "WARN", "SHEETS_SYNC_ENABLED=false")
    if not settings.google_sheets_spreadsheet_id:
        return CheckResult("sheets_sync", "WARN", "GOOGLE_SHEETS_SPREADSHEET_ID is empty")
    if not settings.google_service_account_json_path:
        return CheckResult("sheets_sync", "WARN", "GOOGLE_SERVICE_ACCOUNT_JSON_PATH is empty")
    if not Path(settings.google_service_account_json_path).expanduser().exists():
        return CheckResult("sheets_sync", "FAIL", "Google service account JSON file does not exist")
    return _check_compose_service("sheets_sync", required=True)


def _check_vk(settings: IntegrationSettings) -> CheckResult:
    if not settings.vk_bot_enabled:
        return CheckResult("vk_bot", "WARN", "VK_BOT_ENABLED=false")
    if not settings.vk_bot_token:
        return CheckResult("vk_bot", "FAIL", "VK_BOT_TOKEN is empty")
    return _check_compose_service("vk_bot", required=True)


def _check_ollama() -> CheckResult:
    provider = (os.getenv("LLM_PROVIDER") or "ollama").strip().lower()
    if _env_flag("DISABLE_LLM_ANSWERS", False):
        return CheckResult("llm_endpoint", "WARN", "DISABLE_LLM_ANSWERS=true, LLM formatting is not required")
    if provider != "ollama":
        return CheckResult("llm_endpoint", "FAIL", "Only LLM_PROVIDER=ollama is supported")
    base_url = (os.getenv("BASE_URL") or "http://127.0.0.1:11434/v1").rstrip("/")
    try:
        response = httpx.get(f"{base_url}/models", timeout=3.0, trust_env=False)
        response.raise_for_status()
        return CheckResult("llm_endpoint", "OK", f"Ollama model endpoint is reachable: {base_url}/models")
    except Exception as exc:
        return CheckResult("llm_endpoint", "FAIL", f"LLM is enabled but Ollama endpoint is unavailable: {exc}")


def _summary(results: list[CheckResult]) -> Status:
    if any(item.status == "FAIL" for item in results):
        return "FAIL"
    if any(item.status == "WARN" for item in results):
        return "WARN"
    return "OK"


def main() -> int:
    parser = argparse.ArgumentParser(description="Check local RAGU demo health.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    args = parser.parse_args()

    load_dotenv(REPO_ROOT / ".env", override=True)
    settings = IntegrationSettings()

    results = [
        _check_fastapi(settings),
        _check_compose_service("orchestrator", required=True),
        _check_memgraph(),
        _check_sheets(settings),
        _check_vk(settings),
        _check_ollama(),
    ]
    summary = _summary(results)

    if args.json:
        print(json.dumps({"summary": summary, "checks": [asdict(item) for item in results]}, ensure_ascii=False, indent=2))
    else:
        print(f"RAGU demo health: {summary}")
        for item in results:
            print(f"[{item.status}] {item.name}: {item.message}")
    return 2 if summary == "FAIL" else 0


if __name__ == "__main__":
    raise SystemExit(main())
