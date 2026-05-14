from __future__ import annotations

import argparse
import json
import os
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


def _pid_alive(pid_file: Path) -> bool:
    if not pid_file.exists():
        return False
    raw = pid_file.read_text(encoding="utf-8", errors="ignore").strip()
    if not raw.isdigit():
        return False
    pid = int(raw)
    if os.name == "nt":
        import ctypes

        process_query_limited_information = 0x1000
        handle = ctypes.windll.kernel32.OpenProcess(process_query_limited_information, False, pid)
        if handle:
            ctypes.windll.kernel32.CloseHandle(handle)
            return True
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _check_pid(name: str, pid_file: Path, required: bool) -> CheckResult:
    if _pid_alive(pid_file):
        return CheckResult(name, "OK", f"process is running, pid_file={pid_file}")
    if required:
        return CheckResult(name, "FAIL", f"required process is not running, pid_file={pid_file}")
    return CheckResult(name, "WARN", f"component is disabled or optional, pid_file={pid_file}")


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


def _check_sheets(settings: IntegrationSettings, run_dir: Path) -> CheckResult:
    if not settings.sheets_sync_enabled:
        return CheckResult("sheets_sync", "WARN", "SHEETS_SYNC_ENABLED=false")
    if not settings.google_sheets_spreadsheet_id:
        return CheckResult("sheets_sync", "WARN", "GOOGLE_SHEETS_SPREADSHEET_ID is empty")
    if not settings.google_service_account_json_path:
        return CheckResult("sheets_sync", "WARN", "GOOGLE_SERVICE_ACCOUNT_JSON_PATH is empty")
    if not Path(settings.google_service_account_json_path).expanduser().exists():
        return CheckResult("sheets_sync", "FAIL", "Google service account JSON file does not exist")
    return _check_pid("sheets_sync", run_dir / "sheets_sync.pid", required=True)


def _check_telegram(settings: IntegrationSettings, run_dir: Path) -> CheckResult:
    if settings.bot_platform not in {"telegram", "both"}:
        return CheckResult("telegram_bot", "WARN", f"BOT_PLATFORM={settings.bot_platform}, Telegram is not required")
    if not settings.telegram_bot_token:
        return CheckResult("telegram_bot", "FAIL", "TELEGRAM_BOT_TOKEN is empty")
    return _check_pid("telegram_bot", run_dir / "bot.pid", required=True)


def _check_vk(settings: IntegrationSettings, run_dir: Path) -> CheckResult:
    if settings.bot_platform not in {"vk", "both"}:
        return CheckResult("vk_bot", "WARN", f"BOT_PLATFORM={settings.bot_platform}, VK is not required")
    if not settings.vk_bot_token:
        return CheckResult("vk_bot", "FAIL", "VK_BOT_TOKEN is empty")
    return _check_pid("vk_bot", run_dir / "vk_bot.pid", required=True)


def _check_ollama() -> CheckResult:
    provider = (os.getenv("LLM_PROVIDER") or "ollama").strip().lower()
    if _env_flag("DISABLE_LLM_ANSWERS", False):
        return CheckResult("llm_endpoint", "WARN", "DISABLE_LLM_ANSWERS=true, LLM formatting is not required")
    if provider in {"mistral", "custom"}:
        api_key = (os.getenv("MISTRAL_API_KEY") if provider == "mistral" else None) or os.getenv("API_KEY") or ""
        if not api_key.strip() or api_key.strip() == "local":
            return CheckResult("llm_endpoint", "FAIL", f"LLM_PROVIDER={provider} requires API_KEY")
        base_url = (os.getenv("BASE_URL") or "").rstrip("/")
        if provider == "mistral" and base_url in {"", "http://127.0.0.1:11434/v1", "http://localhost:11434/v1"}:
            base_url = "https://api.mistral.ai/v1"
        if not base_url:
            return CheckResult("llm_endpoint", "FAIL", f"LLM_PROVIDER={provider} requires BASE_URL")
        try:
            response = httpx.get(
                f"{base_url}/models",
                headers={"Authorization": f"Bearer {api_key}"},
                timeout=5.0,
                trust_env=False,
            )
            response.raise_for_status()
            return CheckResult("llm_endpoint", "OK", f"{provider} model endpoint is reachable: {base_url}/models")
        except Exception as exc:
            return CheckResult("llm_endpoint", "FAIL", f"LLM_PROVIDER={provider} endpoint is unavailable: {exc}")
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
    run_dir = REPO_ROOT / ".run"

    results = [
        _check_fastapi(settings),
        _check_pid("orchestrator", run_dir / "orchestrator.pid", required=True),
        _check_memgraph(),
        _check_sheets(settings, run_dir),
        _check_telegram(settings, run_dir),
        _check_vk(settings, run_dir),
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
