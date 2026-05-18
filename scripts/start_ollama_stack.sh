#!/usr/bin/env bash
set -Eeuo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
compose_file="$repo_root/examples/fastapi_demo/docker-compose.yml"
python_bin="$repo_root/venv/bin/python"
env_file="$repo_root/.env"
run_dir="$repo_root/.run"

pid_file="$run_dir/fastapi_demo.pid"
stdout_log="$run_dir/fastapi_demo.stdout.log"
stderr_log="$run_dir/fastapi_demo.stderr.log"
orchestrator_pid_file="$run_dir/orchestrator.pid"
orchestrator_stdout_log="$run_dir/orchestrator.stdout.log"
orchestrator_stderr_log="$run_dir/orchestrator.stderr.log"
bot_pid_file="$run_dir/bot.pid"
bot_stdout_log="$run_dir/bot.stdout.log"
bot_stderr_log="$run_dir/bot.stderr.log"
vk_bot_pid_file="$run_dir/vk_bot.pid"
vk_bot_stdout_log="$run_dir/vk_bot.stdout.log"
vk_bot_stderr_log="$run_dir/vk_bot.stderr.log"
sheets_pid_file="$run_dir/sheets_sync.pid"
sheets_stdout_log="$run_dir/sheets_sync.stdout.log"
sheets_stderr_log="$run_dir/sheets_sync.stderr.log"
ollama_pid_file="$run_dir/ollama.pid"
ollama_stdout_log="$run_dir/ollama.stdout.log"
ollama_stderr_log="$run_dir/ollama.stderr.log"

die() {
    echo "ERROR: $*" >&2
    exit 1
}

is_truthy() {
    case "${1:-}" in
        1|true|TRUE|True|yes|YES|on|ON) return 0 ;;
        *) return 1 ;;
    esac
}

load_dotenv() {
    local file="$1"
    [[ -f "$file" ]] || die "Missing .env file: $file"
    while IFS= read -r raw_line || [[ -n "$raw_line" ]]; do
        local line="${raw_line#"${raw_line%%[![:space:]]*}"}"
        line="${line%"${line##*[![:space:]]}"}"
        [[ -z "$line" || "${line:0:1}" == "#" ]] && continue
        [[ "$line" =~ ^[A-Za-z_][A-Za-z0-9_]*= ]] || continue
        local key="${line%%=*}"
        local value="${line#*=}"
        value="${value#"${value%%[![:space:]]*}"}"
        value="${value%"${value##*[![:space:]]}"}"
        if [[ "${value:0:1}" == "\"" && "${value: -1}" == "\"" ]]; then
            value="${value:1:${#value}-2}"
        elif [[ "${value:0:1}" == "'" && "${value: -1}" == "'" ]]; then
            value="${value:1:${#value}-2}"
        fi
        export "$key=$value"
    done < "$file"
}

env_default() {
    local name="$1"
    local value="$2"
    if [[ -z "${!name:-}" ]]; then
        export "$name=$value"
    fi
}

clear_dead_local_proxy() {
    local names=(HTTP_PROXY HTTPS_PROXY ALL_PROXY http_proxy https_proxy all_proxy GIT_HTTP_PROXY GIT_HTTPS_PROXY)
    local name
    for name in "${names[@]}"; do
        if [[ "${!name:-}" =~ 127\.0\.0\.1:9 ]]; then
            unset "$name"
        fi
    done
}

docker_ready() {
    docker version >/dev/null 2>&1
}

ensure_docker_ready() {
    if docker_ready; then
        return
    fi

    if [[ "$(uname -s)" == "Darwin" ]]; then
        echo "Docker API is not ready, trying to start Docker Desktop..."
        open -a Docker >/dev/null 2>&1 || true
        for _ in $(seq 1 60); do
            sleep 3
            docker_ready && return
        done
    fi

    die "Docker API is not reachable. Start Docker Desktop or Docker daemon and rerun this script."
}

compose_up() {
    docker compose -f "$compose_file" "$@"
}

wait_tcp_port() {
    local host="$1"
    local port="$2"
    local timeout_sec="$3"
    local name="$4"
    "$python_bin" - "$host" "$port" "$timeout_sec" "$name" <<'PY'
import socket
import sys
import time

host, port, timeout_sec, name = sys.argv[1], int(sys.argv[2]), float(sys.argv[3]), sys.argv[4]
deadline = time.time() + timeout_sec
while time.time() < deadline:
    try:
        with socket.create_connection((host, port), timeout=1):
            raise SystemExit(0)
    except OSError:
        time.sleep(1)
raise SystemExit(f"{name} did not open {host}:{port} within {int(timeout_sec)}s.")
PY
}

is_managed_project_process() {
    local pid="$1"
    [[ "$pid" =~ ^[0-9]+$ ]] || return 1
    kill -0 "$pid" >/dev/null 2>&1 || return 1
    local cmdline
    cmdline="$(ps -p "$pid" -o command= 2>/dev/null || true)"
    [[ -n "$cmdline" ]] || return 1
    [[ "$cmdline" == *"$repo_root"* || "$cmdline" == *"$repo_root/venv"* ]]
}

stop_process_by_pid_file() {
    local pid_path="$1"
    local name="$2"
    [[ -f "$pid_path" ]] || return
    local pid
    pid="$(head -n 1 "$pid_path" 2>/dev/null || true)"
    if [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" >/dev/null 2>&1; then
        if is_managed_project_process "$pid"; then
            echo "Stopping old $name process PID=$pid ..."
            kill "$pid" >/dev/null 2>&1 || true
            for _ in $(seq 1 10); do
                kill -0 "$pid" >/dev/null 2>&1 || break
                sleep 1
            done
            kill -0 "$pid" >/dev/null 2>&1 && kill -9 "$pid" >/dev/null 2>&1 || true
        else
            echo "$name PID file is stale: PID=$pid is not a managed project process. Removing PID file only."
        fi
    fi
    rm -f "$pid_path"
}

stop_listener_on_port() {
    local port="$1"
    if ! command -v lsof >/dev/null 2>&1; then
        echo "lsof is not available, skip port listener cleanup for $port."
        return
    fi
    local pids pid
    pids="$(lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true)"
    for pid in $pids; do
        if is_managed_project_process "$pid"; then
            echo "Stopping managed listener on port $port (PID=$pid) ..."
            kill "$pid" >/dev/null 2>&1 || true
        else
            echo "Port $port is used by PID=$pid, but it is not a managed project process. Leaving it running."
        fi
    done
}

start_managed_process() {
    local name="$1"
    local pid_path="$2"
    local stdout_path="$3"
    local stderr_path="$4"
    shift 4

    stop_process_by_pid_file "$pid_path" "$name"
    rm -f "$stdout_path" "$stderr_path"

    echo "Starting $name..."
    (
        cd "$repo_root"
        nohup "$python_bin" "$@" >"$stdout_path" 2>"$stderr_path" &
        echo $! >"$pid_path"
    )

    local pid
    pid="$(cat "$pid_path")"
    sleep 2
    if ! kill -0 "$pid" >/dev/null 2>&1; then
        die "$name process exited early. Check logs: $stdout_path and $stderr_path"
    fi
}

python_module_present() {
    local module="$1"
    "$python_bin" -c "import importlib.util, sys; sys.exit(0 if importlib.util.find_spec('$module') else 1)" >/dev/null 2>&1
}

ollama_tags_json() {
    local api_root="$1"
    "$python_bin" - "$api_root" <<'PY'
import json
import sys
import urllib.request

api_root = sys.argv[1].rstrip("/")
try:
    with urllib.request.urlopen(api_root + "/api/tags", timeout=4) as resp:
        print(resp.read().decode("utf-8"))
except Exception:
    print("{}")
PY
}

ollama_tags() {
    local api_root="$1"
    ollama_tags_json "$api_root" | "$python_bin" -c "import json,sys; data=json.load(sys.stdin); print('\n'.join(str(m.get('name','')) for m in data.get('models', [])))" 2>/dev/null || true
}

ollama_model_present() {
    local model="$1"
    local tags="$2"
    grep -Fxq "$model" <<<"$tags" && return 0
    [[ "$model" != *:* ]] && grep -Fxq "$model:latest" <<<"$tags" && return 0
    return 1
}

ensure_ollama_api_ready() {
    local ollama_bin="$1"
    local api_root="$2"
    local tags
    tags="$(ollama_tags "$api_root")"
    [[ -n "$tags" ]] && return

    echo "Ollama API is not ready, starting ollama serve..."
    rm -f "$ollama_stdout_log" "$ollama_stderr_log"
    nohup "$ollama_bin" serve >"$ollama_stdout_log" 2>"$ollama_stderr_log" &
    echo $! >"$ollama_pid_file"

    for _ in $(seq 1 45); do
        sleep 1
        tags="$(ollama_tags "$api_root")"
        [[ -n "$tags" ]] && return
    done
    die "Ollama API is not reachable at $api_root/api/tags. Check logs: $ollama_stdout_log and $ollama_stderr_log"
}

[[ -x "$python_bin" ]] || die "Python from venv not found: $python_bin"
command -v docker >/dev/null 2>&1 || die "Docker is not installed or not in PATH."
[[ -f "$env_file" ]] || die "Missing .env file: $env_file"

mkdir -p "$run_dir"
load_dotenv "$env_file"
clear_dead_local_proxy
ensure_docker_ready

env_default API_KEY "local"
env_default LLM_PROVIDER "ollama"
env_default BASE_URL "http://127.0.0.1:11434/v1"
env_default EMBEDDING_BASE_URL "http://127.0.0.1:11434/v1"
env_default LLM_MODEL_NAME "qwen2.5:3b"
env_default EMBEDDER_MODEL_NAME "nomic-embed-text"
env_default MEMGRAPH_URI "bolt://127.0.0.1:7687"
env_default OLLAMA_AUTO_PULL "false"
env_default MEMGRAPH_LAB_ENABLED "true"
env_default BOT_PLATFORM "telegram"
env_default SEED_DEMO_IT_KNOWLEDGE "true"
env_default FASTAPI_START_TIMEOUT_SEC "120"

if is_truthy "${SHEETS_SYNC_ENABLED:-false}" && [[ -n "${GOOGLE_SERVICE_ACCOUNT_JSON_PATH:-}" && ! -f "${GOOGLE_SERVICE_ACCOUNT_JSON_PATH:-}" ]]; then
    echo "WARNING: Google Sheets credentials file does not exist: $GOOGLE_SERVICE_ACCOUNT_JSON_PATH"
    echo "Sheets worker will stay alive, but rows cannot sync until GOOGLE_SERVICE_ACCOUNT_JSON_PATH is fixed."
fi

disable_llm=false
is_truthy "${DISABLE_LLM_ANSWERS:-false}" && disable_llm=true
llm_provider="$(printf '%s' "${LLM_PROVIDER:-ollama}" | tr '[:upper:]' '[:lower:]')"
case "$llm_provider" in
    ollama|mistral|custom) ;;
    *) die "Unsupported LLM_PROVIDER='$llm_provider'. Use ollama, mistral, or custom." ;;
esac

if [[ "$llm_provider" == "mistral" ]]; then
    if [[ -n "${MISTRAL_API_KEY:-}" ]]; then
        export API_KEY="$MISTRAL_API_KEY"
    fi
    [[ -z "${BASE_URL:-}" || "$BASE_URL" =~ 127\.0\.0\.1:11434|localhost:11434 ]] && export BASE_URL="https://api.mistral.ai/v1"
    [[ -z "${EMBEDDING_BASE_URL:-}" || "$EMBEDDING_BASE_URL" =~ 127\.0\.0\.1:11434|localhost:11434 ]] && export EMBEDDING_BASE_URL="https://api.mistral.ai/v1"
    [[ -z "${LLM_MODEL_NAME:-}" || "$LLM_MODEL_NAME" == "qwen2.5:3b" ]] && export LLM_MODEL_NAME="mistral-small-latest"
    [[ -z "${EMBEDDER_MODEL_NAME:-}" || "$EMBEDDER_MODEL_NAME" == "nomic-embed-text" ]] && export EMBEDDER_MODEL_NAME="mistral-embed"
    [[ -n "${API_KEY:-}" && "$API_KEY" != "local" ]] || die "LLM_PROVIDER=mistral requires MISTRAL_API_KEY or API_KEY in .env."
    echo "LLM_PROVIDER=mistral -> skipping local Ollama startup and using Mistral API endpoints."
elif [[ "$llm_provider" == "custom" ]]; then
    [[ -n "${API_KEY:-}" && -n "${BASE_URL:-}" && -n "${EMBEDDING_BASE_URL:-}" ]] || die "LLM_PROVIDER=custom requires API_KEY, BASE_URL, and EMBEDDING_BASE_URL in .env."
    echo "LLM_PROVIDER=custom -> skipping local Ollama startup and using configured OpenAI-compatible endpoints."
else
    command -v ollama >/dev/null 2>&1 || die "Ollama is not installed or not in PATH. Install Ollama or set LLM_PROVIDER=mistral/custom."
    ollama_bin="$(command -v ollama)"
    ollama_api_root="${BASE_URL%/}"
    ollama_api_root="${ollama_api_root%/v1}"
    ensure_ollama_api_ready "$ollama_bin" "$ollama_api_root"
    $disable_llm && echo "DISABLE_LLM_ANSWERS=true -> LLM generation is disabled, embeddings stay enabled for semantic search."

    echo "Checking Ollama models..."
    tags="$(ollama_tags "$ollama_api_root")"
    has_llm=false
    has_emb=false
    ollama_model_present "$LLM_MODEL_NAME" "$tags" && has_llm=true
    ollama_model_present "$EMBEDDER_MODEL_NAME" "$tags" && has_emb=true

    if { [[ "$has_emb" != true ]] || { [[ "$disable_llm" != true ]] && [[ "$has_llm" != true ]]; }; } && ! is_truthy "${OLLAMA_AUTO_PULL:-false}"; then
        if [[ "$disable_llm" == true ]]; then
            die "Required Ollama embedder model is missing (EMB='$EMBEDDER_MODEL_NAME'). Set OLLAMA_AUTO_PULL=true or run: ollama pull $EMBEDDER_MODEL_NAME"
        fi
        die "Required Ollama models are missing (LLM='$LLM_MODEL_NAME', EMB='$EMBEDDER_MODEL_NAME'). Set OLLAMA_AUTO_PULL=true or run: ollama pull $LLM_MODEL_NAME ; ollama pull $EMBEDDER_MODEL_NAME"
    fi

    if [[ "$disable_llm" != true && "$has_llm" != true ]]; then
        echo "Pulling LLM model $LLM_MODEL_NAME ..."
        "$ollama_bin" pull "$LLM_MODEL_NAME"
    fi
    if [[ "$has_emb" != true ]]; then
        echo "Pulling embedder model $EMBEDDER_MODEL_NAME ..."
        "$ollama_bin" pull "$EMBEDDER_MODEL_NAME"
    fi
fi

echo "Starting Memgraph..."
if is_truthy "${MEMGRAPH_LAB_ENABLED:-true}"; then
    echo "Memgraph Lab is enabled -> attempting to start visual UI on http://127.0.0.1:3000 ..."
    compose_up up -d memgraph memgraph-lab || {
        echo "Failed to start memgraph-lab. Starting Memgraph without UI."
        compose_up up -d memgraph
    }
else
    compose_up up -d memgraph
fi
wait_tcp_port "127.0.0.1" "7687" "90" "Memgraph Bolt"

echo "Starting FastAPI service..."
stop_listener_on_port 8000
start_managed_process "FastAPI" "$pid_file" "$stdout_log" "$stderr_log" "examples/fastapi_demo/server.py"

ready=false
for _ in $(seq 1 "${FASTAPI_START_TIMEOUT_SEC:-120}"); do
    if ! kill -0 "$(cat "$pid_file")" >/dev/null 2>&1; then
        die "FastAPI process exited early. Check logs: $stdout_log and $stderr_log"
    fi
    if "$python_bin" - <<'PY' >/dev/null 2>&1
import json
import urllib.request
with urllib.request.urlopen("http://127.0.0.1:8000/status", timeout=2) as resp:
    payload = json.loads(resp.read().decode("utf-8"))
raise SystemExit(0 if "is_indexing" in payload else 1)
PY
    then
        ready=true
        break
    fi
    sleep 1
done
[[ "$ready" == true ]] || die "API did not become ready in time. Check logs: $stdout_log and $stderr_log"

if is_truthy "${SEED_DEMO_IT_KNOWLEDGE:-true}"; then
    echo "Seeding prepared IT knowledge base..."
    "$python_bin" "scripts/seed_demo_it_knowledge.py" --api-base-url "http://127.0.0.1:8000"
else
    echo "SEED_DEMO_IT_KNOWLEDGE=false -> skip prepared IT knowledge seeding."
fi

start_managed_process "Orchestrator" "$orchestrator_pid_file" "$orchestrator_stdout_log" "$orchestrator_stderr_log" -m apps.orchestrator.main
start_managed_process "SheetsSync" "$sheets_pid_file" "$sheets_stdout_log" "$sheets_stderr_log" -m apps.sheets_sync.main

bot_platform="$(printf '%s' "${BOT_PLATFORM:-telegram}" | tr '[:upper:]' '[:lower:]')"
case "$bot_platform" in
    telegram|vk|both|none) ;;
    *) die "Unsupported BOT_PLATFORM='$bot_platform'. Use telegram, vk, both, or none." ;;
esac

if [[ "$bot_platform" == "telegram" || "$bot_platform" == "both" ]]; then
    if [[ -z "${TELEGRAM_BOT_TOKEN:-}" ]]; then
        echo "TELEGRAM_BOT_TOKEN is empty -> skip Telegram bot startup."
        rm -f "$bot_pid_file"
    elif ! python_module_present "aiogram"; then
        echo "aiogram is not installed in venv -> skip Telegram bot startup."
        rm -f "$bot_pid_file"
    else
        start_managed_process "TelegramBot" "$bot_pid_file" "$bot_stdout_log" "$bot_stderr_log" -m apps.bot.main
    fi
else
    echo "BOT_PLATFORM=$bot_platform -> skip Telegram bot startup."
    rm -f "$bot_pid_file"
fi

if [[ "$bot_platform" == "vk" || "$bot_platform" == "both" ]]; then
    if [[ -z "${VK_BOT_TOKEN:-}" ]]; then
        echo "VK_BOT_TOKEN is empty -> skip VK bot startup."
        rm -f "$vk_bot_pid_file"
    else
        start_managed_process "VkBot" "$vk_bot_pid_file" "$vk_bot_stdout_log" "$vk_bot_stderr_log" -m apps.vk_bot.main
    fi
else
    echo "BOT_PLATFORM=$bot_platform -> skip VK bot startup."
    rm -f "$vk_bot_pid_file"
fi

echo ""
echo "Stack is ready."
echo "API docs: http://127.0.0.1:8000/docs"
echo "Status:   http://127.0.0.1:8000/status"
is_truthy "${MEMGRAPH_LAB_ENABLED:-true}" && echo "Memgraph UI: http://127.0.0.1:3000"
echo "Memgraph Bolt: bolt://127.0.0.1:7687"
echo "PID file: $pid_file"
echo "Logs:     $stdout_log / $stderr_log"
echo "Orchestrator PID: $orchestrator_pid_file"
echo "Orchestrator logs: $orchestrator_stdout_log / $orchestrator_stderr_log"
echo "Sheets PID: $sheets_pid_file"
echo "Sheets logs: $sheets_stdout_log / $sheets_stderr_log"
