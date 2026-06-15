#!/usr/bin/env bash
set -Eeuo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
compose_file="$repo_root/examples/fastapi_demo/docker-compose.yml"
env_file="$repo_root/.env"

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

ollama_tags() {
    local api_root="$1"
    curl -fsS "$api_root/api/tags" 2>/dev/null \
        | python -c "import json,sys; data=json.load(sys.stdin); print('\n'.join(str(m.get('name','')) for m in data.get('models', [])))" 2>/dev/null \
        || true
}

ollama_model_present() {
    local model="$1"
    local tags="$2"
    grep -Fxq "$model" <<<"$tags" && return 0
    [[ "$model" != *:* ]] && grep -Fxq "$model:latest" <<<"$tags" && return 0
    return 1
}

ensure_ollama_api_ready() {
    local api_root="$1"
    local tags
    tags="$(ollama_tags "$api_root")"
    [[ -n "$tags" ]] && return

    echo "Ollama API is not ready, starting ollama serve..."
    nohup ollama serve >/tmp/ragu-ollama.stdout.log 2>/tmp/ragu-ollama.stderr.log &
    for _ in $(seq 1 45); do
        sleep 1
        tags="$(ollama_tags "$api_root")"
        [[ -n "$tags" ]] && return
    done
    die "Ollama API is not reachable at $api_root/api/tags."
}

wait_api_ready() {
    local timeout_sec="$1"
    for _ in $(seq 1 "$timeout_sec"); do
        if curl -fsS "http://127.0.0.1:8000/status" >/dev/null 2>&1; then
            return
        fi
        sleep 1
    done
    die "API did not become ready in time."
}

command -v docker >/dev/null 2>&1 || die "Docker is not installed or not in PATH."
command -v ollama >/dev/null 2>&1 || die "Ollama is not installed or not in PATH."
command -v curl >/dev/null 2>&1 || die "curl is required for readiness checks."
command -v python >/dev/null 2>&1 || die "python is required for Ollama model checks."

load_dotenv "$env_file"

env_default LLM_PROVIDER "ollama"
env_default API_KEY "local"
env_default OLLAMA_HOST_BASE_URL "http://127.0.0.1:11434"
env_default OLLAMA_BASE_URL "http://host.docker.internal:11434/v1"
env_default LLM_MODEL_NAME "qwen2.5:3b"
env_default EMBEDDER_MODEL_NAME "nomic-embed-text"
env_default EMBEDDING_DIM "20"
env_default OLLAMA_AUTO_PULL "false"
env_default MEMGRAPH_LAB_ENABLED "true"
env_default VK_BOT_ENABLED "false"
env_default SHEETS_SYNC_ENABLED "false"
env_default FASTAPI_START_TIMEOUT_SEC "120"

[[ "${LLM_PROVIDER,,}" == "ollama" ]] || die "Unsupported LLM_PROVIDER. Use ollama."

ensure_docker_ready
ollama_api_root="${OLLAMA_HOST_BASE_URL%/}"
ensure_ollama_api_ready "$ollama_api_root"

disable_llm=false
is_truthy "${DISABLE_LLM_ANSWERS:-false}" && disable_llm=true
tags="$(ollama_tags "$ollama_api_root")"
has_llm=false
has_emb=false
ollama_model_present "$LLM_MODEL_NAME" "$tags" && has_llm=true
ollama_model_present "$EMBEDDER_MODEL_NAME" "$tags" && has_emb=true

if { [[ "$has_emb" != true ]] || { [[ "$disable_llm" != true ]] && [[ "$has_llm" != true ]]; }; } && ! is_truthy "${OLLAMA_AUTO_PULL:-false}"; then
    die "Required Ollama models are missing. Set OLLAMA_AUTO_PULL=true or run: ollama pull $LLM_MODEL_NAME ; ollama pull $EMBEDDER_MODEL_NAME"
fi
if [[ "$disable_llm" != true && "$has_llm" != true ]]; then
    ollama pull "$LLM_MODEL_NAME"
fi
if [[ "$has_emb" != true ]]; then
    ollama pull "$EMBEDDER_MODEL_NAME"
fi

profile_args=()
is_truthy "${MEMGRAPH_LAB_ENABLED:-true}" && profile_args+=(--profile lab)
is_truthy "${SHEETS_SYNC_ENABLED:-false}" && profile_args+=(--profile sheets)
is_truthy "${VK_BOT_ENABLED:-false}" && profile_args+=(--profile vk)

echo "Starting RAGU compose stack..."
docker compose -f "$compose_file" "${profile_args[@]}" up -d --build
wait_api_ready "${FASTAPI_START_TIMEOUT_SEC:-120}"

echo ""
echo "Stack is ready."
echo "API docs: http://127.0.0.1:8000/docs"
echo "Status:   http://127.0.0.1:8000/status"
is_truthy "${MEMGRAPH_LAB_ENABLED:-true}" && echo "Memgraph UI: http://127.0.0.1:3000"
echo "Memgraph Bolt: bolt://127.0.0.1:7687"
echo "Prepared IT seed is explicit: ./venv/bin/python scripts/seed_demo_it_knowledge.py --api-base-url http://127.0.0.1:8000"
