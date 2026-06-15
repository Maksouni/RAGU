#!/usr/bin/env bash
set -Eeuo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
compose_file="$repo_root/examples/fastapi_demo/docker-compose.yml"
env_file="$repo_root/.env"

is_truthy() {
    case "${1:-}" in
        1|true|TRUE|True|yes|YES|on|ON) return 0 ;;
        *) return 1 ;;
    esac
}

load_dotenv_if_exists() {
    local file="$1"
    [[ -f "$file" ]] || return
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

load_dotenv_if_exists "$env_file"

if command -v docker >/dev/null 2>&1 && docker version >/dev/null 2>&1; then
    echo "Stopping RAGU compose stack..."
    docker compose -f "$compose_file" --profile lab --profile sheets --profile vk stop || true
else
    echo "Docker CLI/API is not reachable. Skip compose stop."
fi

if is_truthy "${STOP_OLLAMA_ON_STOP:-false}"; then
    pkill -f "ollama serve" >/dev/null 2>&1 || true
    echo "Ollama processes stopped because STOP_OLLAMA_ON_STOP=true."
fi

echo "Done."
