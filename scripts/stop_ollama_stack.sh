#!/usr/bin/env bash
set -Eeuo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
compose_file="$repo_root/examples/fastapi_demo/docker-compose.yml"
run_dir="$repo_root/.run"
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

is_managed_project_process() {
    local pid="$1"
    [[ "$pid" =~ ^[0-9]+$ ]] || return 1
    kill -0 "$pid" >/dev/null 2>&1 || return 1
    local cmdline
    cmdline="$(ps -p "$pid" -o command= 2>/dev/null || true)"
    [[ -n "$cmdline" ]] || return 1
    [[ "$cmdline" == *"$repo_root"* || "$cmdline" == *"$repo_root/venv"* ]]
}

child_pids() {
    local parent="$1"
    if command -v pgrep >/dev/null 2>&1; then
        pgrep -P "$parent" 2>/dev/null || true
    fi
}

stop_pid_tree() {
    local pid="$1"
    local name="$2"
    local child
    for child in $(child_pids "$pid"); do
        stop_pid_tree "$child" "$name"
    done
    if kill -0 "$pid" >/dev/null 2>&1; then
        echo "Stopping $name process PID=$pid ..."
        kill "$pid" >/dev/null 2>&1 || true
        for _ in $(seq 1 8); do
            kill -0 "$pid" >/dev/null 2>&1 || return
            sleep 1
        done
        kill -0 "$pid" >/dev/null 2>&1 && kill -9 "$pid" >/dev/null 2>&1 || true
    fi
}

stop_process_tree_by_pid_file() {
    local pid_path="$1"
    local name="$2"
    if [[ ! -f "$pid_path" ]]; then
        echo "$name PID file not found."
        return
    fi

    local pid
    pid="$(head -n 1 "$pid_path" 2>/dev/null || true)"
    if [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" >/dev/null 2>&1; then
        if is_managed_project_process "$pid"; then
            stop_pid_tree "$pid" "$name"
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
    local pid
    for pid in $(lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true); do
        if is_managed_project_process "$pid"; then
            echo "Stopping managed listener on port $port (PID=$pid) ..."
            kill "$pid" >/dev/null 2>&1 || true
        else
            echo "Port $port is used by PID=$pid, but it is not a managed project process. Leaving it running."
        fi
    done
}

docker_ready() {
    command -v docker >/dev/null 2>&1 && docker version >/dev/null 2>&1
}

load_dotenv_if_exists "$env_file"

stop_process_tree_by_pid_file "$run_dir/bot.pid" "TelegramBot"
stop_process_tree_by_pid_file "$run_dir/vk_bot.pid" "VkBot"
stop_process_tree_by_pid_file "$run_dir/sheets_sync.pid" "SheetsSync"
stop_process_tree_by_pid_file "$run_dir/orchestrator.pid" "Orchestrator"
stop_process_tree_by_pid_file "$run_dir/fastapi_demo.pid" "FastAPI"

stop_listener_on_port 8000

echo "Stopping Memgraph containers..."
if docker_ready; then
    docker compose -f "$compose_file" stop memgraph memgraph-lab || true
else
    echo "Docker CLI/API is not reachable. Skip Memgraph stop."
fi

if is_truthy "${STOP_OLLAMA_ON_STOP:-false}"; then
    if [[ -f "$run_dir/ollama.pid" ]]; then
        stop_process_tree_by_pid_file "$run_dir/ollama.pid" "Ollama"
    elif command -v pkill >/dev/null 2>&1; then
        pkill -f "ollama serve" >/dev/null 2>&1 || true
    fi
    echo "Ollama processes stopped because STOP_OLLAMA_ON_STOP=true."
fi

echo "Done."
