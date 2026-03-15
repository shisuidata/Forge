#!/usr/bin/env bash
# Forge 飞书 Bot 进程管理脚本
# 用法：
#   ./scripts/bot.sh start    启动后台服务
#   ./scripts/bot.sh stop     停止服务
#   ./scripts/bot.sh restart  重启服务
#   ./scripts/bot.sh status   查看运行状态
#   ./scripts/bot.sh log      实时查看日志

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$SCRIPT_DIR/.."
PID_FILE="$ROOT/.forge/bot.pid"
LOG_FILE="$ROOT/.forge/bot.log"

mkdir -p "$ROOT/.forge"

_is_running() {
    [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null
}

case "${1:-status}" in
start)
    if _is_running; then
        echo "Bot is already running (pid=$(cat "$PID_FILE"))"
        exit 0
    fi
    echo "Starting Forge Bot..."
    cd "$ROOT"
    nohup env PYTHONPATH="$ROOT" python3 web/feishu.py >> "$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    sleep 1
    if _is_running; then
        echo "Bot started (pid=$(cat "$PID_FILE")), logging to $LOG_FILE"
    else
        echo "Bot failed to start, check $LOG_FILE"
        exit 1
    fi
    ;;
stop)
    if ! _is_running; then
        echo "Bot is not running"
        rm -f "$PID_FILE"
        exit 0
    fi
    PID=$(cat "$PID_FILE")
    kill "$PID"
    rm -f "$PID_FILE"
    echo "Bot stopped (pid=$PID)"
    ;;
restart)
    "$0" stop || true
    sleep 1
    "$0" start
    ;;
status)
    if _is_running; then
        echo "Bot is running (pid=$(cat "$PID_FILE"))"
    else
        echo "Bot is not running"
    fi
    ;;
log)
    tail -f "$LOG_FILE"
    ;;
*)
    echo "Usage: $0 {start|stop|restart|status|log}"
    exit 1
    ;;
esac
