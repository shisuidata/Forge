#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="${PYTHON_BIN:-}"
if [[ -z "$PYTHON_BIN" ]]; then
  if [[ -x ".venv/bin/python" ]]; then
    PYTHON_BIN=".venv/bin/python"
  elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="python3"
  else
    echo "FAIL python: no .venv/bin/python or python3 found" >&2
    exit 1
  fi
fi

run_required() {
  echo
  echo "==> $*"
  "$@"
}

run_optional() {
  echo
  echo "==> $*"
  if ! "$@"; then
    echo "WARN optional check failed: $*" >&2
    return 0
  fi
}

echo "Forge production smoke"
echo "python: $PYTHON_BIN"

run_required "$PYTHON_BIN" -m forge.cli doctor

if [[ -n "${DATABASE_URL:-}" ]]; then
  run_required "$PYTHON_BIN" - <<'PY'
from sqlalchemy import create_engine, text
from config import cfg

engine = create_engine(cfg.DATABASE_URL)
try:
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
finally:
    engine.dispose()

print("OK database: SELECT 1 succeeded")
PY
else
  echo
  echo "SKIP database: DATABASE_URL is not set"
fi

if [[ "${FORGE_RUN_PROVIDER_SMOKE:-auto}" != "false" ]]; then
  if [[ -n "${LLM_API_KEY:-}" ]]; then
    run_required "$PYTHON_BIN" scripts/provider_smoke.py
  else
    echo
    echo "SKIP provider: LLM_API_KEY is not set"
  fi
fi

if [[ -n "${FORGE_BASE_URL:-}" ]]; then
  run_required "$PYTHON_BIN" - <<'PY'
import json
import os
import sys
import urllib.request

url = os.environ["FORGE_BASE_URL"].rstrip("/") + "/health/readiness"
with urllib.request.urlopen(url, timeout=10) as resp:
    data = json.load(resp)

status = data.get("status")
print(f"readiness: {status}")
if status == "fail":
    print(json.dumps(data, ensure_ascii=False, indent=2))
    sys.exit(1)
PY
else
  echo
  echo "SKIP http readiness: FORGE_BASE_URL is not set"
fi

echo
echo "Production smoke completed."
