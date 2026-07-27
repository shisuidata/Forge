#!/usr/bin/env bash
set -uo pipefail

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

PROFILE="${FORGE_PROFILE:-prod}"
OUT="${FORGE_SMOKE_OUT:-.forge/production-smoke.json}"
ARTIFACT_DIR="${FORGE_SMOKE_ARTIFACT_DIR:-$(dirname "$OUT")}"
mkdir -p "$ARTIFACT_DIR"

DOCTOR_OUT="${FORGE_DOCTOR_OUT:-$ARTIFACT_DIR/doctor.json}"
DATABASE_OUT="${FORGE_DATABASE_SMOKE_OUT:-$ARTIFACT_DIR/database-smoke.json}"
PROVIDER_OUT="${FORGE_PROVIDER_SMOKE_OUT:-$ARTIFACT_DIR/provider-smoke.json}"
HTTP_OUT="${FORGE_HTTP_READINESS_OUT:-$ARTIFACT_DIR/http-readiness.json}"

echo "Forge production smoke"
echo "python: $PYTHON_BIN"
echo "profile: $PROFILE"

echo
echo "==> $PYTHON_BIN -m forge.cli doctor --profile $PROFILE --json"
if "$PYTHON_BIN" -m forge.cli doctor --profile "$PROFILE" --json > "$DOCTOR_OUT"; then
  DOCTOR_CODE=0
else
  DOCTOR_CODE=$?
fi
cat "$DOCTOR_OUT"

echo
echo "==> database SELECT 1"
if "$PYTHON_BIN" - "$DATABASE_OUT" <<'PY'; then
import json
import sys

from sqlalchemy import create_engine, text
from config import cfg

out = sys.argv[1]
payload = {"status": "unknown", "operation": "SELECT 1", "error": ""}
try:
    if not cfg.DATABASE_URL:
        payload["status"] = "skipped"
        payload["error"] = "DATABASE_URL is not configured"
    else:
        engine = create_engine(cfg.DATABASE_URL)
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        finally:
            engine.dispose()
        payload["status"] = "ok"
except Exception as exc:
    payload["status"] = "fail"
    payload["error"] = str(exc)

with open(out, "w", encoding="utf-8") as f:
    json.dump(payload, f, ensure_ascii=False, indent=2)
    f.write("\n")
print(json.dumps(payload, ensure_ascii=False, indent=2))
sys.exit(0 if payload["status"] in {"ok", "skipped"} else 1)
PY
  DATABASE_CODE=0
else
  DATABASE_CODE=$?
fi

if [[ "${FORGE_RUN_PROVIDER_SMOKE:-auto}" != "false" ]]; then
  echo
  echo "==> $PYTHON_BIN scripts/provider_smoke.py --json --out $PROVIDER_OUT"
  if "$PYTHON_BIN" scripts/provider_smoke.py --json --out "$PROVIDER_OUT"; then
    PROVIDER_CODE=0
  else
    PROVIDER_CODE=$?
    if [[ "$PROVIDER_CODE" == "2" ]]; then
      PROVIDER_CODE=0
    fi
  fi
else
  PROVIDER_CODE=0
  "$PYTHON_BIN" - "$PROVIDER_OUT" <<'PY'
import json
import sys
payload = {
    "provider": "",
    "model": "",
    "tool_call": "skipped",
    "schema": "skipped",
    "compile": "skipped",
    "dialect": "",
    "status": "skipped",
    "error": "FORGE_RUN_PROVIDER_SMOKE=false",
    "sql_preview": "",
}
with open(sys.argv[1], "w", encoding="utf-8") as f:
    json.dump(payload, f, ensure_ascii=False, indent=2)
    f.write("\n")
print(json.dumps(payload, ensure_ascii=False, indent=2))
PY
fi

if [[ -n "${FORGE_BASE_URL:-}" ]]; then
  echo
  echo "==> HTTP readiness"
  if "$PYTHON_BIN" - "$HTTP_OUT" "$PROFILE" <<'PY'; then
import json
import os
import sys
import urllib.parse
import urllib.request

out, profile = sys.argv[1], sys.argv[2]
base = os.environ["FORGE_BASE_URL"].rstrip("/")
url = base + "/health/readiness?" + urllib.parse.urlencode({"profile": profile})
payload = {"status": "unknown", "url": url, "error": ""}
try:
    with urllib.request.urlopen(url, timeout=10) as resp:
        payload = json.load(resp)
        payload["url"] = url
except Exception as exc:
    payload["status"] = "fail"
    payload["error"] = str(exc)

with open(out, "w", encoding="utf-8") as f:
    json.dump(payload, f, ensure_ascii=False, indent=2)
    f.write("\n")
print(json.dumps(payload, ensure_ascii=False, indent=2))
sys.exit(0 if payload.get("status") != "fail" else 1)
PY
    HTTP_CODE=0
  else
    HTTP_CODE=$?
  fi
else
  HTTP_CODE=0
  "$PYTHON_BIN" - "$HTTP_OUT" <<'PY'
import json
import sys
payload = {"status": "skipped", "url": "", "error": "FORGE_BASE_URL is not set"}
with open(sys.argv[1], "w", encoding="utf-8") as f:
    json.dump(payload, f, ensure_ascii=False, indent=2)
    f.write("\n")
print(json.dumps(payload, ensure_ascii=False, indent=2))
PY
fi

"$PYTHON_BIN" - "$OUT" "$PROFILE" "$DOCTOR_OUT" "$DATABASE_OUT" "$PROVIDER_OUT" "$HTTP_OUT" \
  "$DOCTOR_CODE" "$DATABASE_CODE" "$PROVIDER_CODE" "$HTTP_CODE" <<'PY'
import json
import sys
from datetime import datetime, timezone

out, profile, doctor_out, database_out, provider_out, http_out = sys.argv[1:7]
codes = {
    "doctor": int(sys.argv[7]),
    "database": int(sys.argv[8]),
    "provider": int(sys.argv[9]),
    "http_readiness": int(sys.argv[10]),
}
artifacts = {
    "doctor": doctor_out,
    "database": database_out,
    "provider": provider_out,
    "http_readiness": http_out,
}
steps = {}
for name, path in artifacts.items():
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as exc:
        data = {"status": "fail", "error": str(exc)}
        codes[name] = 1
    steps[name] = {"exit_code": codes[name], "artifact": path, "status": data.get("status", "unknown")}

status = "fail" if any(code != 0 for code in codes.values()) else "ok"
payload = {
    "status": status,
    "profile": profile,
    "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    "steps": steps,
}
with open(out, "w", encoding="utf-8") as f:
    json.dump(payload, f, ensure_ascii=False, indent=2)
    f.write("\n")
print(json.dumps(payload, ensure_ascii=False, indent=2))
sys.exit(0 if status == "ok" else 1)
PY
EXIT_CODE=$?

echo
echo "Production smoke summary: $OUT"
exit "$EXIT_CODE"
