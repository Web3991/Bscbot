#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="/home/ubuntu"
LOCK_FILE="$PROJECT_DIR/.bsc-bot.lock"
cd "$PROJECT_DIR"

# Single-instance guard: hold an exclusive lock for the entire process lifetime.
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "❌ Another bsc-bot instance already holds $LOCK_FILE" >&2
  pgrep -af 'bsc_main.py' || true
  exit 1
fi

# Load environment variables from .env if present
if [[ -f .env ]]; then
  eval "$(${PROJECT_DIR}/.venv/bin/python - <<'PY'
from pathlib import Path
import shlex
p = Path('.env')
for raw in p.read_text(encoding='utf-8').splitlines():
    line = raw.strip()
    if not line or line.startswith('#') or '=' not in line:
        continue
    key, value = line.split('=', 1)
    key = key.strip()
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ('\"', "'"):
        value = value[1:-1]
    print(f"export {key}={shlex.quote(value)}")
PY
)"
fi

# Activate virtual environment if available
if [[ -f .venv/bin/activate ]]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
elif [[ -f venv/bin/activate ]]; then
  # shellcheck disable=SC1091
  source venv/bin/activate
fi

mkdir -p logs

# Second guard: refuse to start if a rogue/manual bsc_main.py is already running.
mapfile -t existing_pids < <(pgrep -u ubuntu -f 'bsc_main.py' || true)
if (( ${#existing_pids[@]} > 0 )); then
  echo "❌ Refusing to start: existing bsc_main.py process(es) detected: ${existing_pids[*]}" >&2
  ps -fp "${existing_pids[@]}" || true
  exit 1
fi

required=(TELEGRAM_BOT_TOKEN TELEGRAM_CHAT_ID BSC_PRIVATE_KEY)
for name in "${required[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    echo "❌ Missing required env: $name" >&2
    exit 1
  fi
done

EFFECTIVE_DRY_RUN="$(${PROJECT_DIR}/.venv/bin/python - <<'PY'
import json, os
from pathlib import Path
cfg = Path('bsc_sys_config.json')
if cfg.exists():
    try:
        data = json.loads(cfg.read_text(encoding='utf-8'))
        val = data.get('DRY_RUN')
        if isinstance(val, bool):
            print(str(val).lower())
            raise SystemExit
    except Exception:
        pass
print(os.environ.get('DRY_RUN', 'false').lower())
PY
)"

echo "🚀 Starting BSC bot from $PROJECT_DIR"
echo "🧪 EFFECTIVE_DRY_RUN=${EFFECTIVE_DRY_RUN}"

echo "📝 Logs: $PROJECT_DIR/logs/bsc_bot.log"
exec python3 bsc_main.py
