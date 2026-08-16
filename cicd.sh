#!/usr/bin/env bash

# Deploy through systemd. Do not use nohup or broad process-name kills: systemd
# owns the API and worker cgroups and terminates their complete process trees.
set -euo pipefail

APP_DIR="/home/akap/app"
VENV_DIR="$APP_DIR/venv"
APP_USER="akap"
SYSTEMCTL=(sudo systemctl)
if [[ "${PBE_SYSTEMCTL_NO_SUDO:-0}" == "1" ]]; then
    SYSTEMCTL=(systemctl)
fi

cd "$APP_DIR"

if [[ ! -x "$VENV_DIR/bin/python" ]]; then
    echo "[ERROR] Python virtual environment not found at $VENV_DIR" >&2
    exit 1
fi

if ! "${SYSTEMCTL[@]}" cat pbe-api.service pbe-worker.service >/dev/null; then
    echo "[ERROR] systemd units are not installed. Run sudo deploy/install-systemd.sh first." >&2
    exit 1
fi

echo "[INFO] Stopping managed API and worker services..."
"${SYSTEMCTL[@]}" stop pbe-api.service pbe-worker.service || true

# One-time migration protection: clean up only legacy processes belonging to
# this application. Future starts are fully contained by systemd cgroups.
legacy_patterns=(
    "gunicorn.*app\.api:app"
    "uvicorn.*app\.api:app"
    "python.*app\.worker"
)

for pattern in "${legacy_patterns[@]}"; do
    pkill -TERM -u "$APP_USER" -f "$pattern" 2>/dev/null || true
done

for _ in {1..10}; do
    any_legacy=0
    for pattern in "${legacy_patterns[@]}"; do
        if pgrep -u "$APP_USER" -f "$pattern" >/dev/null; then
            any_legacy=1
            break
        fi
    done
    [[ $any_legacy -eq 0 ]] && break
    sleep 1
done

for pattern in "${legacy_patterns[@]}"; do
    pkill -KILL -u "$APP_USER" -f "$pattern" 2>/dev/null || true
done

echo "[INFO] Preparing SQLite schema and WAL mode..."
"$VENV_DIR/bin/python" -m app.data.bootstrap

echo "[INFO] Starting exactly one worker and the two-worker API..."
"${SYSTEMCTL[@]}" start pbe-worker.service
"${SYSTEMCTL[@]}" start pbe-api.service

sleep 2
"${SYSTEMCTL[@]}" is-active --quiet pbe-worker.service
"${SYSTEMCTL[@]}" is-active --quiet pbe-api.service

echo "[INFO] Deployment complete."
"${SYSTEMCTL[@]}" --no-pager --full status pbe-api.service pbe-worker.service
