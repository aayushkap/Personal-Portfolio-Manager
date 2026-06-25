#!/bin/bash

set -e

APP_DIR="/home/ubuntu/app"
VENV_DIR="$APP_DIR/venv"
LOG_FILE="$APP_DIR/uvicorn.log"

cd "$APP_DIR"

echo "[INFO] Starting CI/CD restart..."

# --- Kill existing Python processes (graceful -> force) ---
echo "[INFO] Stopping existing Python processes..."
pkill -15 -u ubuntu -f "uvicorn" || true
sleep 2

if pgrep -u ubuntu -f "uvicorn" > /dev/null; then
    echo "[WARN] Force killing remaining uvicorn processes..."
    pkill -9 -u ubuntu -f "uvicorn" || true
fi

# Ensure all are dead
TRIES=0
while pgrep -u ubuntu -f "uvicorn" > /dev/null; do
    if [ $TRIES -ge 5 ]; then
        echo "[ERROR] Some processes refused to terminate:"
        pgrep -u ubuntu -af uvicorn
        exit 1
    fi
    sleep 1
    ((TRIES++))
done

echo "[INFO] All uvicorn processes stopped."

# --- Rotate existing log ---
if [ -f "$LOG_FILE" ]; then
    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    mv "$LOG_FILE" "${LOG_FILE}.${TIMESTAMP}"
    echo "[INFO] Archived previous log to ${LOG_FILE}.${TIMESTAMP}"
fi

# --- Activate virtual environment ---
echo "[INFO] Activating virtual environment..."

if [ ! -d "$VENV_DIR" ]; then
    echo "[ERROR] venv not found at $VENV_DIR"
    exit 1
fi

source "$VENV_DIR/bin/activate"

# Verify activation
if ! which python | grep -q "$VENV_DIR"; then
    echo "[ERROR] Failed to activate virtual environment"
    exit 1
fi

echo "[INFO] Virtual environment activated."

# --- Start app with nohup ---
echo "[INFO] Starting Uvicorn (detached)..."

nohup "$VENV_DIR/bin/python" -m uvicorn app.api:app \
    --host 0.0.0.0 \
    --port 8000 \
    > "$LOG_FILE" 2>&1 &

disown

sleep 1

echo "[INFO] Deployment complete."
echo "[INFO] Current log: $LOG_FILE"
echo "[INFO] Archived logs: ${LOG_FILE}.*"
echo "[INFO] Running processes:"
pgrep -af uvicorn