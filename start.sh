#!/usr/bin/env bash

# Local development is the default. It runs a two-worker Gunicorn API and one
# scheduled worker with deterministic process-group cleanup. Production uses
# systemd; CI/CD invokes that path directly.
set -euo pipefail

ACTION="${1:-start}"
MODE="${PBE_MODE:-local}"

if [[ "$MODE" != "systemd" ]]; then
    exec venv/bin/python scripts/local_supervisor.py "$ACTION"
fi

SYSTEMCTL=(sudo systemctl)
if [[ "${PBE_SYSTEMCTL_NO_SUDO:-0}" == "1" ]]; then
    SYSTEMCTL=(systemctl)
fi

case "$ACTION" in
    start|restart)
        "${SYSTEMCTL[@]}" stop pbe-api.service pbe-worker.service || true
        "${SYSTEMCTL[@]}" start pbe-worker.service pbe-api.service
        ;;
    stop)
        "${SYSTEMCTL[@]}" stop pbe-api.service pbe-worker.service
        ;;
    status)
        "${SYSTEMCTL[@]}" status pbe-api.service pbe-worker.service
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status}" >&2
        exit 2
        ;;
esac
