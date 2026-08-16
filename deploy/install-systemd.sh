#!/usr/bin/env bash

set -euo pipefail

APP_DIR="/home/akap/app"
UNIT_DIR="$APP_DIR/deploy/systemd"

if [[ $EUID -ne 0 ]]; then
    echo "Run with sudo: sudo $0"
    exit 1
fi

install -m 0644 "$UNIT_DIR/pbe-api.service" /etc/systemd/system/pbe-api.service
install -m 0644 "$UNIT_DIR/pbe-worker.service" /etc/systemd/system/pbe-worker.service
systemctl daemon-reload
systemctl enable pbe-api.service pbe-worker.service

echo "Installed pbe-api.service and pbe-worker.service."
