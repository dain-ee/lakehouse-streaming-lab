#!/bin/bash

PID_FILE="$HOME/platform/runtime/pids/orders_metrics_5m.pid"

if [ ! -f "$PID_FILE" ]; then
  echo "[WARN] PID file not found."
  exit 0
fi

PID="$(cat $PID_FILE)"

if ps -p "$PID" > /dev/null 2>&1; then
  echo "[INFO] Stopping orders_metrics_5m. PID=$PID"
  kill "$PID"
else
  echo "[WARN] Process not running. PID=$PID"
fi

rm -f "$PID_FILE"
