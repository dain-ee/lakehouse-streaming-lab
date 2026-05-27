#!/bin/bash

PID_FILE="$HOME/platform/runtime/pids/kafka.pid"

if [ ! -f "$PID_FILE" ]; then
  echo "[WARN] Kafka PID file not found."
  exit 0
fi

PID="$(cat $PID_FILE)"

if ps -p "$PID" > /dev/null 2>&1; then
  echo "[INFO] Stopping Kafka. PID=$PID"
  kill "$PID"
else
  echo "[WARN] Kafka process not running. PID=$PID"
fi

rm -f "$PID_FILE"
