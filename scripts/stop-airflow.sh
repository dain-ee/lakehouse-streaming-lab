#!/bin/bash

PID_DIR=/home/ec2-user/platform/runtime/pids

for name in airflow-webserver airflow-scheduler; do
  PID_FILE="$PID_DIR/$name.pid"

  if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")

    if ps -p "$PID" > /dev/null 2>&1; then
      echo "[INFO] Stopping $name. PID=$PID"
      kill "$PID"
    else
      echo "[WARN] $name is not running. PID=$PID"
    fi

    rm -f "$PID_FILE"
  else
    echo "[WARN] PID file not found: $PID_FILE"
  fi
done
