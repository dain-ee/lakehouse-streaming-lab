#!/bin/bash

SPARK_HOME="$HOME/apps/spark"
PID_FILE="$HOME/platform/runtime/pids/history-server.pid"

"$SPARK_HOME/sbin/stop-history-server.sh" >/dev/null 2>&1

if [ -f "$PID_FILE" ]; then
  rm -f "$PID_FILE"
fi

echo "[INFO] Spark History Server stopped."
