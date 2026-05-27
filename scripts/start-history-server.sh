#!/bin/bash

SPARK_HOME="$HOME/apps/spark"
LOG_DIR="$HOME/platform/logs/spark"
PID_DIR="$HOME/platform/runtime/pids"

mkdir -p "$LOG_DIR" "$PID_DIR"

if pgrep -f "org.apache.spark.deploy.history.HistoryServer" > /dev/null; then
  echo "[WARN] Spark History Server is already running."
  exit 0
fi

echo "[INFO] Starting Spark History Server..."

nohup "$SPARK_HOME/sbin/start-history-server.sh" \
  > "$LOG_DIR/history-server-start.log" 2>&1 &

sleep 2

pgrep -f "org.apache.spark.deploy.history.HistoryServer" | head -1 > "$PID_DIR/history-server.pid"

echo "[INFO] Spark History Server started."
echo "[INFO] PID: $(cat $PID_DIR/history-server.pid)"
echo "[INFO] URL: http://<EC2_PUBLIC_IP>:18080"
