#!/bin/bash

SPARK_HOME="$HOME/apps/spark"
JOB_DIR="$HOME/lab/iceberg-lab/jobs"
LOG_DIR="$HOME/platform/logs/spark"
PID_DIR="$HOME/platform/runtime/pids"

mkdir -p "$LOG_DIR" "$PID_DIR"

PID_FILE="$PID_DIR/orders_bronze_current.pid"

if [ -f "$PID_FILE" ] && ps -p "$(cat $PID_FILE)" > /dev/null 2>&1; then
  echo "[WARN] orders_bronze_current is already running. PID=$(cat $PID_FILE)"
  exit 0
fi

echo "[INFO] Starting orders_bronze_current streaming job..."

nohup "$SPARK_HOME/bin/spark-submit" \
  "$JOB_DIR/orders_raw_to_bronze_current.py" \
  > "$LOG_DIR/orders_bronze_current.log" 2>&1 &

echo $! > "$PID_FILE"

echo "[INFO] Started."
echo "[INFO] PID: $(cat $PID_FILE)"
echo "[INFO] Log: $LOG_DIR/orders_bronze_current.log"
