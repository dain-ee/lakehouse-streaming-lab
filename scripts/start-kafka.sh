#!/bin/bash

KAFKA_HOME="$HOME/apps/kafka"
LOG_DIR="$HOME/platform/logs/kafka"
PID_DIR="$HOME/platform/runtime/pids"

mkdir -p "$LOG_DIR" "$PID_DIR"

if [ -f "$PID_DIR/kafka.pid" ] && ps -p "$(cat $PID_DIR/kafka.pid)" > /dev/null 2>&1; then
  echo "[WARN] Kafka is already running. PID=$(cat $PID_DIR/kafka.pid)"
  exit 0
fi

echo "[INFO] Starting Kafka..."

nohup "$KAFKA_HOME/bin/kafka-server-start.sh" \
  "$KAFKA_HOME/config/kraft/server.properties" \
  > "$LOG_DIR/kafka.log" 2>&1 &

echo $! > "$PID_DIR/kafka.pid"

echo "[INFO] Kafka started."
echo "[INFO] PID: $(cat $PID_DIR/kafka.pid)"
echo "[INFO] Log: $LOG_DIR/kafka.log"
