#!/bin/bash

SPARK_HOME="$HOME/apps/spark-3.5.8-bin-hadoop3"
JOB_DIR="$HOME/iceberg_lab/jobs"
LOG_DIR="$HOME/iceberg_lab/logs"

mkdir -p "$LOG_DIR"

echo "[INFO] Starting Bronze Streaming Job..."

nohup $SPARK_HOME/bin/spark-submit \
  $JOB_DIR/orders_raw_to_bronze.py \
  > $LOG_DIR/orders_bronze.log 2>&1 &

echo $! > $LOG_DIR/orders_bronze.pid

echo "[INFO] Bronze Job Started"
echo "[INFO] PID: $(cat $LOG_DIR/orders_bronze.pid)"
echo "[INFO] Log File: $LOG_DIR/orders_bronze.log"
