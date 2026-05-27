#!/bin/bash

SPARK_HOME="$HOME/apps/spark-3.5.8-bin-hadoop3"
JOB_DIR="$HOME/iceberg_lab/jobs"
LOG_DIR="$HOME/iceberg_lab/logs"

mkdir -p "$LOG_DIR"

echo "[INFO] Running merge_orders_current job..."

$SPARK_HOME/bin/spark-submit \
  $JOB_DIR/merge_orders_current.py \
  > $LOG_DIR/merge_orders_current.log 2>&1

echo "[INFO] Merge job finished."
echo "[INFO] Log File: $LOG_DIR/merge_orders_current.log"
