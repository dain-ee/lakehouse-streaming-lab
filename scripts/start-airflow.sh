#!/bin/bash

export AIRFLOW_HOME=/home/ec2-user/platform/airflow

LOG_DIR=/home/ec2-user/platform/logs/airflow
PID_DIR=/home/ec2-user/platform/runtime/pids

mkdir -p "$LOG_DIR" "$PID_DIR"

echo "[INFO] Starting Airflow webserver..."
nohup airflow webserver --port 8081 > "$LOG_DIR/webserver.log" 2>&1 &
echo $! > "$PID_DIR/airflow-webserver.pid"

echo "[INFO] Starting Airflow scheduler..."
nohup airflow scheduler > "$LOG_DIR/scheduler.log" 2>&1 &
echo $! > "$PID_DIR/airflow-scheduler.pid"

echo "[INFO] Airflow started."
echo "[INFO] Webserver PID: $(cat $PID_DIR/airflow-webserver.pid)"
echo "[INFO] Scheduler PID: $(cat $PID_DIR/airflow-scheduler.pid)"
echo "[INFO] Web UI: http://<EC2_PUBLIC_IP>:8081"
