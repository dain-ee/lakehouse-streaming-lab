from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="spark_healthcheck",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
) as dag:

    check_orders_bronze = BashOperator(
        task_id="check_orders_bronze",
        bash_command="""
        echo "[DEBUG] airflow task cwd: $(pwd)"
        ls -ld .
PID_FILE=/home/ec2-user/platform/runtime/pids/orders_bronze_current.pid

if [ ! -f "$PID_FILE" ]; then
    echo "PID file not found"
    /home/ec2-user/lab/iceberg-lab/scripts/run-orders-bronze-current.sh
    exit 0
fi

PID=$(cat $PID_FILE)

if ps -p $PID > /dev/null
then
    echo "Spark job running"
else
    echo "Spark job dead. Restarting..."
    /home/ec2-user/lab/iceberg-lab/scripts/run-orders-bronze-current.sh
fi
"""
    )
