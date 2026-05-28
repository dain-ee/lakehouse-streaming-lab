from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="lakehouse_pipeline",
    start_date=datetime(2026, 5, 1),
    schedule="@daily",
    catchup=False,
    tags=["lakehouse", "kafka", "spark", "iceberg"],
) as dag:

    check_kafka_topics = BashOperator(
        task_id="check_kafka_topics",
        bash_command="""
        /home/ec2-user/apps/kafka/bin/kafka-topics.sh \
          --bootstrap-server localhost:9092 \
          --list | grep -E "orders_raw|orders_dlq"
        """,
    )

    start_bronze_current_streaming = BashOperator(
        task_id="start_bronze_current_streaming",
        bash_command="""
        cd /home/ec2-user/lab/iceberg-lab/scripts
        ./run-orders-bronze-current.sh
        """,
    )

    start_metrics_streaming = BashOperator(
        task_id="start_metrics_streaming",
        bash_command="""
        cd /home/ec2-user/lab/iceberg-lab/scripts
        ./run-orders-metrics-5m.sh
        """,
    )

    validate_current_not_empty = BashOperator(
        task_id="validate_current_not_empty",
        bash_command="""
        ROW_COUNT=$(spark-sql -e "
        SELECT count(*)
        FROM glue_catalog.lakehouse_lab.orders_current;
        " | grep -E "^[0-9]+$")

        echo "orders_current row count: ${ROW_COUNT}"

        if [ "$ROW_COUNT" -le 0 ]; then
          echo "[ERROR] orders_current is empty"
          exit 1
        fi

        echo "[INFO] orders_current validation passed"
        """,
    )

    (
        check_kafka_topics
        >> start_bronze_current_streaming
        >> start_metrics_streaming
        >> validate_current_not_empty
    )
