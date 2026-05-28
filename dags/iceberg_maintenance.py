from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="iceberg_maintenance",
    start_date=datetime(2026, 5, 1),
    schedule="@daily",
    catchup=False,
    tags=["iceberg", "maintenance", "lakehouse"],
) as dag:

    compact_orders_bronze = BashOperator(
        task_id="compact_orders_bronze",
        bash_command="""
        spark-sql -e "
        CALL glue_catalog.system.rewrite_data_files(
          table => 'lakehouse_lab.orders_bronze'
        );
        "
        """,
    )

    compact_orders_current = BashOperator(
        task_id="compact_orders_current",
        bash_command="""
        spark-sql -e "
        CALL glue_catalog.system.rewrite_data_files(
          table => 'lakehouse_lab.orders_current'
        );
        "
        """,
    )

    validate_orders_bronze_files = BashOperator(
        task_id="validate_orders_bronze_files",
        bash_command="""
        spark-sql -e "
        SELECT
          count(*) AS file_count,
          sum(record_count) AS total_rows,
          sum(file_size_in_bytes) AS total_bytes
        FROM glue_catalog.lakehouse_lab.orders_bronze.files;
        "
        """,
    )

    validate_orders_current_files = BashOperator(
        task_id="validate_orders_current_files",
        bash_command="""
        spark-sql -e "
        SELECT
          count(*) AS file_count,
          sum(record_count) AS total_rows,
          sum(file_size_in_bytes) AS total_bytes
        FROM glue_catalog.lakehouse_lab.orders_current.files;
        "
        """,
    )

    (
        compact_orders_bronze
        >> compact_orders_current
        >> validate_orders_bronze_files
        >> validate_orders_current_files
    )
