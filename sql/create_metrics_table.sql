CREATE TABLE IF NOT EXISTS glue_catalog.lakehouse_lab.orders_status_metrics_5m (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    status STRING,
    order_count BIGINT,
    total_amount BIGINT
)
USING iceberg
PARTITIONED BY (days(window_start));
