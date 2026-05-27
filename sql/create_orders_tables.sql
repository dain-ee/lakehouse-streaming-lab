CREATE DATABASE IF NOT EXISTS glue_catalog.lakehouse_lab;

CREATE TABLE IF NOT EXISTS glue_catalog.lakehouse_lab.orders_bronze (
    event_id STRING,
    op STRING,
    order_id BIGINT,
    user_id STRING,
    amount BIGINT,
    status STRING,
    event_time TIMESTAMP,
    ingest_time TIMESTAMP
)
USING iceberg;

CREATE TABLE IF NOT EXISTS glue_catalog.lakehouse_lab.orders_current (
    order_id BIGINT,
    user_id STRING,
    amount BIGINT,
    status STRING,
    event_time TIMESTAMP,
    last_event_id STRING,
    updated_at TIMESTAMP
)
USING iceberg;
