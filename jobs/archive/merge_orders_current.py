from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("merge-orders-current")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

spark.sql("""
MERGE INTO glue_catalog.demo.orders_current AS t
USING (
    SELECT
        order_id,
        user_id,
        amount,
        status,
        event_time,
        event_id AS last_event_id,
        current_timestamp() AS updated_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY order_id
                ORDER BY event_time DESC, event_id DESC
            ) AS rn
        FROM glue_catalog.demo.orders_bronze
        WHERE op IN ('c', 'u')
    )
    WHERE rn = 1
) AS s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET
    t.user_id = s.user_id,
    t.amount = s.amount,
    t.status = s.status,
    t.event_time = s.event_time,
    t.last_event_id = s.last_event_id,
    t.updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT (
    order_id,
    user_id,
    amount,
    status,
    event_time,
    last_event_id,
    updated_at
) VALUES (
    s.order_id,
    s.user_id,
    s.amount,
    s.status,
    s.event_time,
    s.last_event_id,
    s.updated_at
)
""")

print("[INFO] Merge completed.")

spark.sql("""
SELECT *
FROM glue_catalog.demo.orders_current
ORDER BY order_id
""").show(truncate=False)

spark.stop()
