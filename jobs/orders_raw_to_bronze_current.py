from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, current_timestamp, row_number,when
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.window import Window
import time

spark = SparkSession.builder.appName("orders-raw-to-bronze-current").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("op", StringType(), True),
    StructField("order_id", LongType(), True),
    StructField("user_id", StringType(), True),
    StructField("amount", LongType(), True),
    StructField("status", StringType(), True),
    StructField("event_time", StringType(), True),
])

raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "orders_raw")
    .option("startingOffsets", "earliest")
    .option("checkpointLocation", "s3a://dain-iceberg-lab-2026/checkpoints/orders_bronze_current")
    .load()
)

parsed_df = (
    raw_df
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp(col("event_time")))
    .withColumn("ingest_time", current_timestamp())
)

valid_df = parsed_df.filter(
    col("event_id").isNotNull() &
    col("order_id").isNotNull() &
    col("op").isNotNull()
)

invalid_df = parsed_df.filter(
    col("event_id").isNull() |
    col("order_id").isNull() |
    col("op").isNull()
)

def process_batch(batch_df, batch_id):
    start = time.time()

    if batch_df.isEmpty():
        print(f"[INFO] batch_id={batch_id} empty batch")
        return

    print(f"[INFO] Processing batch_id={batch_id}")

    bronze_start = time.time()
    batch_df.writeTo("glue_catalog.lakehouse_lab.orders_bronze").append()
    bronze_end = time.time()

    latest_df = (
        batch_df
        .withColumn(
            "rn",
            row_number().over(
                Window.partitionBy("order_id").orderBy(
                    col("event_time").desc(),
                    col("event_id").desc()
                )
            )
        )
        .filter(col("rn") == 1)
        .drop("rn", "ingest_time")
    )

    latest_df.createOrReplaceGlobalTempView("batch_latest_orders")

    merge_start = time.time()
    spark.sql("""
    MERGE INTO glue_catalog.lakehouse_lab.orders_current AS t
    USING (
        SELECT
            order_id,
            user_id,
            amount,
            status,
            event_time,
            event_id AS last_event_id,
            op,
            current_timestamp() AS updated_at
        FROM global_temp.batch_latest_orders
    ) AS s
    ON t.order_id = s.order_id
    WHEN MATCHED AND s.op = 'd' AND s.event_time >= t.event_time THEN DELETE
    WHEN MATCHED AND s.op <> 'd' AND s.event_time >= t.event_time THEN UPDATE SET
        t.user_id = s.user_id,
        t.amount = s.amount,
        t.status = s.status,
        t.event_time = s.event_time,
        t.last_event_id = s.last_event_id,
        t.updated_at = s.updated_at
    WHEN NOT MATCHED AND s.op <> 'd' THEN INSERT (
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
        current_timestamp()
    )
    """)
    merge_end = time.time()


    end = time.time()

    print(
        f"[METRIC] batch_id={batch_id}, "
        f"bronze_append_sec={bronze_end - bronze_start:.3f}, "
        f"merge_sec={merge_end - merge_start:.3f}, "
        f"total_sec={end - start:.3f}"
    )

    print(f"[INFO] Completed batch_id={batch_id}")

dlq_query = (
    invalid_df
    .selectExpr("to_json(struct(*)) AS value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "orders_dlq")
    .option(
        "checkpointLocation",
        "s3a://dain-iceberg-lab-2026/checkpoints/orders_dlq"
    )
    .start()
)

query = (
    valid_df.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", "s3a://dain-iceberg-lab-2026/checkpoints/orders_bronze_current")
    .start()
)

query.awaitTermination()
