from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = (
    SparkSession.builder
    .appName("orders-metrics-5m")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("event_id", StringType()),
    StructField("op", StringType()),
    StructField("order_id", LongType()),
    StructField("user_id", StringType()),
    StructField("amount", LongType()),
    StructField("status", StringType()),
    StructField("event_time", StringType())
])

raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "orders_raw")
    .option("startingOffsets", "latest")
    .load()
)

orders_df = (
    raw_df
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp("event_time"))
)

metrics_df = (
    orders_df
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("status")
    )
    .agg(
        count("*").alias("order_count"),
        sum("amount").alias("total_amount")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("status"),
        col("order_count"),
        col("total_amount")
    )
)

query = (
    metrics_df.writeStream
    .format("iceberg")
    .outputMode("append")
    .option(
        "checkpointLocation",
        "s3a://dain-iceberg-lab-2026/checkpoints/orders_metrics_5m"
    )
    .toTable("glue_catalog.lakehouse_lab.orders_status_metrics_5m")
)

query.awaitTermination()
