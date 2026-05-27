from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    current_timestamp
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType
)

spark = (
    SparkSession.builder
    .appName("orders-raw-to-bronze")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("op", StringType(), True),
    StructField("order_id", LongType(), True),
    StructField("user_id", StringType(), True),
    StructField("amount", LongType(), True),
    StructField("status", StringType(), True),
    StructField("event_time", StringType(), True)
])

raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "orders_raw")
    .option("startingOffsets", "latest")
    .load()
)

parsed_df = (
    raw_df
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
    .withColumn(
        "event_time",
        to_timestamp(col("event_time"))
    )
    .withColumn(
        "ingest_time",
        current_timestamp()
    )
)

query = (
    parsed_df.writeStream
    .format("iceberg")
    .outputMode("append")
    .option(
        "checkpointLocation",
        "s3a://dain-iceberg-lab-2026/checkpoints/orders_bronze"
    )
    .toTable("glue_catalog.demo.orders_bronze")
)

query.awaitTermination()
