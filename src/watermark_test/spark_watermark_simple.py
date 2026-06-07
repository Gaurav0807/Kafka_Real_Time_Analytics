"""
Simple Watermark Demo - Single Stream

This shows watermark basics without the complexity of joins.
Observe how late events are handled.

Key concepts:
1. WATERMARK = max(event_time) - threshold
2. Events with event_time < watermark are DROPPED
3. Watermark allows Spark to clear old aggregation state

Example:
- Watermark threshold: 1 minute
- Max event time seen: 10:05:00
- Current watermark: 10:04:00
- Event with time 10:03:30 -> ACCEPTED (after watermark)
- Event with time 10:02:00 -> DROPPED (before watermark)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    window,
    count,
    max as spark_max,
    min as spark_min,
    current_timestamp,
)
from pyspark.sql.types import StructType, StructField, StringType


IMPRESSION_SCHEMA = StructType([
    StructField("impression_id", StringType(), True),
    StructField("ad_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("timestamp", StringType(), True),
])

KAFKA_BROKER = "broker:29092"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("SimpleWatermarkDemo")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 70)
    print("SIMPLE WATERMARK DEMO - Impressions Aggregation")
    print("=" * 70)
    print("""
    Watermark = 1 minute
    Window   = 30 seconds (tumbling)

    Watch how:
    - Late events (marked [LATE] in producer) may be dropped
    - Aggregation state is cleaned up after watermark passes
    """)

    # Read from Kafka
    impressions_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", "impressions")
        .option("startingOffsets", "latest")
        .load()
        .select(from_json(col("value").cast("string"), IMPRESSION_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_ts", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("process_ts", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
    )

    # ============================================================
    # WATERMARK: Allow 1 minute late data
    # ============================================================
    watermarked_df = impressions_df.withWatermark("event_ts", "1 minute")

    # ============================================================
    # WINDOWED AGGREGATION
    # ============================================================
    agg_df = (
        watermarked_df
        .groupBy(
            window(col("event_ts"), "30 seconds"),  # 30-second window
            col("ad_id")
        )
        .agg(
            count("*").alias("count"),
            spark_min("event_ts").alias("min_event_time"),
            spark_max("event_ts").alias("max_event_time"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("ad_id"),
            col("count"),
            col("min_event_time"),
            col("max_event_time"),
        )
        .orderBy("window_start", "ad_id")
    )

    print("\nStarting stream... (Press Ctrl+C to stop)\n")

    # Output to console
    query = (
        agg_df.writeStream
        .outputMode("complete")  # Show all aggregations
        .format("console")
        .option("truncate", "false")
        .option("numRows", 20)
        .trigger(processingTime="5 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()


# [309] Sent impression: imp_1780768187440_4538 | ad=ad_003 | user=user_006 | event_time=2026-06-06 23:19:47
# [310] Sent impression: imp_1780768189398_5640 | ad=ad_003 | user=user_007 | event_time=2026-06-06 23:18:52 [LATE]