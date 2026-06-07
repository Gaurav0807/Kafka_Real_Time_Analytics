"""
Spark Structured Streaming with Watermark - Ad CTR Analytics

REALISTIC SCENARIO:
===================
1. User sees ad (IMPRESSION) at time T
2. User clicks ad (CLICK) at time T + 3-5 minutes
3. Click data may arrive LATE due to network delay

WATERMARK STRATEGY:
==================
- Impressions watermark: 2 minutes (impressions arrive quickly)
- Clicks watermark: 7 minutes (click happens 3-5 min after impression + network delay)

WHY 7 MINUTES FOR CLICKS?
- Click happens: 3-5 minutes after impression
- Network delay: up to 2 minutes for late arrivals
- Total: 5 + 2 = 7 minutes watermark

Run with:
    docker exec -it spark-master /spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
    /app_code/watermark_test/spark_watermark_join.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    window,
    count,
    expr,
)
from pyspark.sql.types import StructType, StructField, StringType


# Schema for Impression events
IMPRESSION_SCHEMA = StructType([
    StructField("impression_id", StringType(), True),
    StructField("ad_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("timestamp", StringType(), True),
])

# Schema for Click events
CLICK_SCHEMA = StructType([
    StructField("click_id", StringType(), True),
    StructField("impression_id", StringType(), True),
    StructField("ad_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("timestamp", StringType(), True),
])

KAFKA_BROKER = "broker:29092"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("AdCTRAnalytics")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/watermark_checkpoint_v2")
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
        .getOrCreate()
    )


def read_kafka_stream(spark, topic, schema):
    """Read from Kafka topic and parse JSON."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
        .select(from_json(col("value").cast("string"), schema).alias("data"))
        .select("data.*")
        .withColumn("event_ts", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss"))
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    print("=" * 70)
    print("         AD CTR ANALYTICS WITH WATERMARK")
    print("=" * 70)
    print("""
    SCENARIO:
    - Impression happens at time T
    - Click happens at T + 3-5 minutes (user thinks before clicking)
    - Click data may arrive late (network delay)

    WATERMARKS (SHORT FOR DEMO):
    - Impressions: 30 seconds
    - Clicks: 30 seconds

    JOIN CONDITION:
    - Same ad_id
    - Same user_id
    - Click within 10 minutes of impression
    """)
    print("=" * 70)

    # ============================================================
    # STEP 1: Read Impressions with WATERMARK (30 seconds for demo)
    # ============================================================
    impressions_df = read_kafka_stream(spark, "impressions", IMPRESSION_SCHEMA)

    impressions_with_watermark = (
        impressions_df
        .withWatermark("event_ts", "30 seconds")
        .select(
            col("impression_id"),
            col("ad_id").alias("imp_ad_id"),
            col("user_id").alias("imp_user_id"),
            col("event_ts").alias("imp_event_ts"),
        )
    )

    print("[1] Impressions: 30-second watermark")

    # ============================================================
    # STEP 2: Read Clicks with WATERMARK (30 seconds for demo)
    # ============================================================
    clicks_df = read_kafka_stream(spark, "clicks", CLICK_SCHEMA)

    clicks_with_watermark = (
        clicks_df
        .withWatermark("event_ts", "30 seconds")
        .select(
            col("click_id"),
            col("ad_id").alias("clk_ad_id"),
            col("user_id").alias("clk_user_id"),
            col("event_ts").alias("clk_event_ts"),
        )
    )

    print("[2] Clicks: 30-second watermark")

    # ============================================================
    # STEP 3: Stream-Stream JOIN
    # ============================================================
    joined_df = impressions_with_watermark.join(
        clicks_with_watermark,
        expr("""
            imp_ad_id = clk_ad_id AND
            imp_user_id = clk_user_id AND
            clk_event_ts >= imp_event_ts AND
            clk_event_ts <= imp_event_ts + interval 10 minutes
        """),
        "leftOuter"
    )

    print("[3] JOIN: impression + click (same ad, same user, click within 10 min)")

    # ============================================================
    # STEP 4: Aggregate - CTR per Ad per Window
    # ============================================================
    ctr_df = (
        joined_df
        .groupBy(
            window(col("imp_event_ts"), "30 seconds"),  # 30-second tumbling window
            col("imp_ad_id")
        )
        .agg(
            count("impression_id").alias("impressions"),
            count("click_id").alias("clicks"),
        )
        .withColumn("ctr_percent", expr("ROUND(clicks * 100.0 / impressions, 2)"))
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("imp_ad_id").alias("ad_id"),
            col("impressions"),
            col("clicks"),
            col("ctr_percent"),
        )
    )

    print("[4] Aggregation: CTR per ad per 30-second window")

    # ============================================================
    # STEP 5: Output
    # ============================================================
    print("\n" + "=" * 70)
    print("STARTING... (Results appear after ~30-60 seconds)")
    print("=" * 70 + "\n")

    query = (
        ctr_df.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .trigger(processingTime="10 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
