from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


def create_spark_session():
    return SparkSession.builder.appName("CryptoStreaming"). \
        config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0"). \
        getOrCreate()

def process_streaming_data(spark, kafka_bootstrap_servers, kafka_topic):
    # Define the schema for the incoming JSON data
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("name", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("last_updated", StringType(), True),
        StructField("current_time", StringType(), True)
    ])

    # Read data from Kafka topic
    kafka_stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic) \
        .option('startingOffsets', 'latest') 
        .load()
    )

    # Deserialize JSON data and enforce schema
    value_column = from_json(col("value").cast("string"), schema)
    crypto_data_df = kafka_stream_df.select(value_column.alias("crypto_data"))

    # Cast current_time to timestamp
    select_data = crypto_data_df.withColumn("current_time", col("crypto_data.current_time").cast("timestamp"))

    # # Apply watermark to handle late data it will ignore data that come after 3 minutes from current time
    #windowed_data_df = select_data.withWatermark("current_time", "3 minutes")

    # Extract start and end of the window and create separate columns
    windowed_data_df = select_data.select(
        "crypto_data.id",
        "crypto_data.symbol",
        "crypto_data.price",
        "current_time",
        window("current_time", "3 minutes", "2 minutes").alias("window")
    ).select(
        "id",
        "symbol",
        "price",
        "current_time",
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end")
    )

    query = (
        windowed_data_df.writeStream.outputMode("append")
        .format("csv")
        .option("checkpointLocation", "/Users/rawatg/Desktop/aws_project/spark_streaming_pipeline/src/checkpoint")
        .start("/Users/rawatg/Desktop/aws_project/spark_streaming_pipeline/src/output")
    )

    # Await termination
    query.awaitTermination()

if __name__ == "__main__":
    kafka_bootstrap_servers = 'localhost:9092'
    kafka_topic = 'selected_crypto_data'

    spark = create_spark_session()
    process_streaming_data(spark, kafka_bootstrap_servers, kafka_topic)
