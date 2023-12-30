from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

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
        StructField("last_updated", StringType(), True)
        #StructField("commits", IntegerType(), True)
    ])

    # Read data from Kafka topic
    kafka_stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic) \
        .option('startingOffsets', 'earliest') 
        .load()
    )

    # Deserialize JSON data and enforce schema
    value_column = from_json(col("value").cast("string"), schema)
    crypto_data_df = kafka_stream_df.select(value_column.alias("crypto_data"))

    crypto_data_df = crypto_data_df.withColumn("timestamp",col("crypto_data.last_updated").cast("timestamp"))

    window_duration = "10 minutes"
    sliding_interval = "5 minutes"
    watermark_delay = "10 minutes"


    #Tumbling Window :- Non-overlapping here every 5 minutes it trigger and process it . 
    # Suppose a event occur at 10:05 it will assign to tumbling window that spands from 10:05 to 10:10. 
    # Each window represents a distinct time period, and events within that period are processed together as group.
    # window_duration = "5 minutes"
    # windowed_data = (
    #     crypto_data_df.groupBy(window("timestamp",window_duration))
    #     .agg(
    #     sum("crypto_data.price").alias("total_price")
    #     )
    # )

    #Sliding window:- 
    # windowed_data = (
    #     crypto_data_df.groupBy(window("timestamp",window_duration,sliding_interval))
    #     .agg(
    #     sum("crypto_data.price").alias("total_price")
    #     )
    # )
    # Watermark :- Stream processing for handling late events. 

    windowed_data = (
        crypto_data_df.withWatermark("timestamp",watermark_delay).groupBy(window("timestamp",window_duration,sliding_interval))
        .agg(
        sum("crypto_data.price").alias("total_price")
        )
    )

    query = (
        windowed_data.writeStream.outputMode("complete")
        .format("console").option("truncate",False)
        .option("checkpointLocation","/Users/grawa1/Desktop/Open_Source/Kafka/src/real_time_api/checkpoint")
        .start("/Users/grawa1/Desktop/Open_Source/Kafka/src/real_time_api/output")
    )

    # query = (
    #     select_data.writeStream.outputMode("append")
    #     .format("json")
    #     .option("checkpointLocation","/Users/grawa1/Desktop/Open_Source/Kafka/src/real_time_api/checkpoint")
    #     .start("/Users/grawa1/Desktop/Open_Source/Kafka/src/real_time_api/output")
    # )






    query.awaitTermination()

if __name__ == "__main__":
    # Define your Kafka configurations
    kafka_bootstrap_servers = 'localhost:9092'
    kafka_topic = 'selected_crypto_data'


    spark = create_spark_session()


    process_streaming_data(spark, kafka_bootstrap_servers, kafka_topic)



