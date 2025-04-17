from pyspark.sql import  SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType
from pyspark.sql.functions import from_json

spark = SparkSession.builder.appName("SparkStreaming_Application") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0").getOrCreate()

kafka_bootstrap_server = '127.0.0.1:9092'
kafka_topic = 'users_input'


# Read data from Kafka topic
kafka_stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_server)
    .option("subscribe", kafka_topic)
    .option("startingOffsets", '{"users_input":{"0":10}}')
    #.option("startingOffsets", 'latest')
    .option("maxOffsetsPerTrigger", 20)
    .load()
)

#  1st Offset File:

v1
{"batchWatermarkMs":0,"batchTimestampMs":1744887126959,"conf":{"spark.sql.streaming.stateStore.providerClass":"org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider","spark.sql.streaming.join.stateFormatVersion":"2","spark.sql.streaming.stateStore.compression.codec":"lz4","spark.sql.streaming.stateStore.rocksdb.formatVersion":"5","spark.sql.streaming.statefulOperator.useStrictDistribution":"true","spark.sql.streaming.flatMapGroupsWithState.stateFormatVersion":"2","spark.sql.streaming.multipleWatermarkPolicy":"min","spark.sql.streaming.aggregation.stateFormatVersion":"2","spark.sql.shuffle.partitions":"200"}}
{"users_input":{"0":30}}

#2nd Offset File:
v1
{"batchWatermarkMs":0,"batchTimestampMs":1744887132532,
 "conf":{"spark.sql.streaming.stateStore.providerClass":"org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider","spark.sql.streaming.join.stateFormatVersion":"2","spark.sql.streaming.stateStore.compression.codec":"lz4","spark.sql.streaming.stateStore.rocksdb.formatVersion":"5","spark.sql.streaming.statefulOperator.useStrictDistribution":"true", "spark.sql.streaming.flatMapGroupsWithState.stateFormatVersion":"2","spark.sql.streaming.multipleWatermarkPolicy":"min","spark.sql.streaming.aggregation.stateFormatVersion":"2","spark.sql.shuffle.partitions":"200"}}
{"users_input":{"0":50}}



inferred_schema = StructType([
    StructField("transactionId", StringType(), True),
    StructField("name", StringType(), True),
    StructField("userId", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("transactiontime", StringType(), True),
    StructField("transactionType", StringType(), True),
    StructField("city", StringType(), True)

])


json_df = kafka_stream_df.selectExpr("CAST(value AS STRING) AS message")

print("json Dataframe")
# query = json_df.writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .start()

#inferred_schema = spark.read.json(json_df.rdd.map(lambda r: r.message)).schema

parsed_df = json_df.select(from_json("message", inferred_schema).alias("data"))
final_df = parsed_df.selectExpr("data.*")



query = final_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate",False) \
    .option("checkpointLocation","/Users/rawatg/Desktop/aws_project/spark_streaming_pipeline/spark/main/producer_consumer_basic/checkpoint") \
    .start()

query.awaitTermination()
