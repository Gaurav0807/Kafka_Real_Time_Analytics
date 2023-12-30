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



    #Stateless Transformation :- 
    #stateless_df = crypto_data_df.withColumn("indian_price",expr("crypto_data.price * 83.25"))
    #select_data = stateless_df.select("crypto_data.id","crypto_data.symbol","crypto_data.price","indian_price")
    #Output Mode :- Append


    #StateFull Transformation :- It create state folder with statefull transfotmation. Inside that we might find state directory with delta files.
    # These delta files represent the state information for maintaining the state across batches in a fault-tolerance.
    stateful_df = crypto_data_df.groupBy("crypto_data.id","crypto_data.symbol").agg(expr("sum(crypto_data.price)").alias("cumulative_price"))

    select_data = stateful_df.select("id","symbol","cumulative_price")

    query = (
        select_data.writeStream.outputMode("update")
        .format("console").option("truncate",False)
        .option("checkpointLocation","/Users/grawa1/Desktop/Open_Source/Kafka/src/real_time_api/checkpoint")
        .start()
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



