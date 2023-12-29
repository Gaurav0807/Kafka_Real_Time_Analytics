from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

def create_spark_session():
    return SparkSession.builder.appName("CryptoStreaming"). \
        config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0"). \
        getOrCreate()

def process_streaming_data(spark, kafka_bootstrap_servers, kafka_topic):
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("name", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("last_updated", StringType(), True)
        #StructField("commits", IntegerType(), True)
    ])

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

    # query = (
    #     crypto_data_df.writeStream.outputMode("append")
    #     .format("console")
    #     .start()
    # )
    # query = (
    #     crypto_data_df.writeStream.outputMode("append")
    #     .format("json")
    #     .option("checkpointLocation","/Users/grawa1/Desktop/Open_Source/Kafka/src/real_time_api/checkpoint")
    #     .start("/Users/grawa1/Desktop/Open_Source/Kafka/src/real_time_api/output")
    # )

    select_data = crypto_data_df.select("crypto_data.id","crypto_data.symbol","crypto_data.price")


    #Fixed Interval Trigger
    query = (
        select_data.writeStream.trigger(processingTime="10 seconds").outputMode("append") #Fixed Interval Trigger
        .format("console").option("truncate",False)
        .start()
    )

    #trigger(once=True) :- One-Time

    # Continous
    # query = (
    #     select_data.writeStream.trigger(continuous="10 seconds").outputMode("append") #Fixed Interval Trigger
    #     .format("console").option("truncate",False)
    #     .start()
    # )
    






    # Await termination
    query.awaitTermination()

if __name__ == "__main__":
    kafka_bootstrap_servers = 'localhost:9092'
    kafka_topic = 'selected_crypto_data'
    spark = create_spark_session()


    process_streaming_data(spark, kafka_bootstrap_servers, kafka_topic)




#1.) Metadata file :- Inside that id is serves as unique indentifier for particular spark streaming job.
#  If job is stopped and rhen restarted. Spark will use this identifier to recover state and continue processing from where it left off.