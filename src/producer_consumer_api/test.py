


from pyspark.sql import  SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType
from pyspark.sql.functions import from_json

spark = SparkSession.builder.appName("SparkStreaming_Application") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0").getOrCreate()

df = spark.read.format("orc").load("/Users/rawatg/Desktop/aws_project/spark_streaming_pipeline/spark/main/producer_consumer_basic/checkpoint/offsets")

df.show(truncate=False)