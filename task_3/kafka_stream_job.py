from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

schema = StructType([
    StructField("transaction_id", LongType()),
    StructField("user_id", LongType()),
    StructField("category", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("transaction_datetime", StringType())
])

spark = SparkSession.builder.appName("KafkaETL").getOrCreate()

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "broker.kafka.region.cloud.yandex.net:9091") \
    .option("subscribe", "transactions-stream") \
    .option("startingOffsets", "earliest") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select("data.*")

json_df.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "s3a://etl-exam-storage/checkpoints/") \
    .option("path", "s3a://etl-exam-storage/streamed/") \
    .outputMode("append") \
    .start() \
    .awaitTermination()
