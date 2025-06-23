from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("BatchETLJob").getOrCreate()

input_path = "s3a://etl-dataproc/raw/transactions.csv"
output_path = "s3a://etl-dataproc/processed/transactions_cleaned"

df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

df_clean = df.dropna(subset=["transaction_id", "user_id", "amount"])

df_clean = df_clean.filter(col("amount") > 0)

df_clean.write.mode("overwrite").parquet(output_path)

print(f"Successfully processed data saved to {output_path}")
