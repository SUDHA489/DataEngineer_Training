from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import *

builder = SparkSession.builder \
    .appName("running pit table scripit") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

print(" delta_sat_sales rows :")
df = spark.read.format("delta").load("delta/satellites/delta_sat_sales_details")
df.show()

latest_per_key = df.groupBy("sales_hashkey").agg(
    max(df.LoadDate).alias("latestLoadDate")
)


pit_df = df.join(
    latest_per_key,
    (df.sales_hashkey == latest_per_key.sales_hashkey) & 
    (df.LoadDate == latest_per_key.latestLoadDate),
    "inner"
).select(df["*"])


pit_df.write.format("delta").mode("overwrite").save("delta/pits/delta_pit_sales")

pit_df = spark.read.format("delta").load("delta/pits/delta_pit_sales")

print(" pit table rows with latest loaddate for each hashkey:")
pit_df.show()