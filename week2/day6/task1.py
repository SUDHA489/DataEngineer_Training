from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
from delta.tables import DeltaTable
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder \
    .appName("Delta Example") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df=spark.read.csv("regions.csv",header=True,inferSchema=True)

delta_path = "delta_table"
df.write.format("delta").mode("overwrite").save(delta_path)


df_filtered = df.filter(df.Country == 'USA')
df_filtered.show()