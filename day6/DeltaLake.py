from pyspark.sql import SparkSession
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

data = [(1, "alice"), (2, "bob"), (3, "carol")]
df = spark.createDataFrame(data, ["id", "name"])

delta_path = "output/path/delta_table"
df.write.format("delta").mode("overwrite").save(delta_path)

df2 = spark.createDataFrame([(4, "david"), (5, "eva")], ["id", "name"])
df2.write.format("delta").mode("append").save(delta_path)

print("Delta table content:")
spark.read.format("delta").load(delta_path).show()

DeltaTable.forPath(spark, delta_path).vacuum(0.0)

log_path = os.path.join(delta_path, "_delta_log")
print("Files inside _delta_log:")
print(os.listdir(log_path))