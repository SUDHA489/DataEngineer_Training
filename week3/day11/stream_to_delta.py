from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder \
    .appName("stream to delta conversion") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df = spark.readStream.format("rate").option("rowsPerSecond", 5).load()

result = df

query = (result.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "C:/Users/lakshmi.sudha/Documents/trainingPractice/week3/day11/delta_checkpoint")\
    .start("C:/Users/lakshmi.sudha/Documents/trainingPractice/week3/day11/delta_table")

)

query.awaitTermination(30)

reading_data = spark.read.format("delta").load("delta_table")

reading_data.show()