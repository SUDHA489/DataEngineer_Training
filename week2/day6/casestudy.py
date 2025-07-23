from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder \
    .appName("Case Study Example") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

data = [
    (1, 'Alice', 'UK'),
    (2, 'Bob', 'Germany'),
    (3, 'Carlos', 'Spain')
]

df = spark.createDataFrame(data, ['user_id', 'name', 'country'])

df.write.mode("overwrite").parquet('/tmp/customer_parquet')

parquet_df = spark.read.parquet("/tmp/customer_parquet")


parquet_df.write.mode("overwrite").format("delta").save("/tmp/customer_delta")


spark.sql("CREATE TABLE IF NOT EXISTS customer_delta USING DELTA LOCATION '/tmp/customer_delta'")

spark.sql("SELECT * FROM customer_delta").show()

spark.sql("DELETE FROM customer_delta WHERE user_id = 2")


spark.sql("SELECT * FROM customer_delta").show()

spark.sql("DESCRIBE HISTORY customer_delta").show()


spark.sql("SELECT * FROM customer_delta VERSION AS OF 0")

spark.sql("RESTORE TABLE customer_delta TO VERSION AS OF 0")

spark.sql("SELECT * FROM customer_delta").show()