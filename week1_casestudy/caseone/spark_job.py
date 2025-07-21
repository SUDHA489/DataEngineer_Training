from pyspark.sql import SparkSession
from pyspark.sql.functions import sum,col

spark = SparkSession.builder\
    .appName("ParquetProcessingJob") \
    .getOrCreate()

df=spark.read.parquet("output.parquet")
df.show()

result = df.groupBy("zone_id").agg(
    sum(col("paid_amount")).alias("total_paid")
)

result = result.repartition(1)

result.write.mode("overwrite").parquet("zone_totals.parquet")

spark.stop()