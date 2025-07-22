from pyspark.sql import SparkSession
from pyspark.sql.functions import col,sum,avg
spark=SparkSession.builder\
.appName("Lab3 spark practice")\
.master("local[*]")\
.getOrCreate()



df=spark.read.parquet("output_partitioned_by_region.parquet")

df.show()


df_filtered = df.filter(col("region") == "East")

df_grouped = df_filtered.groupBy("region").agg(
    sum("total_duration").alias("region_total_duration"),
    avg("avg_duration").alias("region_avg_duration")
)

df_transformed = df_grouped.withColumn(
    "region_duration_hours",
    col("region_total_duration") / 60
)

df_cached = df_transformed.cache()
df_cached.count()

result_cached = df_cached.orderBy(col("region_total_duration").desc())

result_cached.show()


spark.sparkContext.setCheckpointDir("chkpt_dir")
df_checkpointed = df_transformed.checkpoint(eager=True)

result_checkpoint = df_checkpointed.orderBy(col("region_total_duration").desc())

result_checkpoint.show()