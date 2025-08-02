from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("with out watermark").getOrCreate()

df = spark.readStream.format("rate").option('rowsPerSecond',5).load()

df = df.withColumn(
    "eventTime",
    when(
        col("value") % 3 == 0,
        expr("timestamp - INTERVAL 5 seconds")
    ).otherwise(col("timestamp"))
)

with_watermark = df.withWatermark("eventTime","5 seconds").groupBy(
    window(col("eventTime"),"10 seconds")
).count().withColumnRenamed("count", "count_with_watermark")


q1 = with_watermark.writeStream\
.outputMode("complete").format("console").option("truncate",False)\
.option("checkpointLocation","/tmp/checkpoint_with_watermark")\
.queryName("withWatermark")\
.start()

q1.awaitTermination()