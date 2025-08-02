from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark =  SparkSession.builder.appName("micro batch processing").getOrCreate()

rate_df =  spark.readStream.format("rate").option("rowsPerSecond",5).load()

transformed_df = rate_df.withColumn("value_times_2",col("value")*2)

query = transformed_df.writeStream.outputMode("append")\
    .format("console").start()

query.awaitTermination()