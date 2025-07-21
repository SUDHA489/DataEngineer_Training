from pyspark.sql import SparkSession
from pyspark.sql.functions import avg,sum,col

spark=SparkSession.builder\
.appName("DataFrame Lab")\
.master("local[*]")\
.getOrCreate()


df1= spark.read.csv("users.csv",header=True,inferSchema=True)

df2= spark.read.csv("activity.csv",header=True, inferSchema=True)

filter_data_by_age=df1.filter("age > 30").join(df2,df1["id"]==df2["user_id"],"inner")

agg_data = filter_data_by_age.groupBy("user_id").agg(
    sum("duration_min").alias("total_duration"),
    avg("duration_min").alias("avg_duration")
).withColumn("duration_hrs",col("total_duration")/60)

final_data = agg_data.join(
    df1.select("id", "name", "region"),
    agg_data["user_id"] == df1["id"],
    "left"
).select("user_id", "name", "region", "total_duration", "avg_duration", "duration_hrs")

final_data.show()

final_data.write.mode("overwrite").parquet("output_path/sample")