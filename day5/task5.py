from pyspark.sql import SparkSession
from pyspark.sql.functions import sum,col,count
import time

spark=SparkSession.builder\
.appName("performance between rdd and df")\
.master("local[*]")\
.getOrCreate()


df = spark.read.option("header", "true").csv("clicks.csv")

rdd = spark.sparkContext.textFile("clicks.csv")
header = rdd.first()
rdd = rdd.filter(lambda row: row != header).map(lambda row: row.split(","))

t0 = time.time()

df_result = df.groupBy("user_id").agg(count("*").alias("clicks"))
df_result.show()

print("\n--- DataFrame execution plan ---")
df_result.explain()

t1 = time.time()
print(f"DataFrame execution time: {t1 - t0:.4f} seconds")


t2 = time.time()

rdd_result = rdd.map(lambda x: (x[0], 1)) \
                .reduceByKey(lambda a, b: a + b)
print("\n--- RDD results ---")
print(rdd_result.collect())

t3 = time.time()
print(f"RDD execution time: {t3 - t2:.4f} seconds")

spark.stop()