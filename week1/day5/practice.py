from pyspark.sql import SparkSession
import time
spark=SparkSession.builder\
.appName("parquet partitioning practice")\
.master("local[*]")\
.getOrCreate()

t1=time.time()
df=spark.read.csv("orders.csv",header=True,inferSchema=True)
df.show()
t2=time.time()
print("execution time for reading csv:",t2-t1)
df.write.partitionBy("region").parquet("partitionFiles/")

t3=time.time()
df=spark.read.parquet("partitionFiles/")
df.show()
t4=time.time()
print("execution time for reading partition files:",t4-t3)