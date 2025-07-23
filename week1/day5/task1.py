from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark=SparkSession.builder\
.appName("DataFrame Lab")\
.master("local[*]")\
.getOrCreate()

print("spark session created")


products= spark.read.csv("products.csv",header=True, inferSchema=True)

orders= spark.read.csv("orders.csv",header=True, inferSchema=True)

data=products.join(orders, products["id"]== orders["product_id"],"inner").withColumn("revenue", col("price") * col("quantity"))
data=data.groupBy("category").sum("revenue")

data.show()