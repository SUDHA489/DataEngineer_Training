from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import *

builder = SparkSession.builder \
    .appName("data vault model") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

customers = spark.read.option("header", True).csv("customers.csv")
products = spark.read.option("header", True).csv("products.csv")
sales= spark.read.option("header", True).csv("sales.csv")

customers.write.format("delta").mode("overwrite").save("./delta/raw/delta_customers")
products.write.format("delta").mode("overwrite").save("./delta/raw/delta_products")
sales.write.format("delta").mode("overwrite").save("./delta/raw/delta_sales")

delta_customers=spark.read.format("delta").load("./delta/raw/delta_customers")
delta_products=spark.read.format("delta").load("./delta/raw/delta_products")
delta_sales=spark.read.format("delta").load("./delta/raw/delta_sales")


print("showing delta_customers:")
delta_customers.show()
print("showing delta_products:")
delta_products.show()
print("showing delta_sales:")
delta_sales.show()


hub_customer=delta_customers.withColumn(
    "customer_hashkey",
    sha2(delta_customers.customer_id.cast("string"),256)
).withColumn("LoadDate", current_timestamp()) \
 .withColumn("RecordSource", lit("deltaCustomerSourceSystem")) \
 .select("customer_hashkey", "customer_id", "LoadDate", "RecordSource")


hub_product = delta_products.withColumn(
    "product_hashkey",
    sha2(delta_products.product_id.cast("string"), 256)
).withColumn("LoadDate", current_timestamp()) \
 .withColumn("RecordSource", lit("deltaProductSourceSystem")) \
 .select("product_hashkey", "product_id", "LoadDate", "RecordSource")


hub_sales = delta_sales.withColumn(
    "sales_hashkey",
    sha2(delta_sales.sales_id.cast("string"), 256)
).withColumn("LoadDate", current_timestamp()) \
 .withColumn("RecordSource", lit("deltaSalesSourceSystem")) \
 .select("sales_hashkey", "sales_id", "LoadDate", "RecordSource")


hub_customer.write.format("delta").mode("overwrite").save("./delta/hubs/hub_customer")
hub_product.write.format("delta").mode("overwrite").save("./delta/hubs/hub_product")
hub_sales.write.format("delta").mode("overwrite").save("./delta/hubs/hub_sales")


delta_hub_customer = spark.read.format("delta").load("./delta/hubs/hub_customer")
delta_hub_product = spark.read.format("delta").load("./delta/hubs/hub_product")
delta_hub_sales = spark.read.format("delta").load("./delta/hubs/hub_sales")

print("showing delta_hub_customer:")
delta_hub_customer.show()
print("showing delta_hub_product:")
delta_hub_product.show()
print("showing delta_hub_sales:")
delta_hub_sales.show()

links_sales_customer=delta_sales\
    .withColumn("sales_hashkey",sha2(delta_sales.sales_id.cast("string"), 256))\
.withColumn("customer_hashkey",sha2(delta_sales.customer_id.cast("string"), 256))\
.withColumn("links_sales_customer_hashkey",sha2(concat_ws(" ","sales_hashkey","customer_hashkey").cast("string"), 256))\
.withColumn("LoadDate",current_timestamp())\
.withColumn("RecordSource",lit("deltaSalesSourceSystem"))\
.select("links_sales_customer_hashkey","sales_hashkey","customer_hashkey","LoadDate","RecordSource")


links_sales_product=delta_sales\
.withColumn("sales_hashkey",sha2(delta_sales.sales_id.cast("string"), 256))\
.withColumn("product_hashkey",sha2(delta_sales.product_id.cast("string"), 256))\
.withColumn("links_sales_customer_hashkey",sha2(concat_ws(" ","sales_hashkey","product_hashkey").cast("string"), 256))\
.withColumn("LoadDate",current_timestamp())\
.withColumn("RecordSource",lit("deltaSalesSourceSystem"))\
.select("links_sales_customer_hashkey","product_hashkey","sales_hashkey","LoadDate","RecordSource")



links_sales_customer.write.format("delta").mode("overwrite").save("./delta/links/links_sales_customer")
links_sales_product.write.format("delta").mode("overwrite").save("./delta/links/links_sales_product")


delta_links_sales_customer=spark.read.format("delta").load("./delta/links/links_sales_customer")
delta_links_sales_product=spark.read.format("delta").load("./delta/links/links_sales_product")

print("showing delta_links_sales_customer:")
delta_links_sales_customer.show()
print("showing delta_links_sales_product:")
delta_links_sales_product.show()


sat_customer_details = delta_customers \
  .withColumn("customer_hashkey", sha2(delta_customers.customer_id.cast("string"), 256)) \
  .withColumn("LoadDate", current_timestamp()) \
  .withColumn("RecordSource", lit("deltaCustomerSourceSystem")) \
  .select("customer_hashkey", "name", "address", "contact_details", "LoadDate", "RecordSource")


sat_product_details = delta_products \
  .withColumn("product_hashkey", sha2(delta_products.product_id.cast("string"), 256)) \
  .withColumn("LoadDate", current_timestamp()) \
  .withColumn("RecordSource", lit("deltaProductSourceSystem")) \
  .select("product_hashkey", "product_name", "category", "price", "LoadDate", "RecordSource")


sat_sales_details = delta_sales \
  .withColumn("sales_hashkey", sha2(delta_sales.sales_id.cast("string"), 256)) \
  .withColumn("LoadDate", current_timestamp()) \
  .withColumn("RecordSource", lit("deltaSalesSourceSystem")) \
  .select("sales_hashkey", "purchase_date", "quantity", "sales_amount", "LoadDate", "RecordSource")



sat_customer_details.write.format("delta").mode("overwrite").save("./delta/sats/sat_customer_details")
sat_product_details.write.format("delta").mode("overwrite").save("./delta/sats/sat_product_details")
sat_sales_details.write.format("delta").mode("overwrite").save("./delta/sats/sat_sales_details")


delta_sat_customer_details = spark.read.format("delta").load("./delta/sats/sat_customer_details")
delta_sat_product_details = spark.read.format("delta").load("./delta/sats/sat_product_details")
delta_sat_sales_details = spark.read.format("delta").load("./delta/sats/sat_sales_details")

print("showing delta_sat_customer_details")
delta_sat_customer_details.show()
print("showing delta_sat_product_details")
delta_sat_product_details.show()
print("showing delta_sat_sales_details")
delta_sat_sales_details.show()