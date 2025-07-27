from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import *

builder = SparkSession.builder \
    .appName("ecommerce data vault model") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# reading csv files
customers = spark.read.csv("data/dim_customer.csv",header=True,inferSchema=True)
products = spark.read.csv("data/dim_product.csv",header=True,inferSchema=True)
sales = spark.read.csv("data/fact_sales.csv",header=True,inferSchema=True)


# converting csv files in to delta tables
customers.write.format("delta").mode("overwrite").save("delta/raw/delta_customers")
products.write.format("delta").mode("overwrite").save("delta/raw/delta_products")
sales.write.format("delta").mode("overwrite").save("delta/raw/delta_fact_sales")


# delta tables for dim_customer, dim_product, fact_sales
delta_customers = spark.read.format("delta").load("delta/raw/delta_customers")
delta_products = spark.read.format("delta").load("delta/raw/delta_products")
delta_fact_sales = spark.read.format("delta").load("delta/raw/delta_fact_sales")

# creating hubs
hub_customer = delta_customers\
.withColumn("customer_hashkey",sha2(delta_customers.customer_id.cast("string"),256))\
.withColumn("LoadDate",current_timestamp())\
.withColumn("RecordSource",lit("delta_customers"))\
.select("customer_hashkey","customer_id","LoadDate","RecordSource")


hub_product = delta_products\
.withColumn("product_hashkey",sha2(delta_products.product_id.cast("string"),256))\
.withColumn("LoadDate",current_timestamp())\
.withColumn("RecordSource",lit("delta_products"))\
.select("product_hashkey","product_id","LoadDate","RecordSource")

hub_sales = delta_fact_sales\
.withColumn("sales_hashkey",sha2(delta_fact_sales.sales_id.cast("string"),256))\
.withColumn("LoadDate",current_timestamp())\
.withColumn("RecordSource",lit("delta_fact_sales"))\
.select("sales_hashkey","sales_id","LoadDate","RecordSource")


hub_customer.write.format("delta").mode("overwrite").save("delta/hubs/delta_hub_customers")
hub_product.write.format("delta").mode("overwrite").save("delta/hubs/delta_hub_products")
hub_sales.write.format("delta").mode("overwrite").save("delta/hubs/delta_hub_sales")

delta_hub_customers=spark.read.format("delta").load("delta/hubs/delta_hub_customers")
delta_hub_products=spark.read.format("delta").load("delta/hubs/delta_hub_products")
delta_hub_sales=spark.read.format("delta").load("delta/hubs/delta_hub_sales")


# creating links

link_sales_customer = delta_fact_sales\
.withColumn("link_sales_customer_hashkey",sha2(concat_ws("+",delta_fact_sales.sales_id.cast("string"),delta_fact_sales.customer_id.cast("string")),256))\
.withColumn("sales_hashkey",sha2(delta_fact_sales.sales_id.cast("string"),256))\
.withColumn("customer_hashkey",sha2(delta_fact_sales.customer_id.cast("string"),256))\
.withColumn("LoadDate",current_timestamp())\
.withColumn("RecordSource",lit("delta_fact_sales"))\
.select("link_sales_customer_hashkey","sales_hashkey","customer_hashkey","LoadDate","RecordSource")


link_sales_product = delta_fact_sales\
.withColumn("link_sales_product_hashkey",sha2(concat(delta_fact_sales.sales_id.cast("string"),delta_fact_sales.product_id.cast("string")),256))\
.withColumn("sales_hashkey",sha2(delta_fact_sales.sales_id.cast("string"),256))\
.withColumn("product_hashkey",sha2(delta_fact_sales.product_id.cast("string"),256))\
.withColumn("LoadDate",current_timestamp())\
.withColumn("RecordSource",lit("delta_fact_sales"))\
.select("link_sales_product_hashkey","sales_hashkey","product_hashkey","LoadDate","RecordSource")


link_sales_customer.write.format("delta").mode("overwrite").save("delta/links/delta_link_sales_customer")
link_sales_product.write.format("delta").mode("overwrite").save("delta/links/delta_link_sales_product")

delta_link_sales_customer = spark.read.format("delta").load("delta/links/delta_link_sales_customer")
delta_link_sales_product = spark.read.format("delta").load("delta/links/delta_link_sales_product")


# creating satellites

sat_customer_details = delta_customers\
.withColumn("customer_hashkey",sha2(delta_customers.customer_id.cast("string"),256))\
.withColumn("LoadDate",current_timestamp())\
.withColumn("RecordSource",lit("delta_customers"))\
.select("customer_hashkey","first_name","last_name","email","contact_number","country","city","LoadDate","RecordSource")


sat_product_details = delta_products\
.withColumn("product_hashkey",sha2(delta_products.product_id.cast("string"),256))\
.withColumn("LoadDate",current_timestamp())\
.withColumn("RecordSource",lit("delta_products"))\
.select("product_hashkey","product_name","category","sub_category","price","date_released","LoadDate","RecordSource")


sat_sales_details = delta_fact_sales\
.withColumn("sales_hashkey",sha2(delta_fact_sales.sales_id.cast("string"),256))\
.withColumn("LoadDate",current_timestamp())\
.withColumn("RecordSource",lit("delta_fact_sales"))\
.select("sales_hashkey","quantity","unit_price","order_date","LoadDate","RecordSource")


sat_customer_details.write.format("delta").mode("overwrite").save("delta/satellites/delta_sat_customer_details")
sat_product_details.write.format("delta").mode("overwrite").save("delta/satellites/delta_sat_product_details")
sat_sales_details.write.format("delta").mode("overwrite").save("delta/satellites/delta_sat_sales_details")


delta_sat_customer_details = spark.read.format("delta").load("delta/satellites/delta_sat_customer_details")
delta_sat_product_details = spark.read.format("delta").load("delta/satellites/delta_sat_product_details")
delta_sat_sales_details = spark.read.format("delta").load("delta/satellites/delta_sat_sales_details")


spark.stop()