from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import *

builder = SparkSession.builder \
    .appName("running scripts") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")

spark = configure_spark_with_delta_pip(builder).getOrCreate()


update_today = [
    (1002, 2, 102, 2, 850, "2023-06-15"),
    (1005, 5, 105, 3, 320, "2023-07-15")
]

update_today = spark.createDataFrame(update_today, ["sales_id", "customer_id", "product_id", "quantity", "unit_price", "order_date"])

update_today = update_today.withColumn("quantity", col("quantity").cast("int"))\
.withColumn("unit_price", col("unit_price").cast("int"))\
.withColumn("order_date", col("order_date").cast("Date"))

update_today = update_today\
.withColumn("sales_hashkey",sha2(update_today.sales_id.cast("string"),256))\
.withColumn("LoadDate",current_timestamp())\
.withColumn("RecordSource",lit("updates_today"))\
.select("sales_hashkey","quantity","unit_price","order_date","LoadDate","RecordSource")

print("appending new rows to existing satellite of sales:")
update_today.write.format("delta").mode("append").save("delta/satellites/delta_sat_sales_details")

print("loading satellite of sales details:")
spark.read.format("delta").load(
    "delta/satellites/delta_sat_sales_details"
).show()


print("creating table on delta_sat_sales_details.")
spark.sql("""
    CREATE TABLE IF NOT EXISTS my_fact_sales
    USING DELTA
    LOCATION 'file:/C:/Users/lakshmi.sudha/Documents/trainingPractice/week2/week2-case_study/delta/satellites/delta_sat_sales_details'
""")

print("seeing all tables:")
spark.sql("show tables").show()

print("getting history of my_fact_sales table")
spark.sql(" describe history my_fact_sales ").show(truncate=False)

print("using zorder by.")
spark.sql(" optimize my_fact_sales zorder by (order_date)")

print("showing my_fact_sales table rows:")
spark.sql("select * FROM my_fact_sales").show()


print("deleting the row with sales_id=1007 ")
spark.sql("delete from my_fact_sales where sales_hashkey = sha2('1007',256)")

print("showing my_fact_sales table rows after deletion:")
spark.sql("select * from my_fact_sales").show()


spark.sql("restore table my_fact_sales to version as of 35")

print(" restoring table after deletion: ")
spark.sql("select * from my_fact_sales").show()

spark.sql("vacuum my_fact_sales RETAIN 0 HOURS")

print("loading satellite of sales details:")
spark.read.format("delta").load(
    "delta/satellites/delta_sat_sales_details"
).show()

spark.stop()