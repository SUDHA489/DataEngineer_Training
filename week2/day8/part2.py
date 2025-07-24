from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import *

builder = SparkSession.builder \
    .appName("scd type 2") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

source_df = spark.createDataFrame([
    (1, "Alice", "New York", "Gold"),
    (2, "Bob", "San Francisco", "Silver"),
    (3, "Charlie", "Los Angeles", "Platinum")
], ["CustomerID", "Name", "Address", "LoyaltyTier"])


new_dim_customer_df = source_df.withColumn(
    "CustomerSK", monotonically_increasing_id()
).withColumn(
    "StartDate", current_date()
).withColumn(
    "EndDate", lit(None).cast("date")
).withColumn(
    "IsCurrent", lit(True)
)

new_dim_customer_df.show()


new_dim_customer_df.write.format("delta").mode("overwrite").save("path/to/dim_customer")


new_source_df = spark.createDataFrame([
    (1, "Alice", "Chicago", "Gold"),     # Address changed!
    (2, "Bob", "San Francisco", "Silver") # Same
], ["CustomerID", "Name", "Address", "LoyaltyTier"])


existing_df = spark.read.format("delta").load("path/to/dim_customer")

joined_df = new_source_df.alias("src").join(
    existing_df.filter("IsCurrent = true").alias("tgt"),
    on="CustomerID",
    how="left"
)

print("after joining :")
joined_df.show()


changed_df = joined_df.filter(
    (col("src.Address") != col("tgt.Address")) |
    (col("src.LoyaltyTier") != col("tgt.LoyaltyTier"))
).select("src.*")

print("Changed rows detected")
changed_df.show()



expired_df = joined_df.filter(
    (col("src.Address") != col("tgt.Address")) |
    (col("src.LoyaltyTier") != col("tgt.LoyaltyTier"))
).select("tgt.CustomerSK").withColumn(
    "EndDate", current_date()
).withColumn(
    "IsCurrent", lit(False)
)

print("Rows to expire")
expired_df.show()