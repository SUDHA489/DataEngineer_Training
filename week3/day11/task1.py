from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time

spark = SparkSession.builder \
    .appName("DummyStream") \
    .master("local[*]") \
    .getOrCreate()

stream_df = spark.readStream.format("rate").option("rowsPerSecond", 10).load()

parking_df = stream_df.withColumn(
    "terminal", expr("CASE WHEN value % 3 = 0 THEN 'A' WHEN value % 3 = 1 THEN 'B' ELSE 'C' END")
).withColumn(
    "zone", expr("concat('P', (value % 5) + 1)")
).withColumn(
    "slot_id", (col("value") % 500) + 1
).withColumn(
    "status", expr("CASE WHEN value % 4 = 0 THEN 'Occupied' ELSE 'Available' END")
).withColumn(
    "event_time", from_unixtime(col("timestamp").cast("long")).cast("timestamp")
)

zoneCongestionQuery = parking_df \
    .groupBy("terminal", "zone", "status", window("event_time", "1 minute")) \
    .agg(count("*").alias("status_count")) \
    .filter("status = 'Occupied' AND status_count > 40") \
    .select("terminal", "zone", "status_count", "window")

averageQuery = parking_df \
    .withColumn("occupied", expr("CASE WHEN status='Occupied' THEN 1 ELSE 0 END")) \
    .groupBy(window("event_time","15 seconds","5 seconds"),"terminal") \
    .agg(avg("occupied").alias("avg_ocup")) \
    .select(expr("avg_ocup > 0.85").alias("is_high_occupancy"))

query1 = zoneCongestionQuery.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .queryName("ZoneCongestion") \
    .trigger(processingTime='5 seconds') \
    .start()

query2 = averageQuery.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .queryName("TerminalOccupancy") \
    .trigger(processingTime='5 seconds') \
    .start()

query1.awaitTermination()
query2.awaitTermination()