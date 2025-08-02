from pyspark.sql import SparkSession
import time

spark = SparkSession.builder\
.appName("reading files from azure")\
.master("local[*]")\
.getOrCreate()

df = spark.readStream.format("cloudFiles").option("cloudFiles.format","csv").load("https://blobstoragetestvikash551.blob.core.windows.net/csvfiles/customers-100.csv?sp=r&st=2025-07-28T11:51:43Z&se=2025-07-28T20:06:43Z&spr=https&sv=2024-11-04&sr=b&sig=85gz87jJX3YR6hJ%2FhMP0SvnZLAyTuTvXbtwbPFLz2%2FE%3D")

df.writeStream.format("console").outputMode("append").option("truncate",False).start()