from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from delta import configure_spark_with_delta_pip
import os

builder = SparkSession.builder \
    .appName("Delta Example with CVs") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df = spark.read.json("employees.json")

delta_path = "output/path/delta_table"

df.write.format("delta").mode("overwrite").save(delta_path)
print("Original data:")
spark.read.format("delta").load(delta_path).show()

cv_folder = r"C:/Users/lakshmi.sudha/Documents/trainingPractice/day6/files"
print(f"Looking for CVs in: {cv_folder}")

cv_rows = []

for file in os.listdir(cv_folder):
    if file.endswith(".txt") and file.startswith("CV"):
        file_id = file[2:-4]
        cv_path = os.path.join(cv_folder, file)
        with open(cv_path, "r", encoding="utf-8") as f:
            cv_text = f.read().strip()
        cv_rows.append( (int(file_id), cv_text) )
        print(f"Read CV for ID={file_id}: {file}")

cv_df = spark.createDataFrame(cv_rows, ["id", "cv_data"])

print("CV DataFrame:")
cv_df.show(truncate=False)

df_delta = spark.read.format("delta").load(delta_path)

df_joined = df_delta.join(cv_df, on="id", how="left") \
    .select(df_delta["*"], cv_df["cv_data"])

print("Final Delta Table:")
df_joined.show(truncate=False)

df_joined.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .save(delta_path)

print("All done! Updated Delta saved at:", delta_path)