from pyspark.sql import SparkSession

spark=SparkSession.builder\
.appName("DataFrame Lab")\
.master("local[*]")\
.getOrCreate()

print("spark session created")
print(spark.version)


# count no of lines in csv
countlines=spark.read.text("data.csv").count()
print(countlines)


# reading the csv file data
df=spark.read.csv("data.csv",header=True,inferSchema=True)
df.show()

# after filtering data

filter_data= df.filter("paid_amount > 100")
print("showing filtered data ....")
filter_data.show()

# using group by 
filter_data=df.groupBy("zone_id").sum("paid_amount")
filter_data.show()

# using column expression

filter_data=df.withColumn("tax",df.paid_amount*0.18)
filter_data.show()