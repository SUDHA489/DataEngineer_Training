from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
import time
spark=SparkSession.builder\
.appName("using broadcast function")\
.master("local[*]")\
.getOrCreate()


df1=spark.read.csv("users.csv",header=True)
df2=spark.read.csv("transactions.csv",header=True)


st=time.time()
print("time for broadcast join :")
broadcast_df=broadcast(df1)
result=df2.join(broadcast_df,df2.user_id == df1.id)
result.show()
ed=time.time()


print("time for broadcast taken:",ed-st)

print("time for normal join :")
result=df2.join(df1,df1.id==df2.user_id,"inner")
st=time.time()
result.show()
print("time for normal join taken:",st-ed)