from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, StructField, StructType, StringType




spark = SparkSession.builder\
    .master("local[1]") \
    .appName("myApp") \
    .getOrCreate()

schemaRain = StructType([
    StructField("STATION", StringType(), True),
    StructField("STATION_NAME", StringType(), True),
    StructField("DATE", StringType(), True),
    StructField("HPCP", FloatType(), True)  # HPCP: tong luong mua duoc tinh bang inch
])

df = spark.readStream.schema(schemaRain).option("maxfilesperTrigger", 1)\
    .csv("G:\My Drive\apache-spark-structured-streaiming-api--with-mongodb\files", header = True)
print(df.isStreaming)

df.writeStream.format("console").outputMode("append").start().awaitTermination()
