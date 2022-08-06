from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import  StructField, StructType, StringType, FloatType
import pandas as pd


spark = SparkSession \
    .builder \
    .master("local[4]") \
    .appName("myApp") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

schemaRain = StructType([ StructField("STATION", StringType(), True), 
                      StructField("STATION_NAME", StringType(), True),
                      StructField("DATE", StringType(), True),
                      StructField("HPCP", StringType(), True)])


df = spark.readStream.schema(schemaRain).option("maxFilesPerTrigger",1).csv("./files/", header=True)
# check if the df is streaming data
print(df.isStreaming)

# First, stream the data to the display.. 
query = df.writeStream.format("console").outputMode("append").start()

