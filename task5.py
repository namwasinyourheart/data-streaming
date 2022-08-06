
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, FloatType, StructField, StructType, TimestampType, StringType
from pyspark.sql.functions import expr



spark = SparkSession \
    .builder \
    .master("local[4]") \
    .appName("myApp") \
    .getOrCreate()


schemaPrec = StructType([ StructField("STATION", StringType(), True), 
                      StructField("STATION_NAME", StringType(), True),
                      StructField("DATE", StringType(), True),
                      StructField("HPCP", FloatType(), True)])


df = spark.readStream.schema(schemaPrec).option("maxFilesPerTrigger",1).csv("./files/", header=True)

# check if the df is streaming data
print(df.isStreaming)


# rename columns for join



#  Stream the data to the display.. 
query = df.writeStream.format("console")\
    .outputMode("append").start().awaitTermination()
# Now, call a function to write each row of the stream to the database.

