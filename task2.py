
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, FloatType, \
    StructField, StructType, TimestampType, StringType



spark = SparkSession \
    .builder \
    .master("local[1]") \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/prcp.hpcp") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/prcp.hpcp") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()


schemaRain = StructType([ StructField("STATION", StringType(), True), 
                      StructField("STATION_NAME", StringType(), True),
                      StructField("DATE", StringType(), True),
                      StructField("HPCP", FloatType(), True)])


df = spark.readStream.schema(schemaRain).option("maxfilesperTrigger", 1)\
    .csv("/content/drive/MyDrive/apache-spark-structured-streaiming-api--with-mongodb/files", header = True)
# check if the df is streaming data
print(df.isStreaming)

def write_row(batch_df , batch_id):
    batch_df.write.format("mongo").mode("append").save()
    pass
df.writeStream.foreachBatch(write_row).start().awaitTermination()