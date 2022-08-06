from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, StructField, StructType, StringType




spark = SparkSession \
    .builder \
    .master("local[1]") \
    .appName("myApp") \
    .getOrCreate()


schemaRain = StructType([ StructField("STATION", StringType(), True), 
                      StructField("STATION_NAME", StringType(), True),
                      StructField("DATE", StringType(), True),
                      StructField("HPCP", FloatType(), True)])


df = spark.readStream.schema(schemaRain).option("maxFilesPerTrigger",1).csv("./files/", header=True)

df.printSchema()




 



