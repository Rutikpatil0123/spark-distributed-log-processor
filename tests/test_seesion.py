from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("LocalLogProcessor").master("local[*]") \
.config("spark.driver.bindAddress", "127.0.0.1") \
.config("spark.driver.host", "127.0.0.1").getOrCreate()

print("Spark version : ", spark.version)
spark.stop()