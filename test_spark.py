from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
df = spark.createDataFrame([(1, "TCS"), (2, "INFY")], ["id", "stock"])
df.show()
spark.stop()
