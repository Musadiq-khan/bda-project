from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("CheckDrops").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("data/clean_stocks.parquet")
df.printSchema()

drops = df.filter(F.col("ret") < -0.15)
drops = drops.withColumn("n_same_day", F.count("*").over(Window.partitionBy("Date")))

lone = (drops.filter(F.col("n_same_day") == 1)
             .withColumn("implied_ratio", F.round(1 / (1 + F.col("ret")), 2)))

print("Isolated drops (only one stock fell >15% that day):")
(lone.select("Stock", "Date", "Close", F.round("ret", 3).alias("ret"), "implied_ratio")
     .orderBy("Date")
     .show(60))

spark.stop()
