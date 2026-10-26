from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Explore").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("data/stocks/*.csv", header=True, inferSchema=True)
df = (df.withColumnRenamed("Prev Close", "PrevClose")
        .withColumnRenamed("Deliverable Volume", "DeliverableVolume")
        .withColumnRenamed("%Deliverble", "PctDeliverable"))

# Tag every row with the file it came from, so one file = one stock
df = df.withColumn("Stock", F.regexp_extract(F.input_file_name(), r"([^/]+)\.csv$", 1))

print("1) Files containing more than one ticker symbol:")
(df.groupBy("Stock")
   .agg(F.countDistinct("Symbol").alias("n_symbols"))
   .filter("n_symbols > 1")
   .show(truncate=False))

print("2) Rows and date range per stock:")
(df.groupBy("Stock")
   .agg(F.count("*").alias("rows"), F.min("Date").alias("first"), F.max("Date").alias("last"))
   .orderBy("rows")
   .show(60))

print("3) Null count per column:")
df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).show()

print("4) Series types:")
df.groupBy("Series").count().show()

print("5) Possible splits: yesterday's Close vs today's PrevClose")
w = Window.partitionBy("Stock").orderBy("Date")
chk = df.withColumn("gap", F.lag("Close").over(w) / F.col("PrevClose"))
(chk.filter((F.col("gap") > 1.3) | (F.col("gap") < 0.77))
    .select("Stock", "Date", "Close", "PrevClose", "gap")
    .orderBy(F.desc("gap"))
    .show(20))

spark.stop()
