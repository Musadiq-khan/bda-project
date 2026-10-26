from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Explore2").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("data/stocks/*.csv", header=True, inferSchema=True)
df = df.withColumnRenamed("Prev Close", "PrevClose")
df = df.withColumn("Stock", F.regexp_extract(F.input_file_name(), r"([^/]+)\.csv$", 1))

print("0) Rows where PrevClose is 0 or Close is 0:")
(df.filter((F.col("PrevClose") <= 0) | (F.col("Close") <= 0))
   .select("Stock", "Date", "PrevClose", "Open", "Close")
   .show(20))

# try_divide returns NULL instead of crashing when the divisor is 0
df = df.withColumn("ret", F.try_divide(F.col("Close"), F.col("PrevClose")) - 1)

print("A) Extreme one-day moves (likely splits or bonus issues):")
(df.filter((F.col("ret") < -0.35) | (F.col("ret") > 0.5))
   .select("Stock", "Date", "PrevClose", "Close", "ret")
   .orderBy("Stock", "Date")
   .show(100, truncate=False))

print("B) Around the 2001-07-02 oddity:")
(df.filter(F.col("Stock").isin("KOTAKBANK", "INDUSINDBK", "EICHERMOT"))
   .filter(F.col("Date").between("2001-06-26", "2001-07-05"))
   .select("Stock", "Date", "PrevClose", "Open", "High", "Low", "Close")
   .orderBy("Stock", "Date")
   .show(40))

spark.stop()
