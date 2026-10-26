from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Splits").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("data/stocks/*.csv", header=True, inferSchema=True)
df = df.withColumn("Stock", F.regexp_extract(F.input_file_name(), r"([^/]+)\.csv$", 1))
df = df.select("Stock", "Date", "Open", "High", "Low", "Close", "Volume")

# Compare each row with the previous row of the same stock
w = Window.partitionBy("Stock").orderBy("Date")
df = (df.withColumn("prev_close", F.lag("Close").over(w))
        .withColumn("prev_date", F.lag("Date").over(w))
        .withColumn("gap_days", F.datediff("Date", "prev_date"))
        .withColumn("ratio", F.try_divide(F.col("Close"), F.col("prev_close"))))

print("A) Data gaps longer than 10 days:")
(df.filter(F.col("gap_days") > 10)
   .select("Stock", "prev_date", "Date", "gap_days", "prev_close", "Close")
   .orderBy(F.desc("gap_days"))
   .show(30))

# Candidate split days: big drop between two consecutive trading days
cand = (df.filter((F.col("gap_days") <= 10) & (F.col("ratio") < 0.70))
          .withColumn("implied", 1 / F.col("ratio")))

STANDARD = [1.5, 2, 2.5, 3, 4, 5, 10, 20]
best = F.least(*[
    F.struct(F.abs(F.log(F.col("implied") / s)).alias("err"), F.lit(s).alias("std"))
    for s in STANDARD
])

cand = (cand.withColumn("best", best)
            .withColumn("std_ratio", F.col("best.std"))
            .withColumn("err_pct", F.round((F.col("implied") / F.col("best.std") - 1) * 100, 1))
            .withColumn("status", F.when(F.col("best.err") <= 0.08, "OK").otherwise("REVIEW")))

print("B) Candidate events by status:")
cand.groupBy("status").count().show()

print("C) All candidate events:")
(cand.select("Stock", "Date", "prev_close", "Close",
             F.round("implied", 2).alias("implied"), "std_ratio", "err_pct", "status")
     .orderBy("Stock", "Date")
     .show(120, truncate=False))

spark.stop()
