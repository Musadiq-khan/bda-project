from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Splits2").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("data/stocks/*.csv", header=True, inferSchema=True)
df = df.withColumn("Stock", F.regexp_extract(F.input_file_name(), r"([^/]+)\.csv$", 1))
df = df.select("Stock", "Date", "Close")

w = Window.partitionBy("Stock").orderBy("Date")
df = (df.withColumn("prev_close", F.lag("Close").over(w))
        .withColumn("prev_date", F.lag("Date").over(w))
        .withColumn("gap_days", F.datediff("Date", "prev_date"))
        .withColumn("single", F.try_divide(F.col("prev_close"), F.col("Close")))
        .withColumn("avg_before", F.avg("Close").over(w.rowsBetween(-5, -1)))
        .withColumn("avg_after", F.avg("Close").over(w.rowsBetween(0, 4)))
        .withColumn("smooth", F.try_divide(F.col("avg_before"), F.col("avg_after"))))

STANDARD = [1.5, 2, 2.5, 3, 4, 5, 10, 20]

def nearest(col):
    return F.least(*[
        F.struct(F.abs(F.log(col / s)).alias("err"), F.lit(s).alias("std"))
        for s in STANDARD
    ])

ev = (df.filter((F.col("gap_days") <= 10) & (F.col("Close") < 0.70 * F.col("prev_close")))
        .withColumn("n1", nearest(F.col("single")))
        .withColumn("n2", nearest(F.col("smooth"))))

ev = (ev.withColumn("std_single", F.col("n1.std"))
        .withColumn("std_smooth", F.col("n2.std"))
        .withColumn("smooth_off_pct", F.round((F.col("smooth") / F.col("n2.std") - 1) * 100, 1)))

attention = ev.filter((F.col("std_single") != F.col("std_smooth")) |
                      (F.abs(F.col("smooth_off_pct")) > 6))

print(f"Events checked: {ev.count()} | needing attention: {attention.count()}")
(attention.select("Stock", "Date",
                  F.round("single", 2).alias("single_day"),
                  F.round("smooth", 2).alias("smoothed"),
                  "std_single", "std_smooth", "smooth_off_pct")
          .orderBy("Stock", "Date")
          .show(60, truncate=False))

spark.stop()
