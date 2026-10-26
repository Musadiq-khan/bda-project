from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("MovingAvg").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("data/clean_stocks.parquet").select("Stock", "Date", "AdjClose")

w = Window.partitionBy("Stock").orderBy("Date")

df = (df.withColumn("sma20", F.avg("AdjClose").over(w.rowsBetween(-19, 0)))
        .withColumn("sma50", F.avg("AdjClose").over(w.rowsBetween(-49, 0)))
        .withColumn("rn", F.row_number().over(w))
        .withColumn("fwd20", F.lead("AdjClose", 20).over(w) / F.col("AdjClose") - 1))

# Keep only rows where a full 50-day window exists
sig = df.filter(F.col("rn") >= 50)

sig = (sig.withColumn("trend", F.when(F.col("sma20") > F.col("sma50"), 1).otherwise(0))
          .withColumn("prev_trend", F.lag("trend").over(w))
          .withColumn("cross",
                      F.when((F.col("trend") == 1) & (F.col("prev_trend") == 0), "golden")
                       .when((F.col("trend") == 0) & (F.col("prev_trend") == 1), "death")))

print("TCS, last 5 days:")
(sig.filter(F.col("Stock") == "TCS").orderBy(F.desc("Date"))
    .select("Date", F.round("AdjClose", 1).alias("close"),
            F.round("sma20", 1).alias("sma20"), F.round("sma50", 1).alias("sma50"), "trend")
    .show(5))

print("Number of crossovers:")
sig.filter(F.col("cross").isNotNull()).groupBy("cross").count().show()

print("Average next-20-day return after each signal:")
(sig.filter(F.col("cross").isNotNull() & F.col("fwd20").isNotNull())
    .groupBy("cross")
    .agg(F.count("*").alias("n"),
         F.round(F.avg("fwd20") * 100, 2).alias("avg_fwd20_pct"),
         F.round(F.avg((F.col("fwd20") > 0).cast("int")) * 100, 1).alias("pct_positive"))
    .show())

print("Baseline: average next-20-day return on ALL days:")
(sig.filter(F.col("fwd20").isNotNull())
    .agg(F.round(F.avg("fwd20") * 100, 2).alias("avg_fwd20_pct"),
         F.round(F.avg((F.col("fwd20") > 0).cast("int")) * 100, 1).alias("pct_positive"))
    .show())

spark.stop()
