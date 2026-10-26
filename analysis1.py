from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Analysis1").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("data/clean_stocks.parquet")

# Common 10-year window so every stock is compared fairly
d = df.filter(F.col("Date") >= F.lit("2011-05-02").cast("date"))

summary = (d.groupBy("Stock").agg(
                F.min_by("AdjClose", "Date").alias("start_price"),
                F.max_by("AdjClose", "Date").alias("end_price"),
                F.count("*").alias("days"),
                (F.stddev("ret") * F.sqrt(F.lit(252.0))).alias("ann_vol"))
             .withColumn("growth_pct", (F.col("end_price") / F.col("start_price") - 1) * 100)
             .withColumn("cagr_pct", (F.pow(F.col("end_price") / F.col("start_price"), 1 / 10.0) - 1) * 100))

view = summary.select("Stock", "days",
                      F.round("start_price", 1).alias("start"),
                      F.round("end_price", 1).alias("end"),
                      F.round("cagr_pct", 1).alias("cagr_pct"),
                      F.round(F.col("ann_vol") * 100, 1).alias("vol_pct"))

print("Top 10 by 10-year CAGR:")
view.orderBy(F.desc("cagr_pct")).show(10)

print("Bottom 10 by 10-year CAGR:")
view.orderBy("cagr_pct").show(10)

print("Top 10 most volatile:")
view.orderBy(F.desc("vol_pct")).show(10)

print("Stocks in the analysis:", summary.count())

summary.toPandas().to_csv("results_10y.csv", index=False)
spark.stop()
