from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Sectors").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("data/clean_stocks.parquet")

# Metadata: turn the symbol M&M into MM so it matches the file name
meta = (spark.read.csv("data/stock_metadata.csv", header=True, inferSchema=True)
        .withColumn("Stock", F.regexp_replace("Symbol", "&", ""))
        .select("Stock", F.col("Company Name").alias("Company"), "Industry"))

# ---------- Part 1: sector summary over the common 10-year window ----------
d = df.filter(F.col("Date") >= F.lit("2011-05-02").cast("date"))

stock = (d.groupBy("Stock").agg(
              F.min_by("AdjClose", "Date").alias("p0"),
              F.max_by("AdjClose", "Date").alias("p1"),
              (F.stddev("ret") * F.sqrt(F.lit(252.0)) * 100).alias("vol"))
           .withColumn("cagr", (F.pow(F.col("p1") / F.col("p0"), 0.1) - 1) * 100)
           .join(meta, "Stock"))

sector = (stock.groupBy("Industry").agg(
              F.count("*").alias("n"),
              F.round(F.avg("cagr"), 1).alias("avg_cagr"),
              F.round(F.avg("vol"), 1).alias("avg_vol"),
              F.max_by("Stock", "cagr").alias("best"),
              F.min_by("Stock", "cagr").alias("worst"))
          .orderBy(F.desc("avg_cagr")))

print("Sector summary (10-year CAGR % and annualized volatility %):")
sector.show(truncate=False)

# ---------- Part 2: average return of each sector, year by year ----------
yr = (df.withColumn("Year", F.year("Date"))
        .groupBy("Stock", "Year")
        .agg(F.max_by("AdjClose", "Date").alias("year_end")))

w = Window.partitionBy("Stock").orderBy("Year")
yr = yr.withColumn("yret", (F.col("year_end") / F.lag("year_end").over(w) - 1) * 100)

heat = (yr.filter(F.col("Year").between(2012, 2020))
          .join(meta, "Stock")
          .groupBy("Industry")
          .pivot("Year", list(range(2012, 2021)))
          .agg(F.round(F.avg("yret"), 1)))

print("Average yearly return by sector (%):")
heat.orderBy("Industry").show(truncate=False)

sector.toPandas().to_csv("sector_summary.csv", index=False)
heat.toPandas().to_csv("sector_yearly.csv", index=False)
spark.stop()

