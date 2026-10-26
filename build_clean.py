from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("BuildClean").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("data/stocks/*.csv", header=True, inferSchema=True)
df = df.withColumn("Stock", F.regexp_extract(F.input_file_name(), r"([^/]+)\.csv$", 1))
df = df.select("Stock", "Date", "Open", "High", "Low", "Close", "VWAP", "Volume", "Turnover")

# 1) Drop the unreliable early rows of the 4 stocks with the 468-day hole
holey = ["GAIL", "INDUSINDBK", "KOTAKBANK", "SHREECEM"]
df = df.filter(~(F.col("Stock").isin(holey) & (F.col("Date") < F.lit("2001-07-02").cast("date"))))

# Confirmed corporate actions from exchange records: (stock, ex-date, true factor)
manual = spark.createDataFrame(
    [("GAIL", "2017-03-09", 4/3),
     ("GAIL", "2018-03-27", 4/3),
     ("WIPRO", "2019-03-06", 4/3)],
    ["m_stock", "m_date", "m_factor"]
).withColumn("m_date", F.to_date("m_date"))

df = df.join(manual, (df["Stock"] == manual["m_stock"]) & (df["Date"] == manual["m_date"]), "left")

# 2) Detect events and compute each event's adjustment factor
w = Window.partitionBy("Stock").orderBy("Date")
df = (df.withColumn("prev_close", F.lag("Close").over(w))
        .withColumn("gap_days", F.datediff("Date", F.lag("Date").over(w)))
        .withColumn("auto_event", (F.col("gap_days") <= 10) & (F.col("Close") < 0.70 * F.col("prev_close")))
        .withColumn("is_event", F.col("auto_event") | F.col("m_factor").isNotNull())
        .withColumn("event_factor",
                    F.when(F.col("m_factor").isNotNull(), F.col("m_factor"))
                     .when(F.col("auto_event"), F.try_divide(F.col("prev_close"), F.col("Close")))
                     .otherwise(F.lit(1.0))))

# 3) Cumulative factor: the product of all event factors AFTER each row
df = df.withColumn("log_after", F.sum(F.log("event_factor")).over(w.rowsBetween(1, Window.unboundedFollowing)))
df = df.withColumn("adj_factor", F.exp(F.coalesce("log_after", F.lit(0.0))))

# 4) Adjusted prices and volume
for c in ["Open", "High", "Low", "Close", "VWAP"]:
    df = df.withColumn("Adj" + c, F.col(c) / F.col("adj_factor"))
df = df.withColumn("AdjVolume", F.col("Volume") * F.col("adj_factor"))

# 5) Daily return from adjusted close
df = df.withColumn("ret", F.try_divide(F.col("AdjClose"), F.lag("AdjClose").over(w)) - 1)

clean = df.select("Stock", "Date", "Open", "High", "Low", "Close", "Volume",
                  "AdjOpen", "AdjHigh", "AdjLow", "AdjClose", "AdjVWAP", "AdjVolume",
                  "Turnover", "is_event", "adj_factor", "ret")

# 6) Save as Parquet, one folder per stock
clean.write.mode("overwrite").partitionBy("Stock").parquet("data/clean_stocks.parquet")

# 7) Sanity checks
print("Events:", clean.filter("is_event").count())
print("Rows:", clean.count())

print("Infosys: raw vs adjusted close, first and last day")
(clean.filter(F.col("Stock") == "INFY").orderBy("Date")
      .select("Date", "Close", "AdjClose", "adj_factor")
      .limit(1).show())
(clean.filter(F.col("Stock") == "INFY").orderBy(F.desc("Date"))
      .select("Date", "Close", "AdjClose", "adj_factor")
      .limit(1).show())

print("Largest remaining daily moves (should be real market moves, not splits):")
(clean.filter(F.col("ret").isNotNull())
      .orderBy(F.desc(F.abs("ret")))
      .select("Stock", "Date", "Close", F.round("ret", 3).alias("ret"))
      .show(12))

spark.stop()
