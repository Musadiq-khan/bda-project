import time
from datetime import datetime
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------- MongoDB ----------
col = MongoClient("mongodb://localhost:27017")["stockdb"]["prices"]
pipe = [
    {"$match": {"Date": {"$gte": datetime(2011, 5, 2)}, "Close": {"$gt": 0}}},
    {"$group": {"_id": "$Stock", "avg_range": {"$avg": {"$multiply": [
        {"$divide": [{"$subtract": ["$High", "$Low"]}, "$Close"]}, 100]}}}},
    {"$sort": {"avg_range": -1}},
    {"$limit": 10},
]
mongo_times = []
for _ in range(5):
    t0 = time.perf_counter()
    mongo_rows = list(col.aggregate(pipe, allowDiskUse=True))
    mongo_times.append(time.perf_counter() - t0)
print(f"MongoDB: first run {mongo_times[0]:.2f} s | best of 5 {min(mongo_times):.2f} s")

# ---------- Spark ----------
t0 = time.perf_counter()
spark = SparkSession.builder.master("local[*]").appName("Timing").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
startup = time.perf_counter() - t0

def run_spark():
    t = time.perf_counter()
    rows = (spark.read.parquet("data/clean_stocks.parquet")
            .filter((F.col("Date") >= F.lit("2011-05-02").cast("date")) & (F.col("Close") > 0))
            .groupBy("Stock")
            .agg(F.avg((F.col("High") - F.col("Low")) / F.col("Close") * 100).alias("avg_range"))
            .orderBy(F.desc("avg_range")).limit(10).collect())
    return time.perf_counter() - t, rows

spark_times = []
for _ in range(5):
    elapsed, spark_rows = run_spark()
    spark_times.append(elapsed)
print(f"Spark:   session start-up {startup:.1f} s | first query {spark_times[0]:.2f} s | best of 5 {min(spark_times):.2f} s")

# ---------- Do both give the same answer? ----------
print("\nTop 3 MongoDB:", [(r["_id"], round(r["avg_range"], 3)) for r in mongo_rows[:3]])
print("Top 3 Spark:  ", [(r["Stock"], round(r["avg_range"], 3)) for r in spark_rows[:3]])
spark.stop()
