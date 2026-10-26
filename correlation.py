from itertools import combinations
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation

spark = SparkSession.builder.appName("Correlation").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = (spark.read.parquet("data/clean_stocks.parquet")
           .filter(F.col("Date") >= F.lit("2011-05-02").cast("date"))
           .select("Stock", "Date", "ret"))

stocks = sorted([r["Stock"] for r in df.select("Stock").distinct().collect()])

# One row per date, one column per stock
wide = df.groupBy("Date").pivot("Stock", stocks).agg(F.first("ret")).dropna()
wide = wide.toDF(*[c.replace("-", "_") for c in wide.columns])
cols = [c for c in wide.columns if c != "Date"]
print("Trading days used:", wide.count(), "| stocks:", len(cols))

# Pack the stock columns into one vector column, then compute correlations
vec = VectorAssembler(inputCols=cols, outputCol="features").transform(wide).select("features")
corr = Correlation.corr(vec, "features", "pearson").head()[0].toArray()

cm = pd.DataFrame(corr, index=cols, columns=cols)
cm.round(3).to_csv("corr_matrix.csv")

pairs = sorted([(a, b, cm.loc[a, b]) for a, b in combinations(cols, 2)],
               key=lambda x: x[2], reverse=True)

print("\nMost correlated pairs:")
for a, b, c in pairs[:10]:
    print(f"  {a:12s} {b:12s} {c:.2f}")

print("\nLeast correlated pairs:")
for a, b, c in pairs[-10:]:
    print(f"  {a:12s} {b:12s} {c:.2f}")

avg = ((cm.sum() - 1) / (len(cols) - 1)).sort_values(ascending=False)
print("\nAverage correlation with the other stocks (highest 5, lowest 5):")
print(avg.head(5).round(2).to_string())
print(avg.tail(5).round(2).to_string())

spark.stop()
