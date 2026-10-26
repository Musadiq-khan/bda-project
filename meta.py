from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Meta").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

meta = spark.read.csv("data/stock_metadata.csv", header=True, inferSchema=True)
meta.printSchema()
meta.show(60, truncate=False)

stocks = spark.read.parquet("data/clean_stocks.parquet").select("Stock").distinct()

print("Stocks we have that are NOT found in the metadata Symbol column:")
sym_col = [c for c in meta.columns if c.lower() == "symbol"]
if sym_col:
    stocks.join(meta, stocks["Stock"] == meta[sym_col[0]], "left_anti").show(60)
else:
    print("No column called Symbol; check the schema above")

spark.stop()
