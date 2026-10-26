from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("StockAnalysis").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("data/stocks/*.csv", header=True, inferSchema=True)

df = (df.withColumnRenamed("Prev Close", "PrevClose")
        .withColumnRenamed("Deliverable Volume", "DeliverableVolume")
        .withColumnRenamed("%Deliverble", "PctDeliverable"))

df.printSchema()
print("Rows:", df.count())
print("Stocks:", df.select("Symbol").distinct().count())
df.select(F.min("Date"), F.max("Date")).show()
df.show(5)

spark.stop()
