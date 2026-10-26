from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

spark = SparkSession.builder.appName("ML").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = (spark.read.parquet("data/clean_stocks.parquet")
           .select("Stock", "Date", "AdjClose", "AdjVolume", "ret"))

w = Window.partitionBy("Stock").orderBy("Date")
w20 = w.rowsBetween(-19, 0)
w50 = w.rowsBetween(-49, 0)

# ---- Features: only information known on the day itself ----
df = (df.withColumn("sma20", F.avg("AdjClose").over(w20))
        .withColumn("sma50", F.avg("AdjClose").over(w50))
        .withColumn("avgvol20", F.avg("AdjVolume").over(w20))
        .withColumn("rn", F.row_number().over(w))
        .withColumn("f_ret1", F.col("ret"))
        .withColumn("f_ret5", F.col("AdjClose") / F.lag("AdjClose", 5).over(w) - 1)
        .withColumn("f_ret20", F.col("AdjClose") / F.lag("AdjClose", 20).over(w) - 1)
        .withColumn("f_px_vs_sma20", F.col("AdjClose") / F.col("sma20") - 1)
        .withColumn("f_sma20_vs_sma50", F.col("sma20") / F.col("sma50") - 1)
        .withColumn("f_vol20", F.stddev("ret").over(w20))
        .withColumn("f_volume_ratio", F.try_divide(F.col("AdjVolume"), F.col("avgvol20")))
        # ---- Label: what happens in the NEXT 20 trading days ----
        .withColumn("fwd20", F.lead("AdjClose", 20).over(w) / F.col("AdjClose") - 1)
        .withColumn("label", (F.col("fwd20") > 0).cast("double")))

features = ["f_ret1", "f_ret5", "f_ret20", "f_px_vs_sma20",
            "f_sma20_vs_sma50", "f_vol20", "f_volume_ratio"]

data = df.filter(F.col("rn") >= 51).dropna(subset=features + ["label"])

# ---- Time-based split with a one-month gap ----
train = data.filter(F.col("Date") < F.lit("2017-01-01").cast("date"))
test = data.filter(F.col("Date") >= F.lit("2017-02-01").cast("date"))

train, test = train.cache(), test.cache()
n_train, n_test = train.count(), test.count()
pos = test.filter("label = 1").count() / n_test
print(f"Train rows: {n_train} | Test rows: {n_test}")
print(f"Test set: {pos*100:.1f}% of rows are 'up' -> majority-class baseline accuracy = {max(pos, 1-pos)*100:.1f}%")

assembler = VectorAssembler(inputCols=features, outputCol="features", handleInvalid="skip")
train_v = assembler.transform(train)
test_v = assembler.transform(test)

auc_eval = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
acc_eval = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")

models = {
    "Logistic Regression": LogisticRegression(labelCol="label", featuresCol="features", maxIter=50),
    "Random Forest": RandomForestClassifier(labelCol="label", featuresCol="features",
                                            numTrees=100, maxDepth=6, seed=42),
}

for name, algo in models.items():
    model = algo.fit(train_v)
    pred = model.transform(test_v)
    print(f"\n{name}: AUC = {auc_eval.evaluate(pred):.3f} | accuracy = {acc_eval.evaluate(pred)*100:.1f}%")
    if name == "Random Forest":
        imp = sorted(zip(features, model.featureImportances.toArray()), key=lambda x: -x[1])
        print("Feature importance:")
        for f, v in imp:
            print(f"  {f:18s} {v:.3f}")

spark.stop()
