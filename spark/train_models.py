from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer,
    OneHotEncoder,
    VectorAssembler,
    StandardScaler
)
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator
)

# =========================
# SPARK SESSION
# =========================
spark = SparkSession.builder \
    .appName("FlightCancellation_LogisticRegression") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =========================
# READ SILVER DATA
# =========================
SILVER_PATH = "/home/kimly/bigdata-flight-project/data_lake/silver_full"
df = spark.read.parquet(SILVER_PATH)

print("TOTAL ROWS:", df.count())

# =========================
# LABEL PREP
# =========================
df = df.filter(col("cancelled_flag").isNotNull())

df = df.withColumn(
    "label",
    when(col("cancelled_flag") == 1, 1).otherwise(0)
)

# =========================
# FILL NULL NUMERIC (CRITICAL FIX)
# =========================
numeric_cols = ["distance", "dep_delay", "arr_delay", "day_of_week", "month"]
df = df.fillna(0, subset=numeric_cols)

# =========================
# TRAIN / TEST SPLIT
# =========================
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

print("TRAIN COUNT:", train_df.count())
print("TEST COUNT :", test_df.count())

# =========================
# HANDLE CLASS IMBALANCE
# =========================
label_counts = train_df.groupBy("label").count().collect()
count_0 = next(r["count"] for r in label_counts if r["label"] == 0)
count_1 = next(r["count"] for r in label_counts if r["label"] == 1)

ratio = count_0 / count_1

train_df = train_df.withColumn(
    "class_weight",
    when(col("label") == 1, ratio).otherwise(1.0)
)

# =========================
# FEATURES
# =========================
categorical_cols = ["origin", "dest", "airline"]

# =========================
# STRING INDEXERS
# =========================
indexers = [
    StringIndexer(
        inputCol=c,
        outputCol=f"{c}_idx",
        handleInvalid="keep"
    )
    for c in categorical_cols
]

# =========================
# ONE HOT ENCODER
# =========================
encoders = [
    OneHotEncoder(
        inputCol=f"{c}_idx",
        outputCol=f"{c}_ohe"
    )
    for c in categorical_cols
]

# =========================
# VECTOR ASSEMBLER
# =========================
assembler = VectorAssembler(
    inputCols=[f"{c}_ohe" for c in categorical_cols] + numeric_cols,
    outputCol="raw_features"
)

# =========================
# SCALER
# =========================
scaler = StandardScaler(
    inputCol="raw_features",
    outputCol="features",
    withStd=True,
    withMean=False
)

# =========================
# LOGISTIC REGRESSION
# =========================
lr = LogisticRegression(
    featuresCol="features",
    labelCol="label",
    weightCol="class_weight",
    maxIter=20,
    regParam=0.01
)

# =========================
# PIPELINE
# =========================
pipeline = Pipeline(
    stages=indexers + encoders + [assembler, scaler, lr]
)

# =========================
# TRAIN MODEL
# =========================
print("\n===== TRAINING Logistic Regression =====")
model = pipeline.fit(train_df)

preds = model.transform(test_df)

# =========================
# EVALUATION
# =========================
auc_eval = BinaryClassificationEvaluator(
    labelCol="label",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
)

f1_eval = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="f1"
)

precision_eval = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="weightedPrecision"
)

recall_eval = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="weightedRecall"
)

print("Logistic Regression AUC       :", auc_eval.evaluate(preds))
print("Logistic Regression F1        :", f1_eval.evaluate(preds))
print("Logistic Regression Precision :", precision_eval.evaluate(preds))
print("Logistic Regression Recall    :", recall_eval.evaluate(preds))

spark.stop()
