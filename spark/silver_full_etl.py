from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_date, year, month, dayofweek,
    when, current_timestamp
)
from pyspark.sql.types import *

# =====================================================
# SPARK SESSION
# =====================================================
spark = SparkSession.builder \
    .appName("Flight-Silver-Canonical") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =====================================================
# PATH
# =====================================================
BRONZE_PATH = "/home/kimly/bigdata-flight-project/data_lake/bronze_full"
SILVER_PATH = "/home/kimly/bigdata-flight-project/data_lake/silver_full"

# =====================================================
# JSON SCHEMA (MATCH BRONZE 100%)
# =====================================================
schema = StructType([
    StructField("FL_DATE", StringType()),
    StructField("AIRLINE", StringType()),
    StructField("ORIGIN", StringType()),
    StructField("DEST", StringType()),
    StructField("DISTANCE", DoubleType()),
    StructField("DEP_DELAY", DoubleType()),
    StructField("ARR_DELAY", DoubleType()),

    # 🔥 FIX: CANCELLED IS STRING IN JSON ("0.0" / "1.0")
    StructField("CANCELLED", StringType()),

    StructField("CANCELLATION_REASON", StringType())
])

# =====================================================
# READ BRONZE
# =====================================================
bronze = spark.read.parquet(BRONZE_PATH)

# =====================================================
# PARSE JSON
# =====================================================
parsed = (
    bronze
    .select(from_json(col("json"), schema).alias("d"))
    .select("d.*")
)

# =====================================================
# SILVER TRANSFORM (CANONICAL – NO FILTER)
# =====================================================
silver = (
    parsed
    .withColumn("flight_date", to_date(col("FL_DATE")))
    .withColumn("year", year(col("flight_date")))
    .withColumn("month", month(col("flight_date")))
    .withColumn("day_of_week", dayofweek(col("flight_date")))

    .withColumn("airline", col("AIRLINE"))
    .withColumn("origin", col("ORIGIN"))
    .withColumn("dest", col("DEST"))

    .withColumn("distance", col("DISTANCE"))
    .withColumn("dep_delay", col("DEP_DELAY"))
    .withColumn("arr_delay", col("ARR_DELAY"))

    # 🔥 FIX CORE: CANCELLED → cancelled_flag (0/1, NO NULL)
    .withColumn(
        "cancelled_flag",
        when(col("CANCELLED").cast("double") == 1.0, 1).otherwise(0)
    )

    .withColumn("cancellation_reason", col("CANCELLATION_REASON"))

    # Distance bucket
    .withColumn(
        "distance_bucket",
        when(col("DISTANCE") < 300, "SHORT")
        .when(col("DISTANCE") < 800, "MEDIUM")
        .otherwise("LONG")
    )

    # Delay bucket
    .withColumn(
        "delay_bucket",
        when(col("DEP_DELAY").isNull(), "UNKNOWN")
        .when(col("DEP_DELAY") <= 15, "ONTIME")
        .otherwise("DELAYED")
    )

    .withColumn("ingest_ts", current_timestamp())

    .select(
        "flight_date", "year", "month", "day_of_week",
        "airline", "origin", "dest",
        "distance", "dep_delay", "arr_delay",
        "cancelled_flag", "cancellation_reason",
        "distance_bucket", "delay_bucket",
        "ingest_ts"
    )
)

# =====================================================
# HARDEN (ABSOLUTELY NO NULL LABEL)
# =====================================================
silver = silver.fillna({"cancelled_flag": 0})

# =====================================================
# WRITE SILVER
# =====================================================
(
    silver.write
    .mode("overwrite")
    .parquet(SILVER_PATH)
)

print("===================================")
print(f"SILVER FULL RECORD COUNT: {silver.count()}")
print("===================================")

spark.stop()
