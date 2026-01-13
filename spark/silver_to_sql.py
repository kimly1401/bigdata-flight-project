from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, when

# =========================
# SPARK SESSION
# =========================
spark = SparkSession.builder \
    .appName("SilverToSQLServer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =========================
# READ SILVER
# =========================
silver_path = "/home/kimly/bigdata-flight-project/data_lake/silver_full"
silver_df = spark.read.parquet(silver_path)

print("SILVER COUNT:", silver_df.count())

# =========================
# JDBC CONFIG
# =========================
jdbc_url = (
    "jdbc:sqlserver://localhost:14199;"
    "databaseName=flight_dw_full;"
    "encrypt=false;"
    "trustServerCertificate=true"
)

jdbc_properties = {
    "user": "sa",
    "password": "Kimly1401@",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# =========================
# GOLD LOGIC – FIX DATA QUALITY
# =========================
fact_df = (
    silver_df
    # bỏ record không có ngày bay
    .filter(col("flight_date").isNotNull())

    # chuẩn hoá airport code (CHAR 3)
    .withColumn(
        "origin_clean",
        when(col("origin").isNull(), "UNK")
        .otherwise(substring(col("origin"), 1, 3))
    )
    .withColumn(
        "dest_clean",
        when(col("dest").isNull(), "UNK")
        .otherwise(substring(col("dest"), 1, 3))
    )

    # chuẩn hoá airline (VARCHAR 10)
    .withColumn(
        "airline_clean",
        when(col("airline").isNull(), "UNKNOWN")
        .otherwise(substring(col("airline"), 1, 10))
    )

    .select(
        col("flight_date"),
        col("origin_clean").alias("origin"),
        col("dest_clean").alias("dest"),
        col("airline_clean").alias("airline"),
        col("distance"),
        col("dep_delay"),
        col("arr_delay"),
        col("cancelled_flag"),
        col("distance_bucket"),
        col("delay_bucket")
    )
)

print("FACT COUNT AFTER CLEAN:", fact_df.count())
fact_df.show(5, truncate=False)

# =========================
# WRITE TO SQL SERVER
# =========================
print("=== JDBC DEBUG ===")
print("JDBC URL:", jdbc_url)
print("DB USER:", jdbc_properties["user"])

fact_df.write \
    .mode("append") \
    .option("batchsize", 5000) \
    .jdbc(
        url=jdbc_url,
        table="dbo.fact_flights",
        properties=jdbc_properties
    )

print("WRITE TO SQL DONE")
spark.stop()
