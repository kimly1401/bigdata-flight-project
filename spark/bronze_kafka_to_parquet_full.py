from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaBronzeFull") \
    .getOrCreate()

raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "flight_topic") \
    .option("startingOffsets", "earliest") \
    .load()

bronze = raw.selectExpr("CAST(value AS STRING) as json")

query = bronze.writeStream \
    .format("parquet") \
    .option("path", "data_lake/bronze_full") \
    .option("checkpointLocation", "checkpoint/bronze_full") \
    .outputMode("append") \
    .start()

query.awaitTermination()
