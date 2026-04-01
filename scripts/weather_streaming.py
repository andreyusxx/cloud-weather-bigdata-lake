from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, year, month, dayofmonth, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType, LongType

# 1. Схема для розшифровки JSON з Kafka
schema = StructType([
    StructField("name", StringType()),
    StructField("dt", LongType()),  # Виправлено на LongType
    StructField("main", StructType([
        StructField("temp", DoubleType()),
        StructField("humidity", IntegerType())
    ])),
    StructField("weather", ArrayType(StructType([
        StructField("description", StringType())
    ]))),
    StructField("cod", IntegerType())
])

def start_streaming():
    # Створюємо сесію
    spark = SparkSession.builder \
        .appName("WeatherStreamingSilver") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 2. ПІДКЛЮЧЕННЯ ДО ПОТОКУ
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "weather_raw") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    # 3. ТРАНСФОРМАЦІЯ
    json_stream = raw_stream.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # 4. ОЧИЩЕННЯ ТА ВИБІР ПОЛІВ
    silver_df = json_stream.filter(col("cod") == 200) \
        .select(
            col("name").alias("city"),
            col("main.temp").alias("temperature"),
            col("main.humidity").alias("humidity"),
            col("weather")[0]["description"].alias("sky_condition"),
            from_unixtime(col("dt")).cast("timestamp").alias("actual_weather_time"),
            current_timestamp().alias("ingested_at")
        )
    
    final_df = silver_df.withColumn("year", year(col("ingested_at"))) \
                        .withColumn("month", month(col("ingested_at"))) \
                        .withColumn("day", dayofmonth(col("ingested_at")))
    
    # 5. ЗАПИС У MINIO (Silver Layer)
    query = final_df.writeStream \
        .format("parquet") \
        .option("path", "s3a://weather-data/silver/weather_history") \
        .option("checkpointLocation", "s3a://weather-data/checkpoints/weather_v3") \
        .partitionBy("year", "month", "day", "city") \
        .outputMode("append") \
        .trigger(processingTime='1 minute') \
        .start()

    print("📡 Стрімінг запущено! Дані записуються в MinIO (Silver Layer)...")
    query.awaitTermination()

if __name__ == "__main__":
    start_streaming()