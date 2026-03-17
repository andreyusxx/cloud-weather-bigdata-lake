from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType

# 1. Схема для розшифровки JSON з Kafka
schema = StructType([
    StructField("name", StringType()),
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
    # Створюємо сесію. Тут Spark підтягне драйвер для роботи з Kafka
    spark = SparkSession.builder \
        .appName("WeatherStreamingSilver") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 2. ПІДКЛЮЧЕННЯ ДО ПОТОКУ (Reading from Kafka)
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "weather_raw") \
        .option("startingOffsets", "latest") \
        .load()

    # 3. ТРАНСФОРМАЦІЯ (Parsing JSON)
    # Перетворюємо бінарні дані в колонки за нашою схемою
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
            current_timestamp().alias("ingested_at")
        )

    # 5. ЗАПИС У КОНСОЛЬ (Для тестування)
    # Спочатку перевіримо, чи бачить Spark дані, просто виводячи їх на екран
    query = silver_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .trigger(processingTime='10 seconds') \
        .start()

    print("🚀 Spark Streaming запущено! Чекаю на повідомлення з Kafka...")
    query.awaitTermination()

if __name__ == "__main__":
    start_streaming()