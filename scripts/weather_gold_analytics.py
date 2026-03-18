from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, count, round, first

def create_gold_report():
    spark = SparkSession.builder \
        .appName("WeatherGoldAnalytics") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    print("📖 Читаю Silver шар (всі історичні та стрімінгові дані)...")
    silver_df = spark.read.parquet("s3a://weather-data/silver/weather_history")

    print("🧹 Очищую дані від дублікатів...")
    clean_df = silver_df.dropDuplicates(["city", "temperature", "humidity"])

    print("📊 Розраховую Gold-метрики...")
    gold_df = clean_df.groupBy("city") \
        .agg(
            round(avg("temperature"), 2).alias("avg_temp"),
            max("temperature").alias("max_temp"),
            min("temperature").alias("min_temp"),
            max("humidity").alias("max_humidity"),
            count("*").alias("total_measurements"),
            first("sky_condition").alias("current_sky")
        ).orderBy(col("avg_temp").desc())

    print("\n🏆 ФІНАЛЬНИЙ ЗВІТ:")
    gold_df.show()

    print("💾 Зберігаю результат у Gold Layer...")
    gold_df.write \
        .mode("overwrite") \
        .parquet("s3a://weather-data/gold/daily_weather_stats")

    print("✅ Процес завершено! Gold-шар оновлено.")

if __name__ == "__main__":
    create_gold_report()