from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min, count
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    return SparkSession.builder \
        .appName("WeatherGoldAnalytics") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

def generate_gold_layer():
    spark = create_spark_session()
    
    input_path = "s3a://weather-data/silver/weather_history"
    output_path = "s3a://weather-data/gold/daily_weather_stats"

    logger.info("📥 Читання даних із Silver шару...")
    silver_df = spark.read.parquet(input_path)

    logger.info("📊 Розрахунок щоденної статистики...")
    gold_df = silver_df.groupBy("city", "year", "month", "day").agg(
        avg("temperature").alias("avg_temp"),
        max("temperature").alias("max_temp"),
        min("temperature").alias("min_temp"),
        avg("humidity").alias("avg_humidity"),
        count("*").alias("measurements_count")
    ).orderBy("year", "month", "day", "city")

    gold_df.show()

    logger.info(f"💾 Збереження аналітики в Gold шар...")
    gold_df.write.mode("overwrite").parquet(output_path)

    logger.info("✅ Gold шар успішно оновлено!")
    spark.stop()

if __name__ == "__main__":
    generate_gold_layer()