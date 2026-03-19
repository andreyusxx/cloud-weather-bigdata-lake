from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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
    
    df_with_date = clean_df.withColumn(
        "report_date", 
        F.to_date(F.col("actual_weather_time"))
    )

    gold_df = df_with_date.groupBy("city", "report_date") \
        .agg(
            F.round(F.avg("temperature"), 2).alias("avg_temp"),
            F.max("temperature").alias("max_temp"),
            F.min("temperature").alias("min_temp"),
            F.max("humidity").alias("max_humidity"),
            F.count("*").alias("total_measurements"),
            F.first("sky_condition").alias("current_sky")
        ) \
        .orderBy(F.col("report_date").desc(), F.col("avg_temp").desc())

    gold_df.write \
        .mode("overwrite") \
        .partitionBy("report_date") \
        .parquet("s3a://weather-data/gold/daily_weather_stats")

    print("✅ Gold шар успішно оновлено з партиціюванням за датою.")
    spark.stop()

if __name__ == "__main__":
    create_gold_report()