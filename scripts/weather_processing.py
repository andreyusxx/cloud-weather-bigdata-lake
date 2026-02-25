from pyspark.sql import SparkSession
from pyspark.sql.functions import col, element_at, year, month, dayofmonth
import os

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += os.pathsep + "C:\\hadoop\\bin"

def create_spark_session():
    return SparkSession.builder \
        .appName("WeatherProcessing") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

def apply_hadoop_fixes(spark):
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.connection.timeout", "60000")
    hadoop_conf.set("fs.s3a.connection.establish.timeout", "5000")
    hadoop_conf.set("fs.s3a.multipart.purge.age", "86400") 
    hadoop_conf.set("fs.s3a.threads.keepalivetime", "60")

def process_weather():

    spark = create_spark_session()
    apply_hadoop_fixes(spark)

    print("📥 Читання сирих даних...")
    input_path = "s3a://weather-data/raw/*.json"
    df = spark.read.option("multiLine", "true").json(input_path)

    print("⚙️ Трансформація даних...")
    
    clean_df = df.select(
        col("name").alias("city"),
        col("main.temp").alias("temperature"),
        col("main.humidity").alias("humidity"),
        element_at(col("weather.description"), 1).alias("sky_condition"),
        col("dt").cast("timestamp").alias("observation_time")
    ).withColumn("year", year(col("observation_time"))) \
    .withColumn("month", month(col("observation_time"))) \
    .withColumn("day", dayofmonth(col("observation_time")))
    
    clean_df.show()
    print(f"💾 Збереження у форматі Parquet...")

    output_path = "s3a://weather-data/silver/weather_history"
    clean_df.write.mode("append").partitionBy("year", "month", "day","city").parquet(output_path)

    print(f"✅ Готово! Дані збережено в: {output_path}")
    spark.stop()

if __name__ == "__main__":
    process_weather()