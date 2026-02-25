from pyspark.sql import SparkSession
from pyspark.sql.functions import col, element_at, year, month, dayofmonth
import os
import boto3
import logging

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += os.pathsep + "C:\\hadoop\\bin"
os.makedirs('logs', exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(filename)s] %(message)s',
    handlers=[
        logging.FileHandler("logs/pipeline.log", mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)
s3_client = boto3.client('s3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='admin',
    aws_secret_access_key='password',
    region_name='us-east-1'
)

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

    logger.info("📥 Читання сирих даних...")
    input_path = "s3a://weather-data/raw/*.json"
    df = spark.read.option("multiLine", "true").json(input_path)

    logger.info("⚙️ Трансформація даних...")
    
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
    logger.info(f"💾 Збереження у форматі Parquet...")

    output_path = "s3a://weather-data/silver/weather_history"
    clean_df.write.mode("append").partitionBy("year", "month", "day","city").parquet(output_path)

    logger.info(f"✅ Готово! Дані збережено в: {output_path}")
    spark.stop()

def archive_raw_files():
    bucket_name = 'weather-data'
    prefix = 'raw/'
    archive_prefix = 'archive/'

    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    
    if 'Contents' in response:
        for obj in response['Contents']:
            file_key = obj['Key']
            if file_key == prefix: continue 

            new_key = file_key.replace(prefix, archive_prefix, 1)

            s3_client.copy_object(
                Bucket=bucket_name,
                CopySource={'Bucket': bucket_name, 'Key': file_key},
                Key=new_key
            )
            s3_client.delete_object(Bucket=bucket_name, Key=file_key)
            logger.info(f"📦 Файл {file_key} переміщено в архів.")
    else:
        logger.info("📭 Папка raw порожня, нічого архівувати.")
if __name__ == "__main__":
    process_weather()
    archive_raw_files()