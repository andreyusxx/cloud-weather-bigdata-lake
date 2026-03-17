from pyspark.sql import SparkSession
from pyspark.sql.functions import col, element_at, year, month, dayofmonth
import os
import boto3
import logging

#os.environ["HADOOP_HOME"] = "C:\\hadoop"
#os.environ["PATH"] += os.pathsep + "C:\\hadoop\\bin"
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
    endpoint_url='http://minio:9000',
    aws_access_key_id='admin',
    aws_secret_access_key='password',
    region_name='us-east-1'
)

def create_spark_session():
    return SparkSession.builder \
        .appName("WeatherProcessing") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
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

    if df.count() == 0:
        logger.warning("📭 Немає даних для обробки.")
        spark.stop()
        return False
    
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
    
    columns_to_check = ["city", "temperature", "humidity", "sky_condition"]
    null_df = clean_df.filter(" OR ".join([f"{c} IS NULL" for c in columns_to_check]))
    null_count = null_df.count()

    anomaly_count = clean_df.filter((col("temperature") < -70) | (col("temperature") > 70)).count()

    if null_count > 0 or anomaly_count > 0:
        logger.error(f"❌ ВАЛІДАЦІЯ ПРОВАЛЕНА: Знайдено {null_count} NULL значень та {anomaly_count} аномалій!")
        spark.stop()
        raise ValueError("Data Quality check failed")

    output_path = "s3a://weather-data/silver/weather_history"
    clean_df.write.mode("append").partitionBy("year", "month", "day", "city").parquet(output_path)

    logger.info(f"✅ Дані успішно збережено в Silver.")
    spark.stop()
    return True

def move_raw_files(target_prefix='archive/'):
    bucket_name = 'weather-data'
    source_prefix = 'raw/'

    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=source_prefix)
    
    if 'Contents' in response:
        for obj in response['Contents']:
            file_key = obj['Key']
            if file_key == source_prefix: continue 
            new_key = file_key.replace(source_prefix, target_prefix, 1)

            s3_client.copy_object(
                Bucket=bucket_name,
                CopySource={'Bucket': bucket_name, 'Key': file_key},
                Key=new_key
            )
            s3_client.delete_object(Bucket=bucket_name, Key=file_key)
            logger.info(f"📦 Файл {file_key} переміщено в {target_prefix}")
    else:
        logger.info("📭 Папка raw порожня, нічого архівувати.")
if __name__ == "__main__":
    try:
        success = process_weather()
        if success:
            move_raw_files(target_prefix='archive/')
        else:
            logger.info("Процес завершено без нових даних.")
    except Exception as e:
        logger.error(f"💥 Критична помилка Spark: {e}")
        # Якщо сталася помилка (наприклад, битий JSON) — переносимо в карантин
        move_raw_files(target_prefix='quarantine/')