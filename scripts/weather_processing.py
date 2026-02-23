from pyspark.sql import SparkSession
import os

spark = SparkSession.builder \
    .appName("WeatherProcessing") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .get_or_create()

def process_weather():
    input_path = "s3a://weather-data/raw/*.json"
    
    df = spark.read.json(input_path)
    df.printSchema()
    
    clean_df = df.select(
        "name", 
        "main.temp", 
        "main.humidity", 
        "weather.description"
    )
    
    clean_df.show()

if __name__ == "__main__":
    process_weather()