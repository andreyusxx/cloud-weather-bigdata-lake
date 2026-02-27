from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'andrii',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 25), 
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'weather_etl_pipeline',
    default_args=default_args,
    description='Збір даних OpenWeather та обробка через Spark',
    schedule_interval='*/20 * * * *', 
    catchup=False,              
    tags=['weather', 'spark']
) as dag:


    task_fetch_data = BashOperator(
        task_id='fetch_weather_api',
        bash_command='python /opt/airflow/scripts/weather_ingestion.py'
    )

    task_process_spark = BashOperator(
        task_id='process_with_spark',
        bash_command=(
        'docker exec spark-master /opt/spark/bin/spark-submit '
        '--master spark://spark-master:7077 '
        '--conf "spark.jars.ivy=/tmp/.ivy2" '
        '--packages org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901 '
        '/opt/spark/scripts/weather_processing.py'
        )
    )
    task_fetch_data >> task_process_spark