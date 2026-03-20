from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 3, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'weather_analytics_and_viz',
    default_args=default_args,
    schedule_interval='*/30 * * * *', 
    catchup=False,
    tags=['weather', 'gold', 'viz']
) as dag:

    task_gold_layer = BashOperator(
        task_id='generate_gold_layer',
        bash_command=(
            'docker exec -u 0 spark-master /opt/spark/bin/spark-submit '
            '--packages org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.5.0 '
            '/opt/spark/scripts/weather_gold_analytics.py'
        )
    )

    task_generate_viz = BashOperator(
        task_id='generate_weather_report',
        bash_command='python /opt/airflow/scripts/weather_visualizer.py'
    )

    task_gold_layer >> task_generate_viz