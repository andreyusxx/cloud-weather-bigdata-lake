import streamlit as st
import pandas as pd
import boto3
import pyarrow.parquet as pq
from io import BytesIO

# Налаштування сторінки
st.set_page_config(page_title="Weather Data Lake Dashboard", layout="wide")
st.title("🌤️ Weather Analytics Dashboard (Gold Layer)")

s3 = boto3.client('s3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='admin',
    aws_secret_access_key='password',
    region_name='us-east-1'
)

def load_gold_data():
    bucket = "weather-data"
    prefix = "gold/daily_weather_stats/"
    
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    
    all_dfs = []
    if 'Contents' in response:
        for obj in response['Contents']:
            if obj['Key'].endswith(".parquet") and "_SUCCESS" not in obj['Key']:
                file_obj = s3.get_object(Bucket=bucket, Key=obj['Key'])
                df = pd.read_parquet(BytesIO(file_obj['Body'].read()))
                all_dfs.append(df)
    
    return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()

try:
    data = load_gold_data()
    
    if not data.empty:
        data['full_date'] = pd.to_datetime(data[['year', 'month', 'day']])
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Cities", data['city'].nunique())
        with col2:
            st.metric("Avg Global Temp", f"{data['avg_temp'].mean():.2f} °C")
        with col3:
            st.metric("Total Measurements", data['measurements_count'].sum())

        st.subheader("Temperature Trends by City")
        chart_data = data.pivot(index='full_date', columns='city', values='avg_temp')
        st.line_chart(chart_data)

        st.subheader("Raw Gold Data")
        st.dataframe(data.sort_values(by='full_date', ascending=False), width='stretch')
    else:
        st.warning("⚠️ Дані в Gold шарі ще не знайдені. Запусти Airflow таск 'generate_gold_layer'!")

except Exception as e:
    st.error(f"❌ Помилка підключення до MinIO: {e}")