import pandas as pd
import matplotlib.pyplot as plt
import s3fs
import sys
import datetime
import matplotlib
import matplotlib.pyplot as plt

matplotlib.use('Agg')
# Ми використовуємо s3fs, щоб pandas міг читати прямо з S3
storage_options = {
    "key": "admin",
    "secret": "password",
    "client_kwargs": {"endpoint_url": "http://minio:9000"}
}

def create_viz():
    print("🚀 Запуск візуалізації...")
    try:
        print("📥 Завантажую дані з Gold Layer...")
        df = pd.read_parquet(
            "s3://weather-data/gold/daily_weather_stats", 
            storage_options=storage_options
        )

        if df.empty:
            print("❌ Дані відсутні!")
            return

        plt.figure(figsize=(10, 6))
        plt.bar(df['city'], df['avg_temp'], color='skyblue')
        plt.title('Average Temperature by City')
        plt.ylabel('Temperature (°C)')
        
        REPORT_DIR = '/opt/airflow/reports'
        
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = f'{REPORT_DIR}/weather_report_{timestamp}.png'
        latest_path = f'{REPORT_DIR}/latest_weather_report.png'
        
        plt.savefig(report_path)
        plt.savefig(latest_path)
        print(f"✅ Файли збережено в папку reports!")
        print(f"✅ Звіт збережено у: {report_path}")
        
        # plt.show()

    except Exception as e:
        print(f"❌ ПОМИЛКА: {e}")

if __name__ == "__main__":
    create_viz()