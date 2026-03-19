from kafka import KafkaConsumer
import json
from datetime import datetime

def run_logger():
    print("👀 Логгер запущено. Чекаю на дані з Kafka...")
    
    # Підключаємося до того ж топіка, що і Spark
    consumer = KafkaConsumer(
        'weather_raw',
        bootstrap_servers=['localhost:29092'],
        auto_offset_reset='earliest',       
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    try:
        for message in consumer:
            data = message.value
            city = data.get('name', 'Невідоме місто')

            main_data = data.get('main', {})
            temp = main_data.get('temp', '??')
            raw_ts = data.get('dt', 'часу немає')

            if raw_ts:
                readable_time = datetime.fromtimestamp(raw_ts).strftime('%H:%M:%S')
            else:
                readable_time = "часу немає"

            print(f"📍 [ОБРОБЛЕНО]: Місто: {city} | Темп: {temp}°C | Час: {readable_time}")
    except KeyboardInterrupt:
        print("\n🛑 Логгер зупинено.")

if __name__ == "__main__":
    run_logger()