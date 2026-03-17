import json
import os
import requests
import time
from confluent_kafka import Producer
from dotenv import load_dotenv

# Завантажуємо .env
load_dotenv() 

API_KEY = os.getenv("OPENWEATHER_API_KEY")
CITIES = ["Kyiv", "Lviv", "Odesa", "Kharkiv", "Dnipro"]
TOPIC = "weather_raw"

# Підключення до Kafka
conf = {'bootstrap.servers': "localhost:29092"}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Помилка доставки: {err}", flush=True)
    else:
        print(f"✅ Повідомлення доставлено в {msg.topic()} [{msg.partition()}]", flush=True)

def fetch_and_send():
    if not API_KEY:
        print("❌ ПОМИЛКА: OPENWEATHER_API_KEY не знайдено в .env файлі!", flush=True)
        return
    else:
        print(f"🔑 API Ключ знайдено (перші символи): {API_KEY[:5]}...", flush=True)

    while True:
        for city in CITIES:
            url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
            try:
                response = requests.get(url)
                data = response.json()

                if response.status_code != 200:
                    print(f"⚠️ API помилка {response.status_code} для {city}: {data.get('message')}", flush=True)
                    continue
                
                try:
                    producer.produce(
                        TOPIC, 
                        value=json.dumps(data).encode('utf-8'), 
                        callback=delivery_report
                    )
                except BufferError:
                    print("⏳ Черга переповнена, очікуємо...", flush=True)
                    producer.poll(1)
                    
                producer.poll(0)
                
            except Exception as e:
                print(f"💥 Помилка запиту: {e}", flush=True)
        
        print("⏳ Пакет відправлено. Чекаємо 10 секунд...", flush=True)
        producer.flush() 
        time.sleep(10)

if __name__ == "__main__":
    print(f"🚀 Producer запускається. Топік: '{TOPIC}'", flush=True)
    fetch_and_send()