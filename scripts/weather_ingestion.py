import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("OPENWEATHER_API_KEY")
CITY = "Kyiv"
URL = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"

def fetch_weather():
    if not API_KEY:
        print("Помилка: Ключ не знайдено! Перевір файл .env")
        return
    
    response = requests.get(URL)
    if response.status_code == 200:
        data = response.json()
        print(f"Погода у місті {CITY}:")
        print(f"Температура: {data['main']['temp']}°C")
        print(f"Вологість: {data['main']['humidity']}%")
    else:
        print(f"Помилка: {response.status_code}")

if __name__ == "__main__":
    fetch_weather()