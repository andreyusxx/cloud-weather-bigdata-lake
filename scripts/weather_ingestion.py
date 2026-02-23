
import requests
import json
import os
from datetime import datetime
from dotenv import load_dotenv
import boto3
from botocore.client import Config

load_dotenv()

API_KEY = os.getenv("OPENWEATHER_API_KEY")
CITY = "Kyiv"
URL = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"


s3 = boto3.resource('s3',
                    endpoint_url='http://localhost:9000',
                    aws_access_key_id='admin',
                    aws_secret_access_key='password',
                    config=Config(signature_version='s3v4'),
                    region_name='us-east-1')

def fetch_weather():

    response = requests.get(URL)
    response.raise_for_status() 
    data = response.json()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"weather_{timestamp}.json"

    bucket_name = 'weather-data'
    s3.Object(bucket_name, f"raw/{filename}").put(Body=json.dumps(data, indent=4))
    

if __name__ == "__main__":
    fetch_weather()