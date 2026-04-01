FROM python:3.9-slim
WORKDIR /app
COPY . .
RUN pip install confluent-kafka requests python-dotenv
CMD ["python", "weather_producer.py"]