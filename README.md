# 🌦️ Real-Time Weather Data Platform (Event-Driven Medallion Architecture)

Цей проект — це масштабована **Data Lakehouse** платформа для стрімінгової обробки погодних даних. Я реалізував архітектуру, яка здатна обробляти потокові дані в реальному часі, забезпечуючи 100% цілісність даних та оптимізоване аналітичне сховище.

---

## 🏗️ Архітектурні рішення (Engineering Excellence)

* **Event-Driven Ingestion:** Використання **Apache Kafka** як центральної нервової системи. Продюсер на Python повністю декуперизований (ізольований) від споживачів, що гарантує стійкість системи до пікових навантажень (**Backpressure handling**).
* **Medallion Architecture (Bronze -> Silver -> Gold):** Чіткий поділ на шари для забезпечення відтворюваності даних та високої якості аналітики.
* **Scalable Storage (Hive-style Partitioning):** Gold-шар оптимізовано за допомогою **партиціювання за датою** (`report_date`). Це мінімізує обсяг сканування даних (**Data Skipping**) та пришвидшує аналітичні запити в десятки разів.
* **Multi-Consumer Pattern:** Реалізовано паралельне споживання одного топіка Kafka:
    1.  **Spark Streaming** — для надійної обробки та запису в Data Lake.
    2.  **Independent Python Logger** — для миттєвого моніторингу "сирих" подій у реальному часі.

---

## 🛡️ Data Quality & Reliability

Я впровадив стратегію **"Zero Tolerance to Corrupted Data"**:
* **Atomic Quarantine:** При виявленні `NULL` значень або аномалій у критичних полях, весь вхідний батч автоматично ізолюється в `quarantine/` зону за допомогою **Boto3**. Це запобігає забрудненню Silver-шару.
* **Schema Validation:** Жорстка типізація даних при переході з JSON до Parquet.
* **Deduplication:** Spark-задачі автоматично очищують дані від дублікатів перед фінальною агрегацією в Gold-шарі.

---

## 🛠 Технологічний стек

* **Streaming:** Apache Kafka (Zookeeper, Broker)
* **Processing:** Apache Spark (Structured Streaming & Batch API)
* **Orchestration:** Apache Airflow
* **Storage:** MinIO (S3-compatible Object Storage)
* **Format:** Apache Parquet (Columnar Storage)
* **Infrastructure:** Docker & Docker Compose

---

## 🚀 Як працює конвеєр

1.  **Ingestion:** Python Producer -> Kafka Topic (`weather_raw`).
2.  **Silver Layer:** Spark Streaming -> Schema Validation -> Quarantine Check -> **Parquet Storage**.
3.  **Gold Layer:** Airflow Orchestration -> Spark Batch Job -> **Partitioned Analytics**.
4.  **Serving:** Аналітичні звіти та візуалізація трендів у реальному часі.

---

## 📊 Приклад оптимізації Gold-шару

Замість повного перезапису всієї таблиці, реалізовано інкрементальне оновлення через партиції:

```python
# Оптимізований запис для накопичення історичних даних
gold_df.write \
    .mode("overwrite") \
    .partitionBy("report_date") \
    .parquet("s3a://weather-data/gold/daily_weather_stats")