# 🌦️ Weather Data Engineering Pipeline (Medallion Architecture)

Цей проект демонструє побудову повноцінного **Data Lake** конвеєра для збору, обробки та візуалізації погодних даних у реальному часі. Проект базується на **Medallion Architecture** (Bronze, Silver, Gold) з використанням сучасного стеку технологій Big Data.

---

## 🚀 Основні особливості (Key Features)

* **Medallion Architecture:** Дані проходять три стадії очищення та агрегації (**Bronze** -> **Silver** -> **Gold**).
* **Data Quality Gates:** Реалізовано багаторівневу валідацію на наявність `NULL` значень та фізичних аномалій.
* **Automated Quarantine:** Впроваджено механізм ізоляції пошкоджених даних у зону **Quarantine** за допомогою **Boto3**.
* **Batch Atomicity:** Реалізовано принцип "все або нічого" — якщо хоча б один файл у батчі пошкоджений, весь батч ізолюється для запобігання забрудненню сховища.
* **Scalability:** Використання **Apache Spark** для обробки даних у форматі **Parquet** з партиціюванням.

---

## 🛠 Технологічний стек

* **Мова:** Python 3.10+
* **Обробка даних:** Apache Spark (PySpark)
* **Оркестрація:** Apache Airflow
* **Сховище:** MinIO (S3-compatible)
* **Візуалізація:** Streamlit
* **Хмарний SDK:** Boto3 (AWS SDK for Python)
* **Контейнеризація:** Docker & Docker Compose

---

## 🏗 Архітектура конвеєра

1.  **Ingestion (Bronze):** Збір JSON-даних з OpenWeather API до `s3://weather-data/raw/`.
2.  **Processing (Silver):** Валідація схеми, типізація та конвертація в Parquet. При виявленні `NULL` або аномалій — автоматичне переміщення всього вхідного батчу в `quarantine/`.
3.  **Analytics (Gold):** Розрахунок щоденної статистики (середня, максимальна, мінімальна температура та вологість).
4.  **Serving:** Візуалізація результатів у Streamlit.



---

## 🛡 Data Quality & Validation

В проекті реалізовано динамічну перевірку якості. Якщо валідація провалюється, Spark зупиняє запис, а Boto3 ізолює файли:

```python
# Динамічний фільтр для перевірки всіх критичних колонок
columns_to_check = ["city", "temperature", "humidity", "sky_condition"]
null_df = clean_df.filter(" OR ".join([f"{c} IS NULL" for c in columns_to_check]))
