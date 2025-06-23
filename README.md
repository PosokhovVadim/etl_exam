# etl_exam

## Задание 1: Yandex Data Transfer — YDB → Object Storage

### ✅ Этапы выполнения:

1. **Создана база данных YDB**
2. **Таблица `transactions_v2` подготовлена и заполнена в ручную**
3. **Создан трансфер в Object Storage**
4. **SQL запрос сохранен в [`каталоге`](./task_1)**

## Задание 2: PySpark + Apache Airflow (пакетная обработка)

### ✅ Этапы выполнения:

1. **Подготовлены данные формата CSV / Parquet**
2. **Развернут кластер Yandex Data Proc**
3. **Разработано [`PySpark-задание`](./task_2/scripts/spark_batch_job.py)**
4. **Разработан [`DAG-файл`](./task_2/batch_etl_dag.py)**
5. **Включено автоматическое создание и удаление кластера**

##  Задание 3: Apache Kafka + PySpark (стриминг)

### ✅ Этапы выполнения:

1. **Запущен кластер Kafka в Yandex Cloud**
2. **Создан [`PySpark-скрипт`](./task_3/kafka_stream_job.py) для чтения из Kafka топика**
3. **Топик заполняется данными (демо-продюсер)**
4. **Выходные данные сохраняются в Object Storage / YDB**


