import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator as PO
from airflow.utils.dates import days_ago
import os
from datetime import datetime, timedelta
from transform_script import transform

# Параметры по умолчанию для DAG
default_args = {
    "owner": "Kirilenkov Kirill",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Создание DAG
dag = DAG(
    "kirilenkov_kirill_dag",
    default_args=default_args,
    description="customer_assessment",
    schedule_interval="0 0 5 * *",
    start_date=days_ago(1),
    catchup=False,
)

def extract_data():
    """
    Извлечение данных из CSV файла
    :return: DataFrame с данными из файла profit_table.csv
    """
    df = pd.read_csv("/opt/airflow/dags/profit_table.csv")
    return df

def transform_product_data(**kwargs):
    """
    Трансформация данных
    :param kwargs: дополнительные параметры, переданные Airflow
    :return: DataFrame с флагами активности
    """
    # Извлечение данных из предыдущего шага
    df = kwargs["ti"].xcom_pull(task_ids="extract_data")
    # Использование текущей даты для трансформации
    date = datetime.now().strftime("%Y-%m-%d")
    transformed_df = transform(df, date)  # Используем оригинальную функцию transform
    return transformed_df

def load_transformed_data(**kwargs):
    """
    Загрузка трансформированных данных в CSV файл
    :param kwargs: дополнительные параметры, переданные Airflow
    """
    # Извлечение трансформированных данных из предыдущего шага
    transformed_df = kwargs["ti"].xcom_pull(task_ids="transform_data")
    output_path = "/opt/airflow/dags/flags_activity.csv"
    # Проверка наличия файла и запись данных
    if os.path.exists(output_path):
        transformed_df.to_csv(output_path, mode="a", header=False, index=False)
    else:
        transformed_df.to_csv(output_path, index=False)

# Создание задач для DAG
extract_task = PO(
    task_id="extract_data",
    python_callable=extract_data,
    dag=dag,
)

transform_task = PO(
    task_id="transform_data",
    python_callable=transform_product_data,
    provide_context=True,
    dag=dag,
)

load_task = PO(
    task_id="load_data",
    python_callable=load_transformed_data,
    provide_context=True,
    dag=dag,
)

# Определение порядка выполнения задач
extract_task >> transform_task >> load_task
