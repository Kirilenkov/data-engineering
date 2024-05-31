import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator as PO
from airflow.utils.dates import days_ago
import os
from datetime import datetime, timedelta
from transform_script import generate_activity_flags

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
    "kirilenkov_kirill_dag_parallel",
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

def transform_product_data(product, **kwargs):
    """
    Трансформация данных для заданного продукта
    :param product: продукт для трансформации данных
    :param kwargs: дополнительные параметры, переданные Airflow
    :return: DataFrame с флагами активности для заданного продукта
    """
    df = kwargs["ti"].xcom_pull(task_ids="extract_data")
    date = datetime.now().strftime("%Y-%m-%d")
    transformed_df = generate_activity_flags(df, date, product)
    return transformed_df

def load_transformed_data(product, **kwargs):
    """
    Загрузка трансформированных данных в CSV файл
    :param product: продукт для загрузки данных
    :param kwargs: дополнительные параметры, переданные Airflow
    """
    transformed_df = kwargs["ti"].xcom_pull(task_ids=f"transform_data_{product}")
    output_path = f"/opt/airflow/dags/flags_activity_{product}.csv"
    # Проверка наличия файла и запись данных
    if os.path.exists(output_path):
        transformed_df.to_csv(output_path, mode="a", header=False, index=False)
    else:
        transformed_df.to_csv(output_path, index=False)

# Список продуктов для обработки
products = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]

# Задача на извлечение данных
extract_task = PO(
    task_id="extract_data",
    python_callable=extract_data,
    dag=dag,
)

# Создание задач на трансформацию и загрузку данных для каждого продукта
for product in products:
    transform_task = PO(
        task_id=f"transform_data_{product}",
        python_callable=transform_product_data,
        provide_context=True,
        op_kwargs={"product": product},
        dag=dag,
    )

    load_task = PO(
        task_id=f"load_data_{product}",
        python_callable=load_transformed_data,
        provide_context=True,
        op_kwargs={"product": product},
        dag=dag,
    )

    # Определение порядка выполнения задач
    extract_task >> transform_task >> load_task
