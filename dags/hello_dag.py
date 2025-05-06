# dags/hello_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from src.utils import hello  # ✅ src içinden fonksiyonu çağır

with DAG(
    dag_id="hello_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["example"]
) as dag:
    t1 = PythonOperator(
        task_id="call_hello_from_src",
        python_callable=hello
    )
