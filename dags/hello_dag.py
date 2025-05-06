from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="hello_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["example"]
) as dag:
    t1 = BashOperator(
        task_id="say_hello",
        bash_command="echo 'Super bir sekilde Merhaba, Airflow projesi çalışıyor!'"
    )
