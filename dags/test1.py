from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.clinical_notes.summarize_notes_service import summarize

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'test1',
    default_args=default_args,
    description='DAG to get clinical notes',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['clinical'],
) as dag:

    get_notes_task = PythonOperator(
        task_id='test1',
        python_callable=summarize,
    )

    get_notes_task
