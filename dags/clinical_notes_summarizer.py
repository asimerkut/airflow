from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from dags.src.clinical_notes.summarize_notes_service import get_clinical_note

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'clinical_notes_summarizer',
    default_args=default_args,
    description='DAG to get clinical notes',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['clinical'],
) as dag:

    get_notes_task = PythonOperator(
        task_id='get_clinical_note',
        python_callable=get_clinical_note,
    )

    get_notes_task
