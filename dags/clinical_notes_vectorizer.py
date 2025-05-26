from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.clinical_notes.vectorize_notes_service import vectorize

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'clinical_notes_vectorizer',
    default_args=default_args,
    description='DAG to vectorize clinical notes using BioBERT',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['clinical', 'ml'],
) as dag:

    vectorize_notes_task = PythonOperator(
        task_id='vectorize_clinical_notes',
        python_callable=vectorize,
    )

    vectorize_notes_task 