from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Connection
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def read_shs_tani_records():
    # Get connection directly from Airflow
    conn = Connection.get_connection_from_secrets('medscan_postgres')
    logging.info(f"Retrieved connection: {conn}")
    
    # Create PostgresHook instance with connection
    pg_hook = PostgresHook(conn)
    logging.info("Created PostgresHook")
    
    # Execute query to fetch 10 records
    query = "SELECT * FROM shs_tani LIMIT 10"
    logging.info(f"Executing query: {query}")
    
    try:
        records = pg_hook.get_records(query)
        logging.info(f"Successfully fetched {len(records)} records")
        # Print records to console
        print("Fetched records from shs_tani table:")
        for record in records:
            print(record)
    except Exception as e:
        logging.error(f"Error executing query: {str(e)}")
        raise

with DAG(
    'clinical_notes_summarizer',
    default_args=default_args,
    description='DAG to read records from shs_tani table',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['clinical', 'postgres'],
) as dag:

    read_records_task = PythonOperator(
        task_id='read_shs_tani_records',
        python_callable=read_shs_tani_records,
    )

    read_records_task
