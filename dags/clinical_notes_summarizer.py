from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def read_shs_tani_records():
    # Hardcoded database connection parameters
    db_params = {
        'host': 'medscan-postgres-postgresql.medscan.svc.cluster.local',
        'port': 5432,
        'database': 'dataml',
        'user': 'medscan_user',
        'password': 'medscan_pass'
    }
    
    # Create PostgresHook instance with direct parameters
    pg_hook = PostgresHook(
        postgres_conn_id=None,  # We're not using a connection from Airflow
        host=db_params['host'],
        port=db_params['port'],
        database=db_params['database'],
        user=db_params['user'],
        password=db_params['password']
    )
    
    # Execute query to fetch 10 records
    query = "SELECT * FROM shs_tani LIMIT 10"
    records = pg_hook.get_records(query)
    
    # Print records to console
    print("Fetched records from shs_tani table:")
    for record in records:
        print(record)

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
