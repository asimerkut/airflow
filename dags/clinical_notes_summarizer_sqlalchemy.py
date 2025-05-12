from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text
from airflow.models import Variable
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
    try:
        # Get database connection details from Airflow Variables
        db_conn = Variable.get("medscan_postgres_conn", deserialize_json=True)
        
        # Create SQLAlchemy engine
        connection_string = f"postgresql://{db_conn['user']}:{db_conn['password']}@{db_conn['host']}:{db_conn['port']}/{db_conn['database']}"
        engine = create_engine(connection_string)
        logging.info("Created SQLAlchemy engine")
        
        # Execute query using SQLAlchemy
        query = text("SELECT * FROM shs_tani LIMIT 10")
        logging.info(f"Executing query: {query}")
        
        with engine.connect() as connection:
            records = connection.execute(query).fetchall()
            logging.info(f"Successfully fetched {len(records)} records")
            
            # Print records to console
            print("Fetched records from shs_tani table:")
            for record in records:
                print(record)
                
    except Exception as e:
        logging.error(f"Error in read_shs_tani_records: {str(e)}")
        raise

with DAG(
    'clinical_notes_summarizer_sqlalchemy',
    default_args=default_args,
    description='DAG to read records from shs_tani table using SQLAlchemy',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['clinical', 'postgres', 'sqlalchemy'],
) as dag:

    read_records_task = PythonOperator(
        task_id='read_shs_tani_records',
        python_callable=read_shs_tani_records,
    )

    read_records_task 