from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text
from airflow.models import Variable
import logging
import traceback

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
        try:
            logging.info("Attempting to get database connection details from Variables")
            db_conn = Variable.get("medscan_postgres_conn", deserialize_json=True)
            logging.info("Successfully retrieved database connection details from Variables")
        except Exception as var_error:
            logging.error(f"Error getting database connection details: {str(var_error)}")
            logging.error(f"Available variables: {Variable.get_all()}")
            raise
        
        # Create SQLAlchemy engine
        try:
            connection_string = f"postgresql://{db_conn['user']}:{db_conn['password']}@{db_conn['host']}:{db_conn['port']}/{db_conn['database']}"
            logging.info(f"Attempting to connect to database at {db_conn['host']}:{db_conn['port']}")
            engine = create_engine(connection_string)
            logging.info("Successfully created SQLAlchemy engine")
        except Exception as conn_error:
            logging.error(f"Error creating database connection: {str(conn_error)}")
            logging.error(f"Connection string (without password): postgresql://{db_conn['user']}:****@{db_conn['host']}:{db_conn['port']}/{db_conn['database']}")
            raise
        
        # Execute query using SQLAlchemy
        try:
            query = text("SELECT * FROM shs_tani LIMIT 10")
            logging.info(f"Executing query: {query}")
            
            with engine.connect() as connection:
                records = connection.execute(query).fetchall()
                logging.info(f"Successfully fetched {len(records)} records")
                
                # Print records to console
                print("Fetched records from shs_tani table:")
                for record in records:
                    print(record)
        except Exception as query_error:
            logging.error(f"Error executing query: {str(query_error)}")
            logging.error(f"Full traceback: {traceback.format_exc()}")
            raise
                
    except Exception as e:
        logging.error(f"Unexpected error in read_shs_tani_records: {str(e)}")
        logging.error(f"Full traceback: {traceback.format_exc()}")
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