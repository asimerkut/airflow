from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text
from airflow.models import Variable
import logging
import traceback
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def read_shs_tani_records():
    logging.info("Starting read_shs_tani_records function")
    
    # Print all available variables for debugging
    try:
        all_vars = Variable.get_all()
        logging.info(f"Available variables: {list(all_vars.keys())}")
    except Exception as e:
        logging.error(f"Error getting variables: {str(e)}")
    
    try:
        # Get database connection details from Airflow Variables
        try:
            logging.info("Attempting to get database connection details from Variables")
            db_conn = Variable.get("medscan_postgres_conn", deserialize_json=True)
            logging.info("Successfully retrieved database connection details")
            logging.info(f"Connection details (without password): {db_conn.get('user')}@{db_conn.get('host')}:{db_conn.get('port')}/{db_conn.get('database')}")
        except Exception as var_error:
            logging.error(f"Error getting database connection details: {str(var_error)}")
            logging.error(f"Full traceback: {traceback.format_exc()}")
            raise
        
        # Create SQLAlchemy engine
        try:
            logging.info("Creating database connection string")
            connection_string = f"postgresql://{db_conn['user']}:{db_conn['password']}@{db_conn['host']}:{db_conn['port']}/{db_conn['database']}"
            logging.info(f"Attempting to connect to database at {db_conn['host']}:{db_conn['port']}")
            engine = create_engine(connection_string)
            logging.info("Successfully created SQLAlchemy engine")
        except Exception as conn_error:
            logging.error(f"Error creating database connection: {str(conn_error)}")
            logging.error(f"Connection string (without password): postgresql://{db_conn['user']}:****@{db_conn['host']}:{db_conn['port']}/{db_conn['database']}")
            logging.error(f"Full traceback: {traceback.format_exc()}")
            raise
        
        # Execute query using SQLAlchemy
        try:
            logging.info("Preparing to execute database query")
            query = text("SELECT * FROM shs_tani LIMIT 10")
            logging.info(f"Executing query: {query}")
            
            with engine.connect() as connection:
                logging.info("Database connection established, executing query")
                records = connection.execute(query).fetchall()
                logging.info(f"Successfully fetched {len(records)} records")
                
                # Print records to console
                logging.info("Printing fetched records")
                print("Fetched records from shs_tani table:")
                for record in records:
                    print(record)
                logging.info("Finished printing records")
        except Exception as query_error:
            logging.error(f"Error executing query: {str(query_error)}")
            logging.error(f"Full traceback: {traceback.format_exc()}")
            raise
                
    except Exception as e:
        logging.error(f"Unexpected error in read_shs_tani_records: {str(e)}")
        logging.error(f"Full traceback: {traceback.format_exc()}")
        raise
    finally:
        logging.info("Completed read_shs_tani_records function")

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