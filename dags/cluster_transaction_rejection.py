from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from src.cluster_rejection_notes.cluster_rejection_service import create_embeddings, perform_clustering

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'cluster_transaction_rejection',
    default_args=default_args,
    description='DAG for creating embeddings and clustering transaction rejections',
    schedule_interval='0 0 * * *',  # Run daily at midnight
    start_date=days_ago(1),
    catchup=False,
    tags=['embedding', 'clustering', 'transaction_rejection'],
)

# Create embeddings task
create_embeddings_task = PythonOperator(
    task_id='create_rejection_embeddings',
    python_callable=create_embeddings,
    dag=dag,
)

# Perform clustering task
perform_clustering_task = PythonOperator(
    task_id='perform_rejection_clustering',
    python_callable=perform_clustering,
    dag=dag,
)

# Set task dependencies
create_embeddings_task >> perform_clustering_task

# Add task dependencies here
# task1 >> task2  # Example dependency 