from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 25),
    'retries': 1,
}

version = 4
data_path = '/opt/airflow/dags/repo/dags/data/'

def print_context(**context):
    def serialize(obj):
        try:
            return str(obj)
        except:
            return "Unserializable"

    print(json.dumps({k: serialize(v) for k, v in context.items()}, indent=2))

def read_csv_to_df(file_path: str) -> str:
    print("reading....... : " + file_path)
    df = pd.read_csv(file_path)
    print("read_csv_to_df : "+file_path)
    print(df)
    js = df.to_json(orient='records')
    print(js)
    return js


def join_csv_files(**context) -> str:
    print("join_csv_files context")
    print_context(**context)

    json1 = context['ti'].xcom_pull(task_ids='read_csv_file1')
    json2 = context['ti'].xcom_pull(task_ids='read_csv_file2')

    df1 = pd.read_json(json1, orient='records')
    df2 = pd.read_json(json2, orient='records')

    joined_df = pd.merge(df1, df2, on='Id', how='outer')
    print("read_csv_to_df")
    print(joined_df)
    ret_json = joined_df.to_json(orient='records')
    return ret_json


def join_print(**context) -> None:
    print("join_print context")
    print_context(**context)
    pass


with DAG('csv_join_dag', default_args=default_args, schedule_interval=None) as dag:

    read_csv_file1 = PythonOperator(
        task_id='read_csv_file1',
        python_callable=read_csv_to_df,
        op_kwargs={'file_path': data_path+'iris1.csv'}
    )

    read_csv_file2 = PythonOperator(
        task_id='read_csv_file2',
        python_callable=read_csv_to_df,
        op_kwargs={'file_path': data_path+'iris2.csv'}
    )

    join_csv = PythonOperator(
        task_id='join_csv_files',
        python_callable=join_csv_files
    )

    join_print = PythonOperator(
        task_id='join_print',
        python_callable=join_print
    )

    read_csv_file1 >> join_csv
    read_csv_file2 >> join_csv
    join_csv >> join_print
