from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 25),
    'retries': 1,
}

version = 2
data_path = '/opt/airflow/dags/repo/dags/data/'

def read_csv_to_df(file_path: str) -> pd.DataFrame:
    print("reading....... : " + file_path)
    df = pd.read_csv(file_path)
    print("read_csv_to_df : "+file_path)
    print(df)
    return df


def join_csv_files(**context) -> pd.DataFrame:
    file_path1 = context['task_instance'].xcom_pull(task_ids='read_csv_file1')
    file_path2 = context['task_instance'].xcom_pull(task_ids='read_csv_file2')
    df1 = read_csv_to_df(file_path1)
    df2 = read_csv_to_df(file_path2)
    joined_df = pd.merge(df1, df2, on='Id', how='outer')
    print("read_csv_to_df")
    print(joined_df)
    return joined_df


def join_print(**context) -> None:
    print("join_print context")
    print(context.values())
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
        op_kwargs={'file_path': data_path+'file2.csv'}
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
