from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
from dotenv import load_dotenv
from functools import lru_cache
from pyspark.sql import SparkSession
import json
import argparse

import src.auto.lib.lib_ai as lib_ai
import src.auto.lib.lib_custom as lib_custom
import src.auto.lib.lib_etl as lib_etl
import src.auto.lib.lib_connector as lib_connector
import src.auto.lib.lib_io_rdbms as lib_io_rdbms
import src.auto.lib.lib_dim as lib_dim
import src.auto.lib.lib_io_filesys as lib_io_filesys
import src.auto.lib.lib_io_user as lib_io_user
import src.auto.lib.lib_stat as lib_stat
import src.auto.lib.lib_vis as lib_vis

PRM = dict(
    PRM_FLOW_ID="9c6f81c5-70a7-481d-bb89-adcd2cc81990",
    PRM_FLOW_NAME="dag_flow_01",
    PRM_FLOW_DATE="2025-05-26 21:21:58"
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 25),
    'retries': 1,
}

version = 1
data_path = '/opt/airflow/dags/repo/dags/data/'

connector_map = dict()
cmp = {}


def task_LibConnector_create_connector_db_postgres_1006():

    object_prop = {
        'host': 'host.docker.internal',
        'port': 5432,
        'database': 'dataml',
        'user': 'postgres',
        'password': 'postgres',
    }
    object_vars = {
        'varmi': 'yokmu',
    }

    object_vars, connector_instance = lib_connector.create_connector_db_postgres(cmp=cmp, object_prop=object_prop, object_vars=object_vars)
    return [object_vars, connector_instance]



def task_LibCustom_etl_query_1009(**context):

    load_dotenv()
    connector_map["1006"] = task_LibConnector_create_connector_db_postgres_1006()

    rdbms_connector = connector_map.get("1006")[1]
    object_prop = {
        'connector_id': '1006',
        'query': "select \n  t.takip_no basket_id, \n  t.brans_grup_kodu grup_id, \n  t.tesis_il_kodu loca_id, \n  t.os_ayaktan_yatarak turu_id,\n  TO_CHAR(t.donem_son_tarih, 'YYYYMM') term_id,\n  json_build_object(\n    'tedavi_turu', t.os_ayaktan_yatarak,\n    'brans_kodu', t.brans_kodu,\n\t  'cinsiyet', t.hak_cinsiyet,\n\t  'tesis_kodu', t.tesis_kodu\n  ) AS basket_info  \nfrom shs_takip t",
    }
    object_vars = {

    }

    object_vars, df_query = lib_custom.etl_query(cmp=cmp, object_prop=object_prop, object_vars=object_vars, rdbms_connector=rdbms_connector)
    print(df_query.head())
    return [object_vars, df_query]




with DAG('dag_flow_05', default_args=default_args, schedule_interval=None) as dag:

    task_LibCustom_etl_query_1009 = PythonOperator(
        task_id='task_LibCustom_etl_query_1009',
        python_callable=task_LibCustom_etl_query_1009,
        op_kwargs={'file_path': data_path+'iris1.csv'}
    )

    task_LibCustom_etl_query_1009

