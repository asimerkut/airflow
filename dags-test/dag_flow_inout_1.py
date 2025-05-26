import argparse
import json
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
from pyspark.sql import SparkSession

import src.auto.lib.lib_connector as lib_connector
import src.auto.lib.lib_custom as lib_custom
from src.auto.sys import sys_node_io
from src.auto.util import static_util
from src.auto.util.enum_util import ConnectorEnum

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

class Cmp:
    def __init__(self, spark_master: str, flow_id: str):
        self.spark = SparkSession.builder.master(spark_master).appName(flow_id).getOrCreate()
cmp = {}
connector_map = dict()


def init_conn():
    load_dotenv()
    global cmp
    cmp = Cmp("local[4]", "9c6f81c5-70a7-481d-bb89-adcd2cc81990")
    connector_map["1012"] = task_LibConnector_create_connector_db_postgres_1012()
    connector_map["1006"] = task_LibConnector_create_connector_db_postgres_1006()


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

def task_LibConnector_create_connector_db_postgres_1012():
    object_prop = {
        'host': 'host.docker.internal',
        'port': 5432,
        'database': 'sgkdb',
        'user': 'postgres',
        'password': 'postgres',
    }
    object_vars = {

    }

    object_vars, connector_instance = lib_connector.create_connector_db_postgres(cmp=cmp, object_prop=object_prop, object_vars=object_vars)
    return [object_vars, connector_instance]


def read_input(param) -> tuple:
    pass


def task_LibCustom_etl_query_1009(**context):

    init_conn()
    object_prop = {
        'connector_id': '1006',
        'query': "select \n  t.takip_no basket_id, \n  t.brans_grup_kodu grup_id, \n  t.tesis_il_kodu loca_id, \n  t.os_ayaktan_yatarak turu_id,\n  TO_CHAR(t.donem_son_tarih, 'YYYYMM') term_id,\n  json_build_object(\n    'tedavi_turu', t.os_ayaktan_yatarak,\n    'brans_kodu', t.brans_kodu,\n\t  'cinsiyet', t.hak_cinsiyet,\n\t  'tesis_kodu', t.tesis_kodu\n  ) AS basket_info  \nfrom shs_takip t",
    }
    object_vars = {

    }
    # example def_port_in      # [None, ('DAT', 'df1'), ('DAT', 'df2')]
    # example  object_port_in  # {'input-0': None, 'input-1': [1009, 1], 'input-2': [1008, 2]}

    #todo:
    flow_id = PRM["PRM_FLOW_ID"]
    node_no = "1009"
    def_port_in = [None]
    def_port_out = [None, ('DAT', 'df_query')]
    object_port_in = {'input-0': None}

    input_port_args = sys_node_io.get_input_port_args(
        flow_id=flow_id,
        def_port_in=def_port_in,
        object_port_in=object_port_in,
        object_prop=None,connector_field=None,service_conn=None) # optional

    #connector_field= service_conn
    input_port_args[ConnectorEnum.rdbms_connector] = connector_map["1006"][1]

    result_tuple = lib_custom.etl_query(cmp=cmp, object_prop=object_prop, object_vars=object_vars, **input_port_args)
    result_tuple = static_util.convert_to_tuple(result_tuple)
    # todo:

    sys_node_io.set_output_port_args(
        flow_id=flow_id,
        node_no=node_no,
        result_tuple=result_tuple,
        def_port_out=def_port_out,
        service_conn=None) # optional


    # Write Output

    return result_tuple


def init_flow():
    finish_task_list = [
        task_LibCustom_etl_query_1009]
    for task_func in finish_task_list:
        task_func()

if __name__ == "__main__":
    print("Flow Start >> " + PRM["PRM_FLOW_ID"])
    parser = argparse.ArgumentParser()
    parser.add_argument("--PRM_EXTRA", type=str, default="{}")
    args = parser.parse_args()
    prm_arg = json.loads(args.PRM_EXTRA)
    PRM.update(prm_arg)
    print(json.dumps(PRM, indent=2))
    init_flow()
    print("Flow Finish >> " + PRM["PRM_FLOW_ID"])


with DAG('dag_flow_06', default_args=default_args, schedule_interval=None) as dag:

    task_LibCustom_etl_query_1009 = PythonOperator(
        task_id='task_LibCustom_etl_query_1009',
        python_callable=task_LibCustom_etl_query_1009,
        op_kwargs={'file_path': data_path+'iris1.csv'}
    )

    task_LibCustom_etl_query_1009

