# -*- coding: utf-8    -*-
# -*- AirFlow DAG File -*-

# Part_Imp ##################

# --PRM_EXTRA {"PRM_FLOW_DESC":"description"}
from airflow import DAG
from airflow.operators.python import PythonOperator
import argparse
import json
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from src.auto.sys import sys_node_io
from src.auto.util import static_util
from src.auto.util.enum_util import NodePortEnum, ConnectorEnum

import src.auto.lib.lib_ai as lib_ai
import src.auto.lib.lib_custom as lib_custom
import src.auto.lib.lib_etl as lib_etl
import src.auto.lib.lib_connector as lib_connector
import src.auto.lib.lib_io_rdbms as lib_io_rdbms

# Part_Ini ##################

load_dotenv()
PRM = dict(
    PRM_FLOW_ID="9c6f81c5-70a7-481d-bb89-adcd2cc81990",
    PRM_FLOW_NAME="Birliktelik-DB-7",
    PRM_FLOW_DATE="2025-05-29 00:07:53"
)

default_args = dict(
    owner='airflow',
    depends_on_past=False,
    start_date=datetime(2025, 5, 25),
    retries=1,
)


class Cmp:
    def __init__(self, spark_master: str, flow_id: str):
        self.spark = SparkSession.builder.master(spark_master).appName(flow_id).getOrCreate()


cmp = None

connector_map = dict()


# Part_Fnc ####################

# {'title': '1008', 'nodePlace': ''}
def node_LibAi_association_fpgrowth_1008():
    flow_id = PRM.get("PRM_FLOW_ID", None)
    node_no = "1008"
    def_port_in = [None, ('DAT', 'df')]
    def_port_out = [None, ('DAT', 'rules'), ('DAT', 'results')]
    object_port_in = {'input-0': None, 'input-1': [1007, 1]}

    object_prop = {
        'min_support': 0.01,
        'use_colnames': True,
        'verbose': 0,
        'metric': 'confidence',
        'min_threshold': 0.1,
        'col_key': 'ix',
        'col_group': 'basket_id',
        'col_item': 'item_id',
    }
    object_vars = {

    }
    connector_field = None

    global cmp
    cmp = Cmp("local[4]", flow_id)

    if connector_field is not None:
        init_conn()

    input_port_args = sys_node_io.get_input_port_args(
        flow_id=flow_id,
        def_port_in=def_port_in,
        object_port_in=object_port_in,
        object_prop=None, connector_field=None, service_conn=None)  # optional

    result_tuple = lib_ai.association_fpgrowth(cmp=cmp, object_prop=object_prop, object_vars=object_vars, **input_port_args)
    result_tuple = static_util.convert_to_tuple(result_tuple)

    sys_node_io.set_output_port_args(
        flow_id=flow_id,
        node_no=node_no,
        result_tuple=result_tuple,
        def_port_out=def_port_out,
        service_conn=None)  # optional

    return result_tuple


# {'title': 'Takip Detayları', 'nodePlace': 'StartNode'}
def node_LibCustom_etl_query_1009():
    flow_id = PRM.get("PRM_FLOW_ID", None)
    node_no = "1009"
    def_port_in = [None]
    def_port_out = [None, ('DAT', 'df_query')]
    object_port_in = {'input-0': None}

    object_prop = {
        'connector_id': '1006',
        'query': "select \n  t.takip_no basket_id, \n  t.brans_grup_kodu grup_id, \n  t.tesis_il_kodu loca_id, \n  t.os_ayaktan_yatarak turu_id,\n  TO_CHAR(t.donem_son_tarih, 'YYYYMM') term_id,\n  json_build_object(\n    'tedavi_turu', t.os_ayaktan_yatarak,\n    'brans_kodu', t.brans_kodu,\n\t  'cinsiyet', t.hak_cinsiyet,\n\t  'tesis_kodu', t.tesis_kodu\n  ) AS basket_info  \nfrom shs_takip t",
    }
    object_vars = {

    }
    connector_field = 'rdbms_connector'

    global cmp
    cmp = Cmp("local[4]", flow_id)

    if connector_field is not None:
        init_conn()

    input_port_args = sys_node_io.get_input_port_args(
        flow_id=flow_id,
        def_port_in=def_port_in,
        object_port_in=object_port_in,
        object_prop=None, connector_field=None, service_conn=None)  # optional
    input_port_args[connector_field] = connector_map.get("1006")[1]

    result_tuple = lib_custom.etl_query(cmp=cmp, object_prop=object_prop, object_vars=object_vars, **input_port_args)
    result_tuple = static_util.convert_to_tuple(result_tuple)

    sys_node_io.set_output_port_args(
        flow_id=flow_id,
        node_no=node_no,
        result_tuple=result_tuple,
        def_port_out=def_port_out,
        service_conn=None)  # optional

    return result_tuple


# {'title': '1010', 'nodePlace': 'FinishNode'}
def node_LibEtl_join_1010():
    flow_id = PRM.get("PRM_FLOW_ID", None)
    node_no = "1010"
    def_port_in = [None, ('DAT', 'df1'), ('DAT', 'df2')]
    def_port_out = [None, ('DAT', 'df_join')]
    object_port_in = {'input-0': None, 'input-1': [1009, 1], 'input-2': [1008, 2]}

    object_prop = {
        'how': 'outer',
        'left_on': 'basket_id',
        'right_on': 'basket_id',
    }
    object_vars = {

    }
    connector_field = None

    global cmp
    cmp = Cmp("local[4]", flow_id)

    if connector_field is not None:
        init_conn()

    input_port_args = sys_node_io.get_input_port_args(
        flow_id=flow_id,
        def_port_in=def_port_in,
        object_port_in=object_port_in,
        object_prop=None, connector_field=None, service_conn=None)  # optional

    result_tuple = lib_etl.join(cmp=cmp, object_prop=object_prop, object_vars=object_vars, **input_port_args)
    result_tuple = static_util.convert_to_tuple(result_tuple)

    sys_node_io.set_output_port_args(
        flow_id=flow_id,
        node_no=node_no,
        result_tuple=result_tuple,
        def_port_out=def_port_out,
        service_conn=None)  # optional

    return result_tuple


# {'title': '1012-target', 'nodePlace': 'ConnNode'}
def node_LibConnector_create_connector_db_postgres_1012():
    flow_id = PRM.get("PRM_FLOW_ID", None)
    node_no = "1012"
    def_port_in = [None]
    def_port_out = [None, ('CON', 'connector_instance')]
    object_port_in = {'input-0': None}

    object_prop = {
        'host': 'host.docker.internal',
        'port': 5432,
        'database': 'sgkdb',
        'user': 'postgres',
        'password': 'postgres',
    }
    object_vars = {

    }
    connector_field = None

    global cmp
    cmp = Cmp("local[4]", flow_id)

    if connector_field is not None:
        init_conn()

    input_port_args = sys_node_io.get_input_port_args(
        flow_id=flow_id,
        def_port_in=def_port_in,
        object_port_in=object_port_in,
        object_prop=None, connector_field=None, service_conn=None)  # optional

    result_tuple = lib_connector.create_connector_db_postgres(cmp=cmp, object_prop=object_prop, object_vars=object_vars, **input_port_args)
    result_tuple = static_util.convert_to_tuple(result_tuple)

    sys_node_io.set_output_port_args(
        flow_id=flow_id,
        node_no=node_no,
        result_tuple=result_tuple,
        def_port_out=def_port_out,
        service_conn=None)  # optional

    return result_tuple


# {'title': '1015', 'nodePlace': 'FinishNode'}
def node_LibIoRdbms_rdbms_write_prediction_1015():
    flow_id = PRM.get("PRM_FLOW_ID", None)
    node_no = "1015"
    def_port_in = [None, ('DAT', 'df')]
    def_port_out = [None]
    object_port_in = {'input-0': None, 'input-1': [1010, 1]}

    object_prop = {
        'connector_id': '1012',
        'term_id': '202301',
        'turu_id': 'A',
        'loca_id': 'TR-01',
        'grup_id': 'B1071',
        'modl_id': '1',
    }
    object_vars = {

    }
    connector_field = 'rdbms_connector'

    global cmp
    cmp = Cmp("local[4]", flow_id)

    if connector_field is not None:
        init_conn()

    input_port_args = sys_node_io.get_input_port_args(
        flow_id=flow_id,
        def_port_in=def_port_in,
        object_port_in=object_port_in,
        object_prop=None, connector_field=None, service_conn=None)  # optional
    input_port_args[connector_field] = connector_map.get("1012")[1]

    result_tuple = lib_io_rdbms.rdbms_write_prediction(cmp=cmp, object_prop=object_prop, object_vars=object_vars, **input_port_args)
    result_tuple = static_util.convert_to_tuple(result_tuple)

    sys_node_io.set_output_port_args(
        flow_id=flow_id,
        node_no=node_no,
        result_tuple=result_tuple,
        def_port_out=def_port_out,
        service_conn=None)  # optional

    return result_tuple


# {'title': 'source', 'nodePlace': 'ConnNode'}
def node_LibConnector_create_connector_db_postgres_1006():
    flow_id = PRM.get("PRM_FLOW_ID", None)
    node_no = "1006"
    def_port_in = [None]
    def_port_out = [None, ('CON', 'connector_instance')]
    object_port_in = {'input-0': None}

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
    connector_field = None

    global cmp
    cmp = Cmp("local[4]", flow_id)

    if connector_field is not None:
        init_conn()

    input_port_args = sys_node_io.get_input_port_args(
        flow_id=flow_id,
        def_port_in=def_port_in,
        object_port_in=object_port_in,
        object_prop=None, connector_field=None, service_conn=None)  # optional

    result_tuple = lib_connector.create_connector_db_postgres(cmp=cmp, object_prop=object_prop, object_vars=object_vars, **input_port_args)
    result_tuple = static_util.convert_to_tuple(result_tuple)

    sys_node_io.set_output_port_args(
        flow_id=flow_id,
        node_no=node_no,
        result_tuple=result_tuple,
        def_port_out=def_port_out,
        service_conn=None)  # optional

    return result_tuple


# {'title': 'Tanı Listesi', 'nodePlace': 'StartNode'}
def node_LibCustom_etl_query_1007():
    flow_id = PRM.get("PRM_FLOW_ID", None)
    node_no = "1007"
    def_port_in = [None]
    def_port_out = [None, ('DAT', 'df_query')]
    object_port_in = {'input-0': None}

    object_prop = {
        'connector_id': '1006',
        'query': 'select\n islem_sira_no ix, takip_no basket_id, tani_kodu item_id \nfrom shs_tani',
    }
    object_vars = {

    }
    connector_field = 'rdbms_connector'

    global cmp
    cmp = Cmp("local[4]", flow_id)

    if connector_field is not None:
        init_conn()

    input_port_args = sys_node_io.get_input_port_args(
        flow_id=flow_id,
        def_port_in=def_port_in,
        object_port_in=object_port_in,
        object_prop=None, connector_field=None, service_conn=None)  # optional
    input_port_args[connector_field] = connector_map.get("1006")[1]

    result_tuple = lib_custom.etl_query(cmp=cmp, object_prop=object_prop, object_vars=object_vars, **input_port_args)
    result_tuple = static_util.convert_to_tuple(result_tuple)

    sys_node_io.set_output_port_args(
        flow_id=flow_id,
        node_no=node_no,
        result_tuple=result_tuple,
        def_port_out=def_port_out,
        service_conn=None)  # optional

    return result_tuple


# Part_Con ##################
def init_conn():
    connector_map["1012"] = node_LibConnector_create_connector_db_postgres_1012()
    connector_map["1006"] = node_LibConnector_create_connector_db_postgres_1006()


# Part_Dag ####################
with DAG(str(PRM.get("PRM_FLOW_NAME")).replace(" ","_"), default_args=default_args, schedule_interval=None) as dag:
    task_LibAi_association_fpgrowth_1008 = PythonOperator(
        task_id="task_LibAi_association_fpgrowth_1008",
        python_callable=node_LibAi_association_fpgrowth_1008,
        op_kwargs={})

    task_LibCustom_etl_query_1009 = PythonOperator(
        task_id="task_LibCustom_etl_query_1009",
        python_callable=node_LibCustom_etl_query_1009,
        op_kwargs={})

    task_LibEtl_join_1010 = PythonOperator(
        task_id="task_LibEtl_join_1010",
        python_callable=node_LibEtl_join_1010,
        op_kwargs={})

    task_LibIoRdbms_rdbms_write_prediction_1015 = PythonOperator(
        task_id="task_LibIoRdbms_rdbms_write_prediction_1015",
        python_callable=node_LibIoRdbms_rdbms_write_prediction_1015,
        op_kwargs={})

    task_LibCustom_etl_query_1007 = PythonOperator(
        task_id="task_LibCustom_etl_query_1007",
        python_callable=node_LibCustom_etl_query_1007,
        op_kwargs={})

    task_LibCustom_etl_query_1007 >> task_LibAi_association_fpgrowth_1008
    task_LibCustom_etl_query_1009 >> task_LibEtl_join_1010
    task_LibAi_association_fpgrowth_1008 >> task_LibEtl_join_1010
    task_LibEtl_join_1010 >> task_LibIoRdbms_rdbms_write_prediction_1015
