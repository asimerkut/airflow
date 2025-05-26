# -*- coding: utf-8 -*-
# -*- python -*-

# Part_Imp ##################

# --PRM_EXTRA {"PRM_FLOW_DESC":"description"}
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

# Part_Ini ##################

load_dotenv()
PRM = dict(
    PRM_FLOW_ID="9c6f81c5-70a7-481d-bb89-adcd2cc81990",
    PRM_FLOW_NAME="Birliktelik-DB",
    PRM_FLOW_DATE="2025-05-26 21:21:58"
)


class Cmp:
    def __init__(self, spark_master: str, flow_id: str):
        self.spark = SparkSession.builder.master(spark_master).appName(flow_id).getOrCreate()


cmp = None

connector_map = dict()


# Part_Fnc ####################

# {'title': '1008', 'nodePlace': ''}
@lru_cache(maxsize=1)
def task_LibAi_association_fpgrowth_1008():
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

    df = task_LibCustom_etl_query_1007()[1]
    object_vars, rules, results = lib_ai.association_fpgrowth(cmp=cmp, object_prop=object_prop, object_vars=object_vars, df=df)
    return [object_vars, rules, results]


# {'title': 'Takip Detayları', 'nodePlace': 'StartNode'}
@lru_cache(maxsize=1)
def task_LibCustom_etl_query_1009():
    rdbms_connector = connector_map.get("1006")[1]
    object_prop = {
        'connector_id': '1006',
        'query': "select \n  t.takip_no basket_id, \n  t.brans_grup_kodu grup_id, \n  t.tesis_il_kodu loca_id, \n  t.os_ayaktan_yatarak turu_id,\n  TO_CHAR(t.donem_son_tarih, 'YYYYMM') term_id,\n  json_build_object(\n    'tedavi_turu', t.os_ayaktan_yatarak,\n    'brans_kodu', t.brans_kodu,\n\t  'cinsiyet', t.hak_cinsiyet,\n\t  'tesis_kodu', t.tesis_kodu\n  ) AS basket_info  \nfrom shs_takip t",
    }
    object_vars = {

    }

    object_vars, df_query = lib_custom.etl_query(cmp=cmp, object_prop=object_prop, object_vars=object_vars, rdbms_connector=rdbms_connector)
    return [object_vars, df_query]


# {'title': '1010', 'nodePlace': 'FinishNode'}
@lru_cache(maxsize=1)
def task_LibEtl_join_1010():
    object_prop = {
        'how': 'outer',
        'left_on': 'basket_id',
        'right_on': 'basket_id',
    }
    object_vars = {

    }

    df1 = task_LibCustom_etl_query_1009()[1]
    df2 = task_LibAi_association_fpgrowth_1008()[2]
    object_vars, df_join = lib_etl.join(cmp=cmp, object_prop=object_prop, object_vars=object_vars, df1=df1, df2=df2)
    return [object_vars, df_join]


# {'title': '1012-target', 'nodePlace': 'ConnNode'}
@lru_cache(maxsize=1)
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


# {'title': '1015', 'nodePlace': 'FinishNode'}
@lru_cache(maxsize=1)
def task_LibIoRdbms_rdbms_write_prediction_1015():
    rdbms_connector = connector_map.get("1012")[1]
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

    df = task_LibEtl_join_1010()[1]
    object_vars = lib_io_rdbms.rdbms_write_prediction(cmp=cmp, object_prop=object_prop, object_vars=object_vars, rdbms_connector=rdbms_connector, df=df)
    return [object_vars, ]


# {'title': 'source', 'nodePlace': 'ConnNode'}
@lru_cache(maxsize=1)
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


# {'title': 'Tanı Listesi', 'nodePlace': 'StartNode'}
@lru_cache(maxsize=1)
def task_LibCustom_etl_query_1007():
    rdbms_connector = connector_map.get("1006")[1]
    object_prop = {
        'connector_id': '1006',
        'query': 'select\n islem_sira_no ix, takip_no basket_id, tani_kodu item_id \nfrom shs_tani',
    }
    object_vars = {

    }

    object_vars, df_query = lib_custom.etl_query(cmp=cmp, object_prop=object_prop, object_vars=object_vars, rdbms_connector=rdbms_connector)
    return [object_vars, df_query]


# Part_Con ##################
def init_conn():
    global cmp
    cmp = Cmp("local[4]", "9c6f81c5-70a7-481d-bb89-adcd2cc81990")
    connector_map["1012"] = task_LibConnector_create_connector_db_postgres_1012()
    connector_map["1006"] = task_LibConnector_create_connector_db_postgres_1006()


# Part_Exe ####################
def init_flow():
    finish_task_list = [
        task_LibEtl_join_1010,
        task_LibIoRdbms_rdbms_write_prediction_1015]
    for task_func in finish_task_list:
        task_func()


if __name__ == "__main__":
    in_port = ("a", "b")

    print("Flow Start >> " + PRM["PRM_FLOW_ID"])
    parser = argparse.ArgumentParser()
    parser.add_argument("--PRM_EXTRA", type=str, default="{}")
    args = parser.parse_args()
    prm_arg = json.loads(args.PRM_EXTRA)
    PRM.update(prm_arg)
    print(json.dumps(PRM, indent=2))
    init_conn()
    init_flow()
    print("Flow Finish >> " + PRM["PRM_FLOW_ID"])

# Part_Dag ####################

# task_LibCustom_etl_query_1007()[1] >> task_LibAi_association_fpgrowth_1008()[1]
# task_LibCustom_etl_query_1009()[1] >> task_LibEtl_join_1010()[1]
# task_LibAi_association_fpgrowth_1008()[2] >> task_LibEtl_join_1010()[2]
# task_LibEtl_join_1010()[1] >> task_LibIoRdbms_rdbms_write_prediction_1015()[1]
