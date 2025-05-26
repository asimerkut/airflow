import ast
import uuid

import pandas as pd
from psycopg2.extras import Json as PgJson

import src.auto as auto
from src.auto.util.enum_util import KeyEnum


def write_table(cmp, object_prop, object_vars, rdbms_connector, df):
    rdbms_connector.write_table(
        schema=object_prop["schema_name"],
        table=object_prop["table_name"],
        dataframe=df,
        insert_template=object_prop["insert_template"],
        has_a_sequence=object_prop["has_a_sequence"],
        sequence_name=object_prop["sequence_name"],
        identity_column=object_prop["identity_column"]
    )
    del df
    return object_vars


def update_table(cmp, object_prop, object_vars, rdbms_connector):
    rdbms_connector.update_table(
        schema=object_prop["schema_name"],
        table=object_prop["table_name"],
        key_value_map=object_prop["key_value_map"],
        where_caluse=object_prop["where_caluse"]
    )
    return object_vars


def truncate_table(cmp, object_prop, object_vars, rdbms_connector, df):
    data = [object_prop["schema_name"], object_prop["table_name"]]
    df_meta_veri = pd.DataFrame(data=[data], columns=["schema_name", "table_name"])

    rdbms_connector.truncate_table(schema_name=object_prop["schema_name"],
                                   table_name=object_prop["table_name"])
    return object_vars, df, df_meta_veri


def read_table(cmp, object_prop, object_vars, rdbms_connector):
    df = rdbms_connector.select_from_table(
        schema_name=object_prop["schema_name"],
        table_name=object_prop["table_name"],
        columns=object_prop["column_names"],
        where_clause=object_prop["where_clause"]
    )
    return object_vars, df


def list_table(cmp, object_prop, object_vars, rdbms_connector):
    df_tables = rdbms_connector.list_tables(object_prop["database_name"], object_prop["schema_name"])
    return object_vars, df_tables


def list_schema(cmp, object_prop, object_vars, rdbms_connector):
    df_schemas = rdbms_connector.list_schemas(object_prop["database_name"])
    return object_vars, df_schemas


def list_database(cmp, object_prop, object_vars, rdbms_connector):
    df_databases = rdbms_connector.list_databases()
    return object_vars, df_databases


def list_column(cmp, object_prop, object_vars, rdbms_connector):
    df_columns = rdbms_connector.list_columns(object_prop["database_name"],
                                              object_prop["schema_name"], object_prop["table_name"])
    return object_vars, df_columns


def delete_table(cmp, object_prop, object_vars, rdbms_connector, df):
    data = [object_prop["database_name"], object_prop["schema_name"], object_prop["table_name"]]
    df_meta_veri = pd.DataFrame(data=[data], columns=["database_name", "schema_name", "table_name"])

    if object_prop["criteria"] is not None:
        rdbms_connector.delete_from_table(schema_name=object_prop["schema_name"],
                                          table_name=object_prop["table_name"],
                                          where_clause=object_prop["criteria"])
    else:
        rdbms_connector.truncate_table(schema_name=object_prop["schema_name"],
                                       table_name=object_prop["table_name"])
    return object_vars, df, df_meta_veri


def delete_schema(cmp, object_prop, object_vars, rdbms_connector, df):
    df_meta_veri = pd.DataFrame(columns=["schema_name"], data=[object_prop["schema_name"]])
    rdbms_connector.drop_schema(object_prop["schema_name"])
    return object_vars, df, df_meta_veri


def delete_database(cmp, object_prop, object_vars, rdbms_connector, df):
    df_meta_veri = pd.DataFrame(columns=["database_name"], data=[object_prop["database_name"]])
    rdbms_connector.drop_database(object_prop["database_name"])
    return object_vars, df, df_meta_veri


def create_table(cmp, object_prop, object_vars, rdbms_connector, df):
    rdbms_connector.create_table(
        schema=object_prop["schema_name"],
        table=object_prop["table_name"],
        dataframe=df,
        if_exists=object_prop["if_exists"],
        index=object_prop["index"]
    )
    return object_vars


def cursor(cmp, object_prop, object_vars, rdbms_connector):
    cursor = rdbms_connector.cursor()
    return object_vars, cursor


def safe_parse(val):
    if isinstance(val, str):
        try:
            return ast.literal_eval(val)
        except Exception:
            return val
    return val


def write_prediction(cmp, object_prop, object_vars, rdbms_connector, df):
    # df['term_id'] = object_prop['term_id']
    # df['loca_id'] = object_prop['loca_id']
    # df['grup_id'] = object_prop['grup_id']
    # df['turu_id'] = object_prop['turu_id']
    df['modl_id'] = object_prop['modl_id']

    grouped = df.groupby(['term_id', 'loca_id', 'grup_id', 'turu_id', 'modl_id']).apply(
        lambda g: pd.Series({
            'predict_false': g['item_id_list'].isna().sum(),
            'predict_true': g['item_id_list'].notna().sum(),
            'predict_total': g.shape[0],
            'predict_rate': round(g['item_id_list'].notna().sum() / g.shape[0] * 100, 2),  # (%)
            'predict_info': g[g['item_id_list'].notna()].assign(
                item_id_list=lambda x: x['item_id_list'].apply(safe_parse),
                basket_info=lambda x: x['basket_info'].apply(safe_parse)
            )  # predict_info
            [['basket_id', 'basket_info', 'item_id_list']].to_dict(orient='records')
        })).reset_index()

    auto.get_system_db().get_item_value_all(KeyEnum.SLOCA)
    auto.get_system_db().get_item_value_all(KeyEnum.SGRUP)
    auto.get_system_db().get_item_value_all(KeyEnum.SMODL)
    auto.get_system_db().get_item_value_all(KeyEnum.STURU)
    auto.get_system_db().get_item_value_all(KeyEnum.STERM)

    for _, row in grouped.iterrows():
        prm_id_val = str(uuid.uuid4())

        term_code = row['term_id']
        loca_code = f"TR-{row['loca_id']:02d}"
        grup_code = row['grup_id']
        turu_code = row['turu_id']
        modl_code = row['modl_id']

        auto.get_system_db().check_item_value(KeyEnum.STERM, term_code)
        auto.get_system_db().check_item_value(KeyEnum.SLOCA, loca_code)
        auto.get_system_db().check_item_value(KeyEnum.SGRUP, grup_code)
        auto.get_system_db().check_item_value(KeyEnum.STURU, turu_code)
        auto.get_system_db().check_item_value(KeyEnum.SMODL, modl_code)

        predict_false = row['predict_false']
        predict_true = row['predict_true']
        predict_total = row['predict_total']
        predict_rate = row['predict_rate']

        predict_info = row['predict_info']
        predict_info = PgJson(predict_info)

        insert_query = """
                       INSERT INTO srvdefinition.kesinti_predict(id,
                                                                 term_id, loca_id, grup_id, turu_id, modl_id,
                                                                 predict_false, predict_true, predict_total, predict_rate, predict_info)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) \
                       """
        params = (prm_id_val,
                  term_code, loca_code, grup_code, turu_code, modl_code,
                  predict_false, predict_true, predict_total, predict_rate, predict_info
                  )
        rdbms_connector.execute_update(insert_query, params)
    return object_vars
