import pandas as pd

from src.auto.util.enum_util import LibEnum


def lib_class():
    return LibEnum.LibIoUser


def table_creator(cmp, object_prop, object_vars):
    df1 = pd.DataFrame(columns=object_prop["column_names"], data=object_prop["table_data"])
    for i, column in enumerate(df1.columns):
        df1[column].astype(object_prop["column_types"][i])
    return object_vars, df1


def user_test(cmp, object_prop, object_vars):
    return object_vars
