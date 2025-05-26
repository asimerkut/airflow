from fractions import Fraction

import numpy as np
import pandas as pd
from sklearn import preprocessing

import src.auto.util.sp_to_pd as b
from src.auto.util import static_util
from src.auto.util.sp_converter import spark_to_pandas_filter


def join(cmp, df1, df2, how, left_on, right_on):
    df_join = pd.merge(df1, df2, how=how, left_on=left_on, right_on=right_on,suffixes=("__t1", "__t2"))
    return df_join


def column_filter(cmp, columns_array, df):
    df_select = df[columns_array]
    return df_select


def column_rename(cmp, df, new_column_name, old_column_name):
    df_renamed_col = df.rename(columns={old_column_name: new_column_name})
    return df_renamed_col


def column_type_cast(cmp, df, columns, target_types):
    pandas_types = []
    for t in target_types:
        if t in static_util.spark_pandas_type_cast:
            t = static_util.spark_pandas_type_cast[t]
        pandas_types.append(t)

    for i in range(len(columns)):
        df[columns[i]] = df[columns[i]].astype(pandas_types[i])
    return df


def constant_value(cmp, column_name, column_value, column_type, df):
    df[column_name] = column_value
    df_constant_value = column_type_cast(cmp=cmp, df=df, columns=[column_name], target_types=[column_type])
    return df_constant_value


def data_head(cmp, df, size):
    return df.iloc[:size, ]


def normalized(cmp, df, normalizer, range, columns):
    if normalizer == 'Min-Max Normalization':
        min_max_scaler = preprocessing.MinMaxScaler(feature_range=tuple(range))
        normalized_values = min_max_scaler.fit_transform(df[columns])
        df[columns] = normalized_values
        m = min_max_scaler
    elif normalizer == 'Z-Score Normalization':
        standard_scaler = preprocessing.StandardScaler()
        normalized_values = standard_scaler.fit_transform(df[columns])
        df[columns] = normalized_values
        m = standard_scaler
    else:
        raise Exception('unknown normalizer: ' + normalizer)

    if isinstance(df, np.ndarray):
        df = pd.DataFrame(df)
    return m, df


def denormalized(cmp, df, model, denormalize, columns):
    if (denormalize == 'Min-Max Normalization') | (denormalize == 'Z-Score Normalization'):
        df[columns] = model.inverse_transform(df[columns])
    else:
        raise Exception('unknown denormalizer ' + denormalize)
    if isinstance(df, np.ndarray):
        df = pd.DataFrame(df)
    return df


def distinct(cmp, df=None, subset=None):
    df_distinct = df.drop_duplicates(subset=subset, keep="first")
    return df_distinct


def group_by(cmp, agg_props, column_props, by, df, dropna):
    agg_params = {}
    pair_zip = zip(column_props, agg_props)
    for column, agg in pair_zip:
        agg_params["_".join([agg, column])] = (column, agg)
    df_groupby = df.groupby(by=by, dropna=dropna).agg(**agg_params)

    df_groupby = df_groupby.reset_index()
    return df_groupby


def list_agg(cmp, column_props, by, df, dropna):
    df_groupby = df.groupby(by=by, dropna=dropna)[column_props].agg(list).reset_index().rename(
        columns={column_props: column_props + '_list'})
    return df_groupby


def window_function(cmp, df, column_name, partition_by, order_by, operation, shift_value):
    pd.set_option('display.max_columns', None)

    if partition_by is None:
        df['_partition'] = 1
        partition_by = '_partition'

    if order_by is None:
        order_by = partition_by

    groups = df.sort_values(by=order_by).groupby(partition_by)

    if operation == 'rank':
        df['Result'] = groups[column_name].rank(method='min').astype("int")
    elif operation == 'dense_rank':
        df['Result'] = groups[column_name].rank(method='dense').astype("int")
    elif operation == 'sum':
        df['Result'] = groups[column_name].transform('sum')
    elif operation == 'count':
        df['Result'] = groups[column_name].transform('count')
    elif operation == 'min':
        df['Result'] = groups[column_name].transform('min')
    elif operation == 'max':
        df['Result'] = groups[column_name].transform('max')
    elif operation == 'lead':
        df['Result'] = groups[column_name].shift(-shift_value)
    elif operation == 'lag':
        df['Result'] = groups[column_name].shift(shift_value)
    elif operation == 'cumsum':
        df['Result'] = groups[column_name].cumsum()
    elif operation == 'cumprod':
        df['Result'] = groups[column_name].cumprod()
    elif operation == 'cummin':
        df['Result'] = groups[column_name].cummin()
    elif operation == 'cummax':
        df['Result'] = groups[column_name].cummax()
    elif operation == 'rolling_mean':
        df['Result'] = groups[column_name].rolling(window=shift_value, min_periods=0).mean().reset_index(drop=True)
    elif operation == 'expanding_sum':
        df['Result'] = groups[column_name].expanding().sum().reset_index(drop=True)
    elif operation == 'ewm_mean':
        df['Result'] = groups[column_name].ewm(span=3, min_periods=1).mean().reset_index(drop=True)

    del groups
    return df


def standardization(cmp, df):
    standard_scaler_model = preprocessing.StandardScaler()
    df = standard_scaler_model.fit_transform(df)
    return df, standard_scaler_model


def row_filter(cmp, df, query_expr, df_id="df", inverse=False):
    pandas_exr = spark_to_pandas_filter(query_expr, df_id)
    if inverse:
        df_qry = df[~eval(pandas_exr)]
    else:
        df_qry = df[eval(pandas_exr)]
    return df_qry.reset_index().drop(['index'], axis=1)


def sorter(cmp, ascending, by, df):
    kind = "quicksort"
    na_position = "first"
    df_sorted = df.sort_values(by=by, ascending=ascending, kind=kind, na_position=na_position)
    return df_sorted.reset_index().drop(['index'], axis=1)


def string_replacer(cmp, append, append_column_name, df, pattern, replace_text, target_column_name):
    if append == True:
        if append_column_name in df.columns:
            raise Exception("The column name appended exists in dataset. Please rename the specified column.")

        df[append_column_name] = df[target_column_name].replace(
            pattern, replace_text)
    else:
        df[target_column_name] = df[target_column_name].replace(
            pattern, replace_text)
    return df


def union(cmp, **dfx):
    dfs = list(dfx.values())
    col_ref = dfs[0].columns
    for i, df in enumerate(dfs[1:], start=2):
        if len(df.columns) != len(col_ref):
            raise Exception(f"{i}. DataFrame kolon sayısı eşit değil! {df.columns} != {col_ref}")
    df_union = pd.concat(dfs, axis=0, ignore_index=True)
    return df_union

def missing_value_operation(cmp, df, column_name, operation, constant_value, window=None):
    column_type = df[column_name].dtype

    if np.issubdtype(column_type, np.number):
        if operation == "Fix Value":
            df[column_name].fillna(value=constant_value, inplace=True)
        elif operation == "Remove Row":
            df.dropna(subset=[column_name], inplace=True)
            df.reset_index(drop=True, inplace=True)  ## id resetlenmiyor -> assertion Error
        elif operation == "Most Frequent Value":
            df[column_name].fillna(df[column_name].value_counts().idxmax(), inplace=True)
        elif operation == "Next Value":
            df[column_name].fillna(df[column_name].bfill(), inplace=True)
        elif operation == "Previous Value":
            df[column_name].fillna(df[column_name].ffill(), inplace=True)
        elif operation == "Average Interpolation":
            df.fillna(df.interpolate(method='linear', axis=0).mean().round(1), inplace=True)
        elif operation == "Linear Interpolation":
            df.fillna(df.interpolate(method='linear', axis=0).mean().round(1), inplace=True)
        elif operation == "Maximum":
            df[column_name].fillna(df[column_name].max(), inplace=True)
        elif operation == "Mean":
            df[column_name].fillna(df[column_name].mean(), inplace=True)
        elif operation == "Median":
            df[column_name].fillna(df[column_name].median(), inplace=True)
        elif operation == "Minimum":
            df[column_name].fillna(df[column_name].min(), inplace=True)
        elif operation == "Moving Average":
            df[column_name] = df[column_name].rolling(window=window, min_periods=1).mean().round(1)
        elif operation == "Rounded Mean":
            df[column_name].fillna(df[column_name].mean().round(2), inplace=True)

    else:
        if operation == "Fix Value":
            df[column_name].fillna(value=constant_value, inplace=True)
        elif operation == "Remove Row":
            df.dropna(subset=[column_name], inplace=True)
            df.reset_index(drop=True, inplace=True)
        elif operation == "Most Frequent Value":
            df[column_name].fillna(df[column_name].value_counts().idxmax(), inplace=True)
        elif operation == "Next Value":
            df[column_name].fillna(method='pad', inplace=True)
        elif operation == "Previous Value":
            df[column_name].fillna(method='bfill', inplace=True)
        elif operation == "Maximum":
            raise ValueError("Operation 'Maximum' is not supported for non-numeric columns.")
        elif operation == "Mean":
            raise ValueError("Operation 'Mean' is not supported for non-numeric columns.")
        elif operation == "Median":
            raise ValueError("Operation 'Median' is not supported for non-numeric columns.")
        elif operation == "Minimum":
            raise ValueError("Operation 'Minimum' is not supported for non-numeric columns.")
        elif operation == "Moving Average":
            raise ValueError("Operation 'Moving Average' is not supported for non-numeric columns.")
        elif operation == "Rounded Mean":
            raise ValueError("Operation 'Rounded Mean' is not supported for non-numeric columns.")

    return df


def data_splitter(cmp, how, df, stratified_sampling_column, absolute_or_relative, split_value):
    def take_from_top(df, absolute_or_relative, split_value):
        df_first, df_second, split_pos = None, None, None
        if absolute_or_relative == "absolute":
            split_pos = split_value
        elif absolute_or_relative == "relative":
            split_pos = len(df) * (split_value / 100)

        split_pos = int(split_pos)

        df_first = df.iloc[:split_pos, ]
        df_second = df.iloc[split_pos:, ]
        return (df_first, df_second)

    def linear_sampling(df, absolute_or_relative, split_value):
        df_first, df_second = None, None
        total = len(df)
        fraction_value = 0.0
        if absolute_or_relative == "absolute":
            fraction_value = total / split_value
        elif absolute_or_relative == "relative":
            fraction_value = total / (total * (split_value / 100))

        f = Fraction(fraction_value)
        numerator = f.numerator
        denomintator = f.denominator

        columns = df.columns
        first_data = []
        second_data = []

        for index, row in df.iterrows():
            value = index % (numerator + denomintator)
            if value < numerator:
                first_data.append(list(row))
            else:
                second_data.append(list(row))
        df_first = pd.DataFrame(columns=columns, data=first_data)
        df_second = pd.DataFrame(columns=columns, data=second_data)
        return (df_first, df_second)

    def randomly(df, absolute_or_relative, split_value):
        shuffled = df.sample(frac=1).reset_index()

        shuffled = shuffled[df.columns.tolist()]
        result = take_from_top(shuffled, absolute_or_relative, split_value)

        return result

    def stratified_sampling(df, stratified_sampling_column, absolute_or_relative, split_value):
        unique_values = df[stratified_sampling_column].unique().tolist()
        df_first_list = []
        df_seconds_list = []

        for value in unique_values:
            df_temp = df[df[stratified_sampling_column] == value]
            df_first, df_second = take_from_top(df_temp, absolute_or_relative, split_value)
            df_first_list.append(df_first)
            df_seconds_list.append(df_second)

        df_first_result = pd.concat(df_first_list)
        df_second_result = pd.concat(df_seconds_list)

        return (df_first_result, df_second_result)

    df_first, df_second = None, None
    if how == "take_from_top":
        df_first, df_second = take_from_top(df, absolute_or_relative, split_value)
    elif how == "linear_sampling":
        df_first, df_second = linear_sampling(df, absolute_or_relative, split_value)
    elif how == "randomly":
        df_first, df_second = randomly(df, absolute_or_relative, split_value)
    elif how == "stratified_sampling":
        df_first, df_second = stratified_sampling(df, stratified_sampling_column, absolute_or_relative,
                                                  split_value)
    else:
        raise Exception("unknown yöntem:" + how)

    return df_first.reset_index().drop(['index'], axis=1), df_second.reset_index().drop(['index'], axis=1)


def data_sampler(cmp, how, df, stratified_sampling_column, absolute_or_relative, proportion):
    df_first, df_second = data_splitter(cmp=cmp, how=how, df=df,
                                        stratified_sampling_column=stratified_sampling_column,
                                        absolute_or_relative=absolute_or_relative, split_value=proportion)
    del df_second
    return df_first


def rule_engine(cmp, append_column, column_name, condition_list, default_value, df, value_list):
    b.df_current = df
    if not append_column and column_name in list(df.columns):
        raise Exception("eklenmek istenen kolon zaten var!")
    for condition, value in zip(condition_list, value_list):
        pandas_condition = spark_to_pandas_filter(condition, "df")
        result = eval(pandas_condition)
        value_result = eval(value)
        df.loc[result, column_name] = value_result
    if default_value is not None:
        df[column_name].fillna(eval(default_value), inplace=True)
    return df


def column_expression(cmp, df, object_prop):
    b.df_current = df

    column_expression = object_prop["column_expression"]
    replace_column = object_prop["replace_column"]
    output_column_name = object_prop["output_column_name"]

    for index, item in enumerate(column_expression):

        df_temp = eval(spark_to_pandas_filter(column_expression[index], "df"))

        if (output_column_name[index] == None) | (output_column_name[index] == ''):
            df = df_temp
        else:
            if replace_column[index] == True:
                if output_column_name[index] in df.columns:
                    df[output_column_name[index]] = df_temp
                else:
                    raise Exception(
                        "The column name appended not exists in dataset. Please rename the specified column.")

            else:
                if output_column_name[index] in df.columns:
                    raise Exception(
                        "The column name appended exists in dataset. Please rename the specified column.")

                else:
                    df[output_column_name[index]] = df_temp

    return df[object_prop["output_column_name"]]


def denormalizer(cmp, df, model, denormalize, columns):
    df = denormalized(cmp, df, model, denormalize, columns)
    return df


def equal_size_data_sampler(cmp, df, column_name):
    min_count = df[column_name].value_counts().min()
    balanced_df = df.groupby(column_name).head(min_count)
    return balanced_df
