import src.auto.lib.etl.impl_etl as LibEtl


def column_filter(cmp, object_prop, object_vars, df):
    columns_array = object_prop["columns_array"]
    df_select = LibEtl.column_filter(cmp=cmp, columns_array=columns_array, df=df)
    return object_vars, df_select


def column_rename(cmp, object_prop, object_vars, df):
    old_column_name = object_prop["old_column_name"]
    new_column_name = object_prop["new_column_name"]
    for i in range(len(object_prop["old_column_name"])):
        df = LibEtl.column_rename(cmp=cmp, df=df, new_column_name=new_column_name[i], old_column_name=old_column_name[i])
    return object_vars, df


def column_type_cast(cmp, object_prop, object_vars, df):
    columns = object_prop["columns"]
    target_types = object_prop["target_types"]
    df = LibEtl.column_type_cast(cmp=cmp, df=df, columns=columns, target_types=target_types)
    return object_vars, df


def constant_value(cmp, object_prop, object_vars, df):
    column_name = object_prop["column_name"]
    column_value = object_prop["column_value"]
    column_type = object_prop["column_type"]
    if object_prop["append"] == "True" and object_prop["column_name"] in df.columns:
        raise Exception("The column name appended exists in dataset. Please rename the specified column.")

    df = LibEtl.constant_value(cmp=cmp, column_type=column_type, column_value=column_value, df=df,
                               column_name=column_name)
    return object_vars, df


def data_head(cmp, object_prop, object_vars, df):
    size = object_prop["size"]
    df_out = LibEtl.data_head(cmp=cmp, df=df, size=size)
    return object_vars, df_out


def data_splitter(cmp, object_prop, object_vars, df):
    absolute_or_relative = object_prop["absolute_or_relative"]
    split_value = object_prop["split_value"]
    how = object_prop["how"]
    stratified_sampling_column = object_prop["stratified_sampling_column"]
    df_first, df_second = LibEtl.data_splitter(cmp=cmp, how=how, df=df, stratified_sampling_column=stratified_sampling_column,
                                               absolute_or_relative=absolute_or_relative, split_value=split_value)
    return object_vars, df_first, df_second


def data_sampler(cmp, object_prop, object_vars, df):
    absolute_or_relative = object_prop["absolute_or_relative"]
    proportion = object_prop["proportion"]
    how = object_prop["how"]
    stratified_sampling_column = object_prop["stratified_sampling_column"]
    df_first = LibEtl.data_sampler(cmp=cmp, how=how, df=df, stratified_sampling_column=stratified_sampling_column,
                                   absolute_or_relative=absolute_or_relative, proportion=proportion)
    return object_vars, df_first


def distinct(cmp, object_prop, object_vars, df):
    subset = object_prop["subset"]
    df_distinct = LibEtl.distinct(cmp=cmp, df=df, subset=subset)
    return object_vars, df_distinct


def group_by(cmp, object_prop, object_vars, df):
    agg_props = object_prop["agg_list"]
    column_props = object_prop["column_list"]
    dropna = object_prop["dropna"]
    by = object_prop["by"]
    df_groupby = LibEtl.group_by(cmp=cmp, agg_props=agg_props, column_props=column_props, by=by, df=df, dropna=dropna)
    return object_vars, df_groupby


def list_agg(cmp, object_prop, object_vars, df):
    column_props = object_prop["column"]
    dropna = object_prop["dropna"]
    by = object_prop["by"]
    df_groupby = LibEtl.list_agg(cmp=cmp, column_props=column_props, by=by, df=df, dropna=dropna)
    return object_vars, df_groupby


def join(cmp, object_prop, object_vars, df1, df2):
    how = object_prop["how"]
    left_on = object_prop["left_on"]
    right_on = object_prop["right_on"]
    df_join = LibEtl.join(cmp=cmp, df1=df1, df2=df2, how=how, left_on=left_on, right_on=right_on)
    return object_vars, df_join


def missing_value(cmp, object_prop, object_vars, df):
    # kolon bazlı operasyonlar
    for i in range(len(object_prop["column_name"])):
        if object_prop["operation"][i] == "Do Nothing":
            continue
        else:
            LibEtl.missing_value_operation(cmp=cmp, df=df,
                                           column_name=object_prop["column_name"][i],
                                           operation=object_prop["operation"][i],
                                           constant_value=object_prop["fix_value"][i],
                                           window=object_prop["moving_average_window"][i])
    return object_vars, df


def normalizer(cmp, object_prop, object_vars, df):
    normalizer = object_prop["normalizer"]
    range = object_prop["feature_range"]
    columns = object_prop["columns"]
    model, df = LibEtl.normalized(cmp=cmp, df=df, normalizer=normalizer, range=range, columns=columns)
    return object_vars, model, df


def row_filter(cmp, object_prop, object_vars, df):
    query_expr = object_prop["query_expr"]
    df_qry = LibEtl.row_filter(cmp, df, query_expr, df_id="df")
    df_qry_else = LibEtl.row_filter(cmp=cmp, df=df, query_expr=query_expr, df_id="df", inverse=True)
    return object_vars, df_qry, df_qry_else


def rule_engine(cmp, object_prop, object_vars, df):
    append_column = object_prop["append_column"]
    column_name = object_prop["column_name"]

    condition_list = object_prop["condition_list"]
    value_list = object_prop["value_list"]
    default_value = object_prop["default_value"]
    df = LibEtl.rule_engine(cmp=cmp, append_column=append_column, column_name=column_name,
                            condition_list=condition_list, default_value=default_value,
                            df=df, value_list=value_list)
    return object_vars, df


def sorter(cmp, object_prop, object_vars, df):
    by = object_prop["by"]
    ascending = object_prop["ascending"]
    df_sorted = LibEtl.sorter(cmp=cmp, ascending=ascending, by=by, df=df)
    return object_vars, df_sorted


def standardization(cmp, object_prop, object_vars, df):
    df, model = LibEtl.standardization(cmp=cmp, df=df)
    return object_vars, df, model


def string_replacer(cmp, object_prop, object_vars, df):
    append = object_prop["append"]
    append_column_name = object_prop["append_column_name"]
    target_column_name = object_prop["target_column_name"]
    pattern = object_prop["pattern"]
    replace_text = object_prop["replace_text"]
    df = LibEtl.string_replacer(cmp=cmp, append=append, append_column_name=append_column_name, df=df,
                                pattern=pattern, replace_text=replace_text, target_column_name=target_column_name)
    return object_vars, df


def union(cmp, object_prop, object_vars, **dfx):
    df_union = LibEtl.union(cmp=cmp, **dfx)
    return object_vars, df_union

def window_function(cmp, object_prop, object_vars, df):
    operation = object_prop["operation"]
    partition_by = object_prop["partition_by"]
    order_by = object_prop["order_by"]
    column_name = order_by = object_prop["column_name"]
    shift_value = object_prop["shift_value"]
    df_window = LibEtl.window_function(cmp=cmp, df=df, column_name=column_name, partition_by=partition_by,
                                       order_by=order_by, operation=operation, shift_value=shift_value)
    return object_vars, df_window


def column_expression(cmp, object_prop, object_vars, df):
    df = LibEtl.column_expression(cmp=cmp, df=df, object_prop=object_prop)
    return object_vars, df


def denormalizer(cmp, object_prop, object_vars, df, model):
    denormalize = object_prop["denormalizer"]
    columns = object_prop["columns"]
    df = LibEtl.denormalizer(cmp=cmp, df=df, model=model, denormalize=denormalize, columns=columns)
    return object_vars, df


def equal_size_data_sampler(cmp, object_prop, object_vars, df):
    column_name = object_prop["column_name"]
    df = LibEtl.equal_size_data_sampler(cmp=cmp, df=df, column_name=column_name)
    return object_vars, df
