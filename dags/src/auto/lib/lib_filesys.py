import os
import shutil

import pandas as pd


def copy(cmp, object_prop, object_vars, filesys_connector, df):
    source = object_prop["source"]
    dest = object_prop["dest"]
    overwrite = object_prop["overwrite"]

    if not filesys_connector.exists(source):
        raise Exception("dosya veya dizin yok! :" + source)

    if filesys_connector.exists(dest) and not overwrite:
        raise Exception("var olan dosya üzerine yazılamaz! " + dest)

    filesys_connector.to_dictionary(source=source, dest=dest)

    fs_data = pd.DataFrame(columns=["source", "dest"], data=[[source, dest]])
    return object_vars, df, fs_data


def create_folder(cmp, object_prop, object_vars, filesys_connector, df):
    parent = object_prop["parent"]
    name = object_prop["name"]
    path = "/".join([parent, name])
    permission = object_prop["permission"]
    exist_ok = object_prop["exist_ok"]

    if not filesys_connector.exists(parent):
        raise Exception("dizin yok! :" + parent)

    if filesys_connector.exists(path):
        raise Exception("zaten var")

    filesys_connector.mkdirs(path, permission=permission, exist_ok=exist_ok)
    fs_data = pd.DataFrame(columns=["path"], data=[[path]])
    return object_vars, df, fs_data


def create_temp_folder(cmp, object_prop, object_vars, filesys_connector, df):
    parent = object_prop["parent"]
    prefix = object_prop["prefix"]
    suffix = object_prop["suffix"]

    if not filesys_connector.exists(parent):
        raise Exception("dizin yok! :" + parent)

    temp_dir_path = filesys_connector.create_temp_directory(prefix=prefix, suffix=suffix,
                                                            directory=parent)
    fs_data = pd.DataFrame(columns=["temp_dir_path"], data=[[temp_dir_path]])
    return object_vars, df, fs_data


def delete_file_folder(cmp, object_prop, object_vars, filesys_connector, df):
    path = object_prop["path"]
    recursive = object_prop["recursive"]
    skip_trash = object_prop["skip_trash"]

    if not filesys_connector.exists(path):
        raise Exception("dosya/dizin yok! :" + path)
    if os.path.exists(path):
        shutil.rmtree(path)

    fs_data = pd.DataFrame(columns=["path"], data=[[path]])
    return object_vars, df, fs_data


def exists(cmp, object_prop, object_vars, filesys_connector, df):
    path = object_prop["path"]
    fs_data = pd.DataFrame(columns=["path"], data=[[filesys_connector.exists(path)]])
    return object_vars, df, fs_data


def file_info(cmp, object_prop, object_vars, filesys_connector, df):
    path = object_prop["path"]
    if not filesys_connector.exists(path):
        raise Exception("dosya/dizin yok! :" + path)
    file_info_dict = filesys_connector.get_file_info(path)
    fs_data = pd.DataFrame(file_info_dict, index=[0])
    return object_vars, df, fs_data


def get_hash_code(cmp, object_prop, object_vars, filesys_connector, df):
    path = object_prop["path"]
    _type = object_prop["type"]
    block_size = object_prop["block_size"]

    if not filesys_connector.exists(path):
        raise Exception("dosya/dizin yok! :" + path)

    hash_code = filesys_connector.get_hash_code(path=path, type=_type, block_size=block_size)
    fs_data = pd.DataFrame(columns=["hash_code"], data=[[hash_code]])
    return object_vars, df, fs_data


def get_permissions(cmp, object_prop, object_vars, filesys_connector, df):
    path = object_prop["path"]

    if not filesys_connector.exists(path):
        raise Exception("dosya/dizin yok! :" + path)

    permission = filesys_connector.get_permissions(path)
    fs_data = pd.DataFrame(columns=["permission"], data=[[permission]])
    return object_vars, df, fs_data


def list_files(cmp, object_prop, object_vars, filesys_connector, df):
    path = object_prop["path"]
    pattern = object_prop["pattern"]

    if not filesys_connector.exists(path):
        raise Exception("dosya/dizin yok! :" + path)

    if pattern is None:
        pattern = "*"

    list_file_info_dict = filesys_connector.list_file_info(f"{path}/{pattern}")

    data = []
    columns = list(list_file_info_dict[0].keys())
    for file_inf in list_file_info_dict:
        row = []
        data.append(row)
        for c in columns:
            row.append(file_inf[c])

    fs_data = pd.DataFrame(columns=columns, data=data)
    return object_vars, df, fs_data


def move(cmp, object_prop, object_vars, filesys_connector, df):
    source = object_prop["source"]
    dest = object_prop["dest"]
    overwrite = object_prop["overwrite"]

    if not filesys_connector.exists(source):
        raise Exception("dosya veya dizin yok! :" + source)

    if filesys_connector.exists(dest) and not overwrite:
        raise Exception("var olan dosya üzerine yazılamaz! " + dest)

    filesys_connector.move(source=source, dest=dest)

    fs_data = pd.DataFrame(columns=["source", "dest"], data=[[source, dest]])
    return object_vars, df, fs_data


def read_binary(cmp, object_prop, object_vars, filesys_connector):
    path = object_prop["path"]

    if not filesys_connector.exists(path):
        raise Exception("dosya veya dizin yok! :" + path)

    data = filesys_connector.read_binary(path)
    return object_vars, data


def read_csv(cmp, object_prop, object_vars, filesys_connector):
    path = object_prop["path"]
    if filesys_connector == None:
        raise Exception("connector tanimli degil!")
    if path == None:
        raise Exception("path tanimsiz!")
    if not filesys_connector.exists(path):
        raise Exception("dosya veya folder bulunmadi! :" + path)
    header = object_prop["header"]
    encoding = object_prop["encoding"]

    df = filesys_connector.read_csv(path=path, header=header, encoding=encoding)
    return object_vars, df


def read_excel(cmp, object_prop, object_vars, filesys_connector):
    path = object_prop["path"]
    sheet_name = object_prop["sheet_name"]

    if not filesys_connector.exists(path):
        raise Exception("dosya veya dizin yok! :" + path)

    df = filesys_connector.read_excel(path, sheet_name=sheet_name, engine='openpyxl')
    return object_vars, df


def read_json(cmp, object_prop, object_vars, filesys_connector):
    path = object_prop["path"]
    orient = object_prop["orient"]

    if not filesys_connector.exists(path):
        raise Exception("dosya veya dizin yok! :" + path)

    df = filesys_connector.read_json(path, orient=orient)
    return object_vars, df


def read_parquet(cmp, object_prop, object_vars, filesys_connector):
    path = object_prop["path"]

    if not filesys_connector.exists(path):
        raise Exception("dosya veya dizin yok! :" + path)

    df = filesys_connector.read_parquet(path)
    return object_vars, df


def read_svg(cmp, object_prop, object_vars, filesys_connector):
    path = object_prop["path"]

    if not filesys_connector.exists(path):
        raise Exception("dosya veya dizin yok! :" + path)

    svg_text = filesys_connector.read_txt(path)
    return object_vars, svg_text


def read_txt(cmp, object_prop, object_vars, filesys_connector):
    path = object_prop["path"]

    if not filesys_connector.exists(path):
        raise Exception("dosya veya dizin yok! :" + path)

    data_txt = filesys_connector.read_txt(path)
    return object_vars, data_txt


def read_xml(cmp, object_prop, object_vars, filesys_connector):
    path = object_prop["path"]

    if not filesys_connector.exists(path):
        raise Exception("dosya veya dizin yok! :" + path)

    df = filesys_connector.read_xml(path)
    return object_vars, df


def set_permissions(cmp, object_prop, object_vars, filesys_connector, df):
    path = object_prop["path"]
    mode = object_prop["mode"]

    if not filesys_connector.exists(path):
        raise Exception("dosya/dizin yok! :" + path)

    filesys_connector.set_permissions(path, mode)
    fs_data = pd.DataFrame(columns=["mode"], data=[[mode]])
    return object_vars, df, fs_data


def un_zip(cmp, object_prop, object_vars, filesys_connector, df):
    source = object_prop["source"]
    dest = object_prop["dest"]
    overwrite = object_prop["overwrite"]

    if not filesys_connector.exists(source):
        raise Exception("dosya veya dizin yok! :" + source)

    if filesys_connector.exists(dest) and not overwrite:
        raise Exception("var olan dosya üzerine yazılamaz! " + dest)

    filesys_connector.unzip(source=source, dest=dest)

    fs_data = pd.DataFrame(columns=["source", "dest"], data=[[source, dest]])
    return object_vars, df, fs_data


def write_binary(cmp, object_prop, object_vars, filesys_connector, df):
    path = object_prop["path"]

    if (filesys_connector.exists(path) and object_prop["if-file-exists"] == "fail"):
        raise Exception("dosya zaten var!")

    filesys_connector.write_bin(path=path, dataframe=df)
    return object_vars


def write_csv(cmp, object_prop, object_vars, filesys_connector, df):
    path = object_prop["path"]

    if (filesys_connector.exists(path) and object_prop["if-file-exists"] == "fail"):
        raise Exception("dosya zaten var!")

    filesys_connector.write_csv(path=path, dataframe=df)
    return object_vars


def write_excel(cmp, object_prop, object_vars, filesys_connector, df):
    path = object_prop["path"]

    if (filesys_connector.exists(path) and object_prop["if-file-exists"] == "fail"):
        raise Exception("dosya zaten var!")

    filesys_connector.write_excel(path=path, dataframe=df, sheet_name=object_prop["sheet_name"])
    return object_vars


def write_json(cmp, object_prop, object_vars, filesys_connector, df):
    path = object_prop["path"]
    columns = object_prop["column_names"]
    orient = object_prop["orient"]

    if (filesys_connector.exists(path) and object_prop["if-file-exists"] == "fail"):
        raise Exception("dosya zaten var!")

    filesys_connector.write_json(path=path, dataframe=df[columns], orient=orient)
    return object_vars


def write_parquet(cmp, object_prop, object_vars, filesys_connector, df):
    path = object_prop["path"]

    if (filesys_connector.exists(path) and object_prop["if-file-exists"] == "fail"):
        raise Exception("dosya zaten var!")

    filesys_connector.write_parquet(path=path, dataframe=df)
    return object_vars


def write_svg(cmp, object_prop, object_vars, filesys_connector, data):
    path = object_prop["path"]

    if (filesys_connector.exists(path) and object_prop["if-file-exists"] == "fail"):
        raise Exception("dosya zaten var!")

    filesys_connector.write_bin(path=path, data=data)
    return object_vars


def write_txt(cmp, object_prop, object_vars, filesys_connector, df):
    path = object_prop["path"]
    text_header = object_prop["text_header"]
    text_description = object_prop["text_description"]
    text_summary = object_prop["text_summary"]
    html = object_prop["html"]
    encoding_ = object_prop["encoding"]

    if (filesys_connector.exists(path) and object_prop["if-file-exists"] == "fail"):
        raise Exception("dosya zaten var!")

    text_data = ""
    if html:
        text_data = df.to_html(buf=path, encoding=encoding_)
    else:
        text_data = df.to_string(buf=path, encoding=encoding_)

    result_text = ""
    if html:
        result_text = f"<html><h3>{text_header}</h3><p>{text_description}</p><p>{text_data}</p><p>{text_summary}</p>"
    else:
        result_text = f"{text_header}\n\r{text_description}\n\r{text_data}\n\r{text_summary}"

    filesys_connector.write_txt(path=path, data=result_text)
    return object_vars


def write_xml(cmp, object_prop, object_vars, filesys_connector, df):
    path = object_prop["path"]
    columns = object_prop["column_names"]

    if (filesys_connector.exists(path) and object_prop["if-file-exists"] == "fail"):
        raise Exception("dosya zaten var!")

    filesys_connector.write_xml(path=path, dataframe=df[columns])
    return object_vars


def create_zip(cmp, object_prop, object_vars, filesys_connector, df):
    source = object_prop["source"]
    dest = object_prop["dest"]
    overwrite = object_prop["overwrite"]

    if not filesys_connector.exists(source):
        raise Exception("dosya veya dizin yok! :" + source)

    if filesys_connector.exists(dest) and not overwrite:
        raise Exception("var olan dosya üzerine yazılamaz! " + dest)

    filesys_connector.zip(source=source, dest=dest)

    fs_data = pd.DataFrame(columns=["source", "dest"], data=[[source, dest]])
    return object_vars, df, fs_data
