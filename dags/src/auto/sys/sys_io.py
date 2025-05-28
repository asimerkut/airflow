import io
import os
import shutil
from pathlib import Path

import pandas as pd

import src.auto.env as env
from src.auto.sys import sys_io_s3
from src.auto.util.enum_util import FsTypeEnum


def get_flow_path(flow_id):
    flow_path = env.PRM_FLOW_PATH + "/" + str(flow_id).replace("-", "")
    if env.fs_type == FsTypeEnum.lfs and not os.path.exists(flow_path):
        os.makedirs(flow_path, 0o777, exist_ok=True)
    return flow_path


def get_node_path(flow_id, node_no):
    flow_path = get_flow_path(flow_id)
    node_path = flow_path + "/" + str(node_no)
    if env.fs_type == FsTypeEnum.lfs and not os.path.exists(node_path):
        os.makedirs(node_path, 0o777, exist_ok=True)
    return node_path


def write_file_txt(file_name, data, mode="w"):
    if env.fs_type == FsTypeEnum.s3fs:
        file_obj = io.BytesIO(data.encode("utf-8"))
        sys_io_s3.write_file_s3(file_obj, file_name)
    else:
        os.makedirs(os.path.dirname(file_name), 0o777, exist_ok=True)
        with open(file_name, f"{mode}t") as txt_file:  # pylint: disable=W1514
            txt_file.write(data)


def write_file_blob(file_name, data, mode="w"):
    if env.fs_type == FsTypeEnum.s3fs:
        file_obj = io.BytesIO(data)
        sys_io_s3.write_file_s3(file_obj, file_name)
    else:
        os.makedirs(os.path.dirname(file_name), 0o777, exist_ok=True)
        with open(file_name, f"{mode}t") as txt_file:  # pylint: disable=W1514
            txt_file.write(data)


def write_file_df(file_name, data):
    buffer = io.BytesIO()
    data.to_parquet(buffer, index=False)
    if env.fs_type == FsTypeEnum.s3fs:
        buffer.seek(0)
        sys_io_s3.write_file_s3(buffer, file_name)
    else:
        os.makedirs(os.path.dirname(file_name), 0o777, exist_ok=True)
        buffer.seek(0)
        with open(file_name, "wb") as f:
            f.write(buffer.getvalue())


def delete_path(path):
    if env.fs_type == FsTypeEnum.s3fs:
        sys_io_s3.delete_path_s3(path)
    else:
        if os.path.exists(path):
            shutil.rmtree(path)


def read_file_txt(path):
    result = ""
    if env.fs_type == FsTypeEnum.s3fs:
        result = sys_io_s3.read_file_txt_s3(path)
    else:
        with open(path, "rt", encoding="utf-8") as txt:
            result = txt.read()
    return result



def read_file_bin(path):
    blob_data = None
    if env.fs_type == FsTypeEnum.s3fs:
        blob_data = sys_io_s3.read_file_bin_s3(path)
    else:
        with open(path, 'rb') as f:
            blob_data = f.read()
    return blob_data


def read_file_df(path):
    pdf = None
    if env.fs_type == FsTypeEnum.s3fs:
        pdf = sys_io_s3.read_file_df_s3(path)
    else:
        if Path(path).exists():
            pdf = pd.read_parquet(path)
            print("File Exists: " + path)
        else:
            print("File NotFound: " + path)
    return pdf
