import io
import os
import shutil
from pathlib import Path

import pickle

import src.auto.env as env
from src.auto.sys import sys_io_s3
from src.auto.util.enum_util import FsTypeEnum
import pyarrow.parquet as pq
import json

from src.auto.util.numpy_util import NumpyEncoder


def get_flow_path(flow_id):
    flow_path = env.PRM_FLOW_PATH + "/" + str(flow_id).replace("-", "")
    if env.fs_type == FsTypeEnum.lfs and not os.path.exists(flow_path):
        os.makedirs(flow_path, 0o777, exist_ok=True)
    return flow_path


def get_node_path(flow_id, node_no: str):
    flow_path = get_flow_path(flow_id)
    node_path = flow_path + "/" + str(node_no)
    if env.fs_type == FsTypeEnum.lfs and not os.path.exists(node_path):
        os.makedirs(node_path, 0o777, exist_ok=True)
    return node_path


def write_file_txt(file_name, data, mode="wb"):
    try:
        if isinstance(data, str):
            data = data.encode("utf-8")
        elif isinstance(data, dict):
            data = json.dumps(data, cls=NumpyEncoder, indent=2)
            data = data.encode('utf-8')

        if env.fs_type == FsTypeEnum.s3fs:
            file_obj = io.BytesIO(data)
            sys_io_s3.write_file_s3(file_obj, file_name)
        else:
            os.makedirs(os.path.dirname(file_name), 0o777, exist_ok=True)
            with open(file_name, mode) as txt_file:  # pylint: disable=W1514
                txt_file.write(data)
    except Exception as e:
        print(f"Error writing file {file_name}: {e}")
        raise e


def write_file_blob(file_name, data, mode="wb"):
    if env.fs_type == FsTypeEnum.s3fs:
        file_obj = io.BytesIO(data)
        sys_io_s3.write_file_s3(file_obj, file_name)
    else:
        os.makedirs(os.path.dirname(file_name), 0o777, exist_ok=True)
        with open(file_name, mode) as bin_file:  # pylint: disable=W1514
            bin_file.write(data)


def write_file_df(file_name, data):
    buffer = io.BytesIO()
    data.to_parquet(buffer, index=False, engine="pyarrow", row_group_size=env.page_size)
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
    try:
        result = ""
        if env.fs_type == FsTypeEnum.s3fs:
            result = sys_io_s3.read_file_txt_s3(path)
        else:
            with open(path, "rb") as txt:
                result = txt.read()
        return result
    except Exception as e:
        print(f"Error reading file {path}: {e}")
        raise e



def read_file_bin(path):
    blob_data = None
    if env.fs_type == FsTypeEnum.s3fs:
        blob_data = sys_io_s3.read_file_bin_s3(path)
    else:
        with open(path, 'rb') as bin:
            blob_data = bin.read()
    return blob_data


def read_file_df(path, page_number=None):
    pdf = None
    if env.fs_type == FsTypeEnum.s3fs:
        pdf = sys_io_s3.read_file_df_s3(path, page_number)
    else:
        if Path(path).exists():
            pf = pq.ParquetFile(path)
            if page_number is None:
                table = pf.read()
            else:
                table = pf.read_row_group(page_number-1)
            pdf = table.to_pandas()
            print("File Exists: " + path)
        else:
            print("File NotFound: " + path)
    return pdf
