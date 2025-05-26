import io

import boto3
import pandas as pd

import dags.auto.env as env
from dags.auto.util.enum_util import FsTypeEnum

s3 = None
if env.fs_type == FsTypeEnum.s3fs:
    s3 = boto3.resource(
        "s3",
        endpoint_url=env.s3.get("endpoint_url"),
        aws_access_key_id=env.s3.get("aws_access_key_id"),
        aws_secret_access_key=env.s3.get("aws_secret_access_key")
    )


def write_file_s3(file_obj, path):
    try:
        file_obj.seek(0)
        bucket = s3.Bucket(env.s3_bucket_name)
        bucket.upload_fileobj(file_obj, path)
    except Exception as e:
        print(e)
        pass


def delete_path_s3(path):
    try:
        bucket = s3.Bucket(env.s3_bucket_name)
        path = path.lstrip("/").rstrip("/")
        obj_list = list(bucket.objects.filter(Prefix=path))
        for obj in obj_list:
            print(f"Deleting: {obj.key}")
            obj.delete()
    except Exception as e:
        print(e)
        pass


def read_file_txt_s3(path):
    try:
        path = path.lstrip("/").rstrip("/")
        obj = s3.Object(env.s3_bucket_name, path)
        content = obj.get()['Body'].read().decode("utf-8")
        return content
    except Exception as e:
        print(e)
        return None


def read_file_bin_s3(path):
    try:
        path = path.lstrip("/").rstrip("/")
        obj = s3.Object(env.s3_bucket_name, path)
        content = obj.get()['Body'].read()
        return content
    except Exception as e:
        print(e)
        return None


def read_file_df_s3(path):
    try:
        file = read_file_bin_s3(path)
        if file is not None:
            return pd.read_parquet(io.BytesIO(file))
        return None
    except Exception as e:
        print(e)
        return None
