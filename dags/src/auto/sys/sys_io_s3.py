import boto3

import src.auto.env as env
from src.auto.util.enum_util import FsTypeEnum
import pyarrow.parquet as pq
import io

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
        content = obj.get()['Body'].read()
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


def read_file_df_s3(path, page_number=None):
    try:
        # todo boto3 yerine s3fs kullanirsak page_number tam destekliyor
        buffer = io.BytesIO(read_file_bin_s3(path))
        pf = pq.ParquetFile(buffer)
        if page_number is None:
            df = pf.read().to_pandas()
        else:
            df = pf.read_row_group(page_number-1).to_pandas()
        return df
    except Exception as e:
        print(e)
        return None
