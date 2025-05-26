import os

import torch
from dotenv import load_dotenv

load_dotenv()

# internal app db: postgres
PRM_DB_HOST = os.getenv('PRM_DB_HOST',"127.0.0.1")
PRM_DB_PORT = os.getenv('PRM_DB_PORT',"5432")
PRM_DB_DATABASE = os.getenv('PRM_DB_DATABASE',"sgkdb")
PRM_DB_SCHEMA = os.getenv('PRM_DB_SCHEMA',"srvdefinition")
PRM_DB_USER = os.getenv('PRM_DB_USER',"adminsgk")
PRM_DB_PASSWORD = os.getenv('PRM_DB_PASSWORD',"SgkAdmin12")

PRM_UUID0 = "00000000-0000-0000-0000-000000000000"
PRM_LFS_PATH = os.getenv('PRM_LFS_PATH','/app/lfs')
PRM_FLOW_PATH = PRM_LFS_PATH + "/flow"
PRM_MODEL_PATH = PRM_LFS_PATH + "/model"
page_size = 100

JDBC_DRIVER_OR = os.getenv('JDBC_DRIVER_OR')
JDBC_PATH_OR = os.getenv('JDBC_PATH_OR')
JDBC_DRIVER_PG = os.getenv('JDBC_DRIVER_PG')
JDBC_PATH_PG = os.getenv('JDBC_PATH_PG')

PRM_SPARK_MASTER = os.getenv('PRM_SPARK_MASTER')
PRM_SPARK_LFS_PATH = os.getenv('PRM_SPARK_LFS_PATH')
PRM_SPARK_CONN_MEMORY = os.getenv('PRM_SPARK_CONN_MEMORY')
PRM_SPARK_EXECUTOR_CORES = os.getenv('PRM_SPARK_EXECUTOR_CORES')
PRM_SPARK_EXECUTOR_MEMORY = os.getenv('PRM_SPARK_EXECUTOR_MEMORY')
PRM_SPARK_DEFAULT_PARALLELISM = os.getenv('PRM_SPARK_DEFAULT_PARALLELISM')

# PRM_ROOT_PATH = os.path.abspath(os.getcwd()).replace("/src", "")
# PRM_DATAFRAME = os.getenv('PRM_DATAFRAME')

# prod db : oracle
dbHost = "host.docker.internal"
dbPort = 1521
dbName = "ORCLCDB"
jdbcUrl = f"jdbc:oracle:thin:@{dbHost}:{dbPort}/{dbName}"
connectionProps = {
    "user": "C##common_user",
    "password": "password",
}

PRM_GPU_TYPE = "cpu"
if torch.backends.mps.is_available():
    PRM_GPU_TYPE = "mps"
elif torch.cuda.is_available():
    PRM_GPU_TYPE = "cuda"

_conf_dat = "_conf.json"
_head_dat = "_head.json"
_rows_dat = "_rows.parquet"
_blob_dat = "_blob.bin"

grp_port_in = "attr_port_in"
grp_prop = "attr_prop"
grp_data = "data"

fs_type = "lfs"
s3_bucket_name = "ibucket"
s3 = {
    "endpoint_url": "http://localhost:9090",
    "aws_access_key_id": "admin",
    "aws_secret_access_key": "admin123"
}
