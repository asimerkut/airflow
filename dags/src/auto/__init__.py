from src.auto.conn.system_db import SystemDb
from src.auto.conn.fs.connector_fs_lfs import ConnectorFsLfs
from src.auto.util.copy_util import CopyUtil

__system_db = SystemDb()
__system_fs = ConnectorFsLfs({})
__copy_util = CopyUtil()

def get_system_db():
    return __system_db

def get_system_fs():
    return __system_fs

def get_copy_util():
    return __copy_util

