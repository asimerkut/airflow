from src.auto.conn.db.connector_db_oracle import ConnectorDbOracle
from src.auto.conn.db.connector_db_postgres import ConnectorDbPostgres
from src.auto.conn.fs.connector_fs_hdfs import ConnectorFsHdfs
from src.auto.conn.fs.connector_fs_lfs import ConnectorFsLfs
from src.auto.conn.fs.connector_fs_rfs import ConnectorFsRfs


# FileSys #
def fs_lfs(cmp, object_prop, object_vars):
    connector = ConnectorFsLfs(object_prop)
    return object_vars, connector


def fs_rfs(cmp, object_prop, object_vars):
    connector = ConnectorFsRfs(object_prop,
                               hostname=object_prop["hostname"],
                               username=object_prop["username"],
                               password=object_prop["password"]
                               )
    # look_for_keys = look_for_keys,
    # allow_agent = allow_agent

    return object_vars, connector


def fs_hdfs(cmp, object_prop, object_vars):
    connector = ConnectorFsHdfs(object_prop,
                                url=object_prop["url"],
                                user=object_prop["user"],
                                spark_hdfs=object_prop["spark_hdfs"]
                                )
    return object_vars, connector


# Database #

def db_oracle(cmp, object_prop, object_vars):
    connector = ConnectorDbOracle(object_prop,
                                  host=object_prop["host"],
                                  port=object_prop["port"],
                                  database=object_prop["database"],
                                  user=object_prop["user"],
                                  password=object_prop["password"]
                                  )
    return object_vars, connector


def db_postgres(cmp, object_prop, object_vars):
    connector = ConnectorDbPostgres(object_prop,
                                    host=object_prop["host"],
                                    port=object_prop["port"],
                                    database=object_prop["database"],
                                    user=object_prop["user"],
                                    password=object_prop["password"]
                                    )
    return object_vars, connector
