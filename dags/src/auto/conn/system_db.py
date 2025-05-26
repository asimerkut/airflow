import uuid

import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import src.auto.env as env
from src.auto.util.custom_map import CustomMap


class SystemDb:

    def __init__(self):
        host = env.PRM_DB_HOST
        port = env.PRM_DB_PORT
        database = env.PRM_DB_DATABASE
        user = env.PRM_DB_USER
        password = env.PRM_DB_PASSWORD
        self.connection = psycopg2.connect(
            host=host, port=port, database=database, user=user, password=password
        )
        self.engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")
        Session = sessionmaker(bind=self.engine)
        self.db_session = Session()
        self.item_map = CustomMap[str, dict]()

        print("System Db Connected...")
        pass

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def execute(self, query):
        cursor = self.connection.cursor()
        try:
            cursor.execute(query=query)
            self.connection.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            raise error

    def query(self, query, params=None):
        query_text = text(query)
        try:  # sqlalchemy2
            with self.engine.begin() as connection:
                df = pd.read_sql(query_text, connection, params=params)
        except Exception as e:  # sqlalchemy1
            result = self.db_session.execute(query_text, params)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df

    def get_item_value_all(self, key_id):
        sql = "select v.code, v.id from srvdefinition.item_value v where v.key_id = :i_key"
        params = {"i_key": key_id}
        df = self.query(sql, params)
        self.item_map.add(key_id, {})
        for index, row in df.iterrows():
            self.item_map.get(key_id)[row["code"]] = row["id"]

    def get_item_value(self, key_id, prm_code):
        prm_code = str(prm_code)
        sql = "select v.* from srvdefinition.item_value v where v.key_id = :i_key and v.code = :i_code"
        params = {"i_key": key_id, "i_code": prm_code}
        df = self.query(sql, params)
        if not df.empty:
            return df.iloc[0].to_dict()
        else:
            return {}

    def check_item_value(self, key_id, prm_code):
        prm_code = str(prm_code)
        if self.item_map.has(key_id) and self.item_map.get(key_id).get(prm_code, None) is not None:
            return str(self.item_map.get(key_id).get(prm_code))
        new_item = self.create_item_value(key_id, prm_code, prm_code)
        self.item_map.get(key_id)[prm_code] = new_item["id"]
        return str(new_item["id"])

    def create_item_value(self, key_id, prm_code, prm_name):
        prm_code = str(prm_code)
        prm_name = str(prm_name)
        schema_name = "srvdefinition"
        table_name = "item_value"
        prm_id = str(uuid.uuid4())
        _columns = ["id", "key_id", "code", "sub_code", "name", "is_leaf", "is_active", "level_no", "order_no"]
        _values = [prm_id, key_id, prm_code, prm_code, prm_name, "true", "true", "1", "1"]
        for i in range(len(_values)):
            if isinstance(_values[i], str):
                _values[i] = "'" + _values[i] + "'"
        sql = f"""
                insert into {schema_name}.{table_name} ({",".join(_columns)}) values ({",".join(_values)})
                """
        self.execute(sql)
        item = self.get_item_value(key_id, prm_code)
        return item
