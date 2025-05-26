import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from src.auto import env
from src.auto.base.base_conn_db import BaseConnDb


class ConnectorDbPostgres(BaseConnDb):

    def __init__(self, object_prop: dict,
                 host=None, port=None, database=None, user=None, password=None):
        super().__init__(object_prop)

        dsn = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        self.engine = create_engine(dsn)
        Session = sessionmaker(bind=self.engine)
        self.db_session = Session()

        self.db_test()
        pass

    def close(self):
        super().close()
        self.db_session.close()

    def __check_injection(self, expr):
        words = ["insert ", "update ", "delete ", "truncate ", "drop "]
        for word in words:
            if word in expr:
                raise Exception("sql injection: ", words)

    def commit(self):
        self.db_session.commit()

    def rollback(self):
        self.db_session.rollback()

    def db_test(self):
        sql_query = f"""SELECT 'X' dummy"""
        result = self.execute_query(sql_query)
        print(f"postgres db_test : {result}")
        return result

    def list_databases(self):
        sql_query = f"""
        SELECT datname as database_name 
          FROM pg_database
         where datistemplate = False 
           and datallowconn = True
        """
        result = self.execute_query(sql_query)
        return result

    def list_schemas(self, db_name):
        self.__check_injection(db_name)

        def check_value(value):
            if value and value != '*':
                return '%'
            else:
                return '#'

        p_user = check_value(db_name)
        sql = f"""
        SELECT distinct table_schema as schema FROM information_schema.tables where table_catalog = '{db_name}'
        """
        result = self.execute_query(sql, {"p_user": p_user})
        return result

    def list_tables(self, db_name, schema):
        self.__check_injection(db_name)
        self.__check_injection(schema)

        sql = f"""
        SELECT table_name FROM information_schema.tables where table_catalog = '{db_name}' and table_schema = '{schema}'
        """
        result = self.execute_query(sql, {"p_owner": schema})
        return result

    def list_columns(self, db_name, schema, table_name):
        self.__check_injection(db_name)
        self.__check_injection(schema)
        self.__check_injection(table_name)

        sql = f"""
                SELECT column_name, data_type, ordinal_position, is_nullable
                FROM information_schema.columns
                WHERE
                    table_catalog = '{db_name}'
                    AND table_schema = '{schema}'
                    AND table_name = '{table_name}'
                    order by ordinal_position asc
                """
        result = self.execute_query(sql, {"p_owner": schema, "p_table_name": table_name})
        return result

    def execute_query(self, query, params: dict = None):
        return self.execute_query_pandas(query, params)

    def prepare_query_with_params(self, query, params):
        if params:
            for key, value in params.items():
                query = query.replace(f":{key}", f"'{value}'")
        return query

    def execute_query_pandas(self, query, params: dict = None):
        query_text = text(query)
        try:  # sqlalchemy
            with self.engine.connect() as connection:
                df = pd.read_sql(query_text, connection, params=params)
        except Exception as e:  # legacy
            result = self.db_session.execute(query_text, params)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

        json_columns = [col for col in df.columns if 'json' in col]
        for col in json_columns:
            first_valid_index = df[col].first_valid_index()
            if first_valid_index and isinstance(df[col].iloc[first_valid_index], dict):
                json_df = df[col].apply(lambda x: pd.Series(x) if isinstance(x, dict) else pd.Series())
                json_df.columns = [f"{col}_{c}" for c in json_df.columns]
                df = pd.concat([df, json_df], axis=1)
                # df = df.drop(col, axis=1)
        return df  # pandas dataframe

    def execute_update(self, query, params=None):
        connection = self.connection()
        try:
            connection.cursor().execute(query, params)
            connection.commit()
            pass
        except (Exception) as error:
            print("Error: %s" % error)
            raise error

    def drop_database(self, database_name):
        self.db_session.autocommit = True
        sql = f"""
        drop database {database_name}
        """
        self.execute_update(sql)

    def drop_schema(self, schema_name=None):
        sql = f"""
        drop schema {schema_name}
        """
        self.execute_update(sql)

    def truncate_table(self, schema_name, table_name):
        sql = f"""
        truncate table {schema_name}.{table_name}
        """
        self.execute_update(sql)

    def delete_from_table(self, schema_name, table_name, where_clause=None):
        sql = f"""
        delete from {schema_name}.{table_name} where {where_clause}
        """
        self.execute_update(sql)

    def select_from_table(self, schema_name, table_name, columns, where_clause=None):

        _columns = ",".join(columns)
        sql = f"""
        select {_columns} from {schema_name}.{table_name}
        """

        if where_clause is not None and where_clause != 'None':
            sql = sql + " where " + where_clause

        return self.execute_query(sql)

    def update_table(self, schema_name, table_name, key_value_map, where_clause=None):

        _key_value_list = []

        for key, value in key_value_map.items():

            if isinstance(value, str):
                value = f"'{value}'"

            _key_value_list.append(key + "=" + value)

        _key_value_list = ",".join(_key_value_list)

        query = f"""
        update {schema_name}.{table_name} set {_key_value_list} where {where_clause}
        """
        self.execute_update(query)
        self.db_session.commit()

    def insert_into_table(self, schema_name, table_name, key_value_map):
        _columns = []
        _values = []
        for key in key_value_map:
            _columns.append(key)

            value = key_value_map[key]
            if isinstance(value, str):
                value = f"'{value}'"
            _values.append(str(value))

        query = f"""
        insert into {schema_name}.{table_name} ({",".join(_columns)}) values ({",".join(_values)})
        """
        self.execute_update(query)
        self.db_session.commit()

    def write_table(self, schema, table, dataframe, has_a_sequence, sequence_name, identity_column,
                    insert_template=None, page_size=env.page_size):

        if insert_template is not None and schema is not None and table is not None:
            raise Exception(
                "insert_template kullanilmissa, schema ve table alanları None atanmalı"
            )
        try:
            cursor = self.connection().cursor()
            tpls = [tuple(x) for x in dataframe.to_numpy()]

            if schema is not None:
                cols_q = ['"' + str(c) + '"' for c in dataframe.columns]
                cols_str = ",".join(list(cols_q))
                val_vars = ",".join(['%s'] * len(cols_q))
                if has_a_sequence is True:
                    sequence_name = sequence_name
                    identity_column = identity_column
                    sql_template = f"INSERT INTO {schema}.{table} ({identity_column},{cols_str}) values (NEXTVAL('{schema}.{sequence_name}'),{val_vars})"
                else:
                    sql_template = f"INSERT INTO {schema}.{table} ({cols_str}) values ({val_vars})"

                # extras.execute_batch(cursor, sql_template, tpls, page_size)
            else:
                # extras.execute_batch(cursor, insert_template, tpls, page_size)
                pass
            self.db_session.commit()

        except (Exception) as e:
            self.db_session.rollback()
            cursor.close()
            raise e

    def create_table(self, schema, table, dataframe, if_exists="fail", index=False):
        dataframe.columns = dataframe.columns.str.lower()
        with self.engine.connect() as conn:
            dataframe.to_sql(
                name=table,
                con=conn,
                schema=schema,
                if_exists=if_exists,
                index=index)

    def connection(self):
        return self.engine.raw_connection()

    def cursor(self):
        return self.connection().cursor()
