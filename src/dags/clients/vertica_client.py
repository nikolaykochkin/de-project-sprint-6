from typing import Dict

from vertica_python import connect
from vertica_python.vertica.connection import Connection
from vertica_python.vertica.cursor import Cursor


class VerticaClient:
    def __init__(self, vertica_conn_info: Dict, stage_schema_name: str, dwh_schema_name: str,
                 path_template: str) -> None:
        self.vertica_conn_info = vertica_conn_info
        self.stage_schema_name = stage_schema_name
        self.dwh_schema_name = dwh_schema_name
        self.path_template = path_template

    def copy_file_to_vertica(self, file_name: str, table_name: str) -> None:
        full_table_name = f"{self.stage_schema_name}.{table_name}"
        conn: Connection
        with connect(**self.vertica_conn_info) as conn, \
                open(self.path_template.format(file_name), 'rb') as fs:
            cur: Cursor = conn.cursor()
            cur.copy(f"""COPY {full_table_name} FROM STDIN 
                            DELIMITER ',' 
                            ENCLOSED BY '\"'
                            NO ESCAPE 
                            SKIP 1 
                            REJECTED DATA AS TABLE {full_table_name}_rej""", fs)
            conn.commit()

    def execute_query_from_template(self, templates_dict: Dict) -> None:
        conn: Connection
        with connect(**self.vertica_conn_info) as conn:
            cur: Cursor = conn.cursor()
            cur.execute(f'SET SEARCH_PATH TO {self.dwh_schema_name}, {self.stage_schema_name};')
            cur.execute(templates_dict.get("query"))
            conn.commit()
