import uuid 

from datetime import datetime 

from typing import Any, Dict, List 

from lib.pg import PgConnect 

from pydantic import BaseModel 


class CdmRepository: 

    def __init__(self, db: PgConnect) -> None: 

        self._db = db 


    # функция наполнения витрины 

    def insert_f(self, table: str, values: Dict): 

        fields = ','.join(values.keys()) 

        values_str = ', '.join(['%(' + value + ')s' for value in values.keys()]) 

        set_str = ', '.join([value + '=%(' + value + ')s' for value in values.keys()]) 

        key_field = f'{table}_unique' 

        with self._db.connection() as conn: 

            with conn.cursor() as cur: 

                cur.execute( 

                    f'insert into cdm.{table} ({fields}) \ 

                     values ({values_str}) \ 

                     ON CONFLICT ON CONSTRAINT {key_field} DO UPDATE \ 

                     set {set_str}', 

                    values 

                ) 
