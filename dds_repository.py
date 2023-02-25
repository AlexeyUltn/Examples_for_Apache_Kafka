import uuid 

from datetime import datetime 

from typing import Any, Dict, List 

 

from lib.pg import PgConnect 

from pydantic import BaseModel 

 

class DdsRepository: 

    def __init__(self, db: PgConnect) -> None: 

        self._db = db 
 

    # Функция обновления 

    def update(self, table, values, key_field): 

        main_fields = {'load_dt': datetime.utcnow(), 'load_src': 'orders-system-kafka'} 

        values.update(main_fields) 

        fields = ','.join(values.keys()) 

        new_str = ', '.join(['%(' + item + ')s' for item in values.keys()]) 

        new_str = ', '.join([item + '=%(' + item + ')s' for item in values.keys()]) 

        with self._db.connection() as conn: 

            with conn.cursor() as cur: 

                cur.execute( 

                    f'insert into dds.{table} ({fields}) \ 

                     values ({new_str}) \ 

                     ON CONFLICT ({key_field}) DO UPDATE \ 

                     set {new_str}', 

                    values 

                ) 

 

    # Вставка хабов 

    def hub_insert(self, object_name: str, object_id: str, add: Dict = None): 

        if object_name  == 'category': 

            id_field = f'{object_name}_name' 

        else: 

            id_field = f'{object_name}_id' 

        values = {f'h_{object_name}_pk' : uuid.uuid3(uuid.NAMESPACE_OID, object_id), 

                  id_field: object_id 

                 } 

        # Добавляем доп. поля 

        if add: values.update(add) 

        key_field = f'h_{object_name}_pk' 

        self.update(f'h_{object_name}', values, key_field) 

 

	# Вставка линков 

    def link_insert(self, object_name1: str, object_name2: str, object_id1: str, object_id2: str): 

        values = {f'hk_{object_name1}_{object_name2}_pk': uuid.uuid3(uuid.NAMESPACE_OID, object_id1+object_id2), 

                  f'h_{object_name1}_pk': uuid.uuid3(uuid.NAMESPACE_OID, object_id1), 

                  f'h_{object_name2}_pk': uuid.uuid3(uuid.NAMESPACE_OID, object_id2), 

                 } 

 

        key_field = f'hk_{object_name1}_{object_name2}_pk' 

        self.update(f'l_{object_name1}_{object_name2}', values, key_field) 

 

	# Вставка сателлитов 

    def satellite_insert(self, table_name: str, object_name: str, object_id: str, attributes: Dict): 

        values = {f'hk_{table_name}_pk': uuid.uuid3(uuid.NAMESPACE_OID, object_id+''.join(str(i) for i in attributes.values())), 

                  f'h_{object_name}_pk': uuid.uuid3(uuid.NAMESPACE_OID, object_id) 

                 } 

        values.update(attributes) 

        key_field = f'hk_{table_name}_pk' 

        self.update(f's_{table_name}', values, key_field) 

 

    # Вставка в кафку 

    def get_dm_data(self, dm_name): 

        if dm_name == 'user_category_counters': 

            query = '''select ou.h_user_pk::varchar as user_id, 

                              c.h_category_pk::varchar as category_id, 

                              c.category_name, 

                              count(*) as order_cnt 

                       from dds.h_order o 

                       join dds.s_order_status os on os.h_order_pk = o.h_order_pk 

                       join dds.l_order_user ou on o.h_order_pk = ou.h_order_pk 

                       join dds.l_order_product op on o.h_order_pk = op.h_order_pk 

                       join dds.h_product p on p.h_product_pk = op.h_product_pk 

                       join dds.l_product_category pc on pc.h_product_pk = p.h_product_pk 

                       join dds.h_category c on c.h_category_pk = pc.h_category_pk 

                       where os.status = 'CLOSED' 

                       group by ou.h_user_pk, c.h_category_pk, c.category_name''' 

        elif dm_name == 'user_product_counters': 

            query = '''select ou.h_user_pk::varchar as user_id, 

                               p.h_product_pk::varchar as product_id, 

                               pn."name" as product_name, 

                               count(*) as order_cnt 

                        from dds.h_order o 

                        join dds.s_order_status os on os.h_order_pk = o.h_order_pk 

                        join dds.l_order_user ou on o.h_order_pk = ou.h_order_pk 

                        join dds.l_order_product op on o.h_order_pk = op.h_order_pk 

                        join dds.h_product p on p.h_product_pk = op.h_product_pk 

                        join dds.s_product_names pn on pn.h_product_pk = p.h_product_pk 

                        where os.status = 'CLOSED' 

                        group by ou.h_user_pk, p.h_product_pk, pn."name"''' 


        with self._db.connection() as conn: 

            with conn.cursor() as cur: 

                cur.execute(query) 

                resp = cur.fetchall() 

                columns = list(cur.description) 

                results = [] 

                for row in resp: 

                    row_dict = {} 

                    for i, col in enumerate(columns): 

                        row_dict[col.name] = row[i] 

                    results.append(row_dict) 

 

        return results
