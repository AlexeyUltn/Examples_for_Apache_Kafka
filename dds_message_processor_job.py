import time, json 

from datetime import datetime 

from logging import Logger 

from lib.kafka_connect import KafkaConsumer, KafkaProducer 

from dds_loader.repository.dds_repository import DdsRepository 


class DdsMessageProcessor: 

    def __init__(self, 

                 consumer: KafkaConsumer, 

                 producer: KafkaProducer, 

                 dds_repository: DdsRepository, 

                 logger: Logger) -> None: 


        self._consumer =  consumer 

        self._producer = producer 

        self._dds_repository = dds_repository 

        self._logger = logger 

        self._batch_size = 50 
 

    def run(self) -> None: 

        # Стартуем джоб 

        self._logger.info(f"{datetime.utcnow()}: STARTING ---------------------------------------------->") 

        for i in range(self._batch_size): 

            message = self._consumer.consume() 

            if message == None: 

                continue 

            if "object_id" in message: 

                self._logger.info(f"Message: {message}") 

                # dds.s_user_names 

                self._dds_repository.satellite_insert('user_names', 'user', message["payload"]["user"]["id"], {"username": message["payload"]["user"]["name"], "userlogin": message["payload"]["user"]["login"]}) 

                # dds.l_order_user 

                self._dds_repository.link_insert("order", "user", str(message["object_id"]), message["payload"]["user"]["id"]) 

                # dds.h_restaurant 

                self._dds_repository.hub_insert("restaurant", message["payload"]["restaurant"]["id"]) 

                # dds.s_restaurant_names 

                self._dds_repository.satellite_insert('restaurant_names', 'restaurant', message["payload"]["restaurant"]["id"], {"name":  message["payload"]["restaurant"]["name"]}) 

                # dds.h_order 

                self._dds_repository.hub_insert("order", str(message["object_id"]), {"order_dt": message["payload"]["date"]}) 

                # dds.s_order_cost 

                self._dds_repository.satellite_insert('order_cost', 'order', str(message["object_id"]), {"cost": message["payload"]["cost"], "payment": message["payload"]["payment"]}) 

                # dds.s_order_status 

                self._dds_repository.satellite_insert('order_status', 'order', str(message["object_id"]), {"status": message["payload"]["status"]}) 

                # dds.h_user 

                self._dds_repository.hub_insert("user", message["payload"]["user"]["id"]) 

                for item in message["payload"]["products"]: 

                    # dds.l_order_product 

                    self._dds_repository.link_insert("order", "product", str(message["object_id"]), item["id"]) 

                    # dds.l_product_restaurant 

                    self._dds_repository.link_insert("product", "restaurant", item["id"], message["payload"]["restaurant"]["id"]) 

                    # dds.l_product_category 

                    self._dds_repository.link_insert("product", "category", item["id"], item["category"]) 

                    # dds.h_product 

                    self._dds_repository.hub_insert("product", item["id"]) 

                    # dds.s_product_names 

                    self._dds_repository.satellite_insert('product_names', 'product', item["id"], {"name":  item["name"]}) 

                    # dds.h_category 

                    self._dds_repository.hub_insert("category", item["category"]) 


                # Заполняем user_category_counters 

                out_message = {'dm_name': 'user_category_counters'} 

                out_message['dm_content'] = self._dds_repository.get_dm_data('user_category_counters') 

                self._logger.info(f"Out msg: {out_message}") 

                self._producer.produce(out_message) 

 

                # Заполняем user_product_counters 

                out_message = ({'dm_name': 'user_product_counters'}) 

                out_message['dm_content'] = self._dds_repository.get_dm_data('user_product_counters') 

                self._logger.info(f"Out msg: {out_message}") 

                self._producer.produce(out_message) 

        # Завершаем джоб 

        self._logger.info(f"{datetime.utcnow()}: FINISHED") 
