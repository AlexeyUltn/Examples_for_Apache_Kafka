from datetime import datetime 

from logging import Logger 

from lib.kafka_connect import KafkaConsumer 

from cdm_loader.repository.cdm_repository import CdmRepository 

from uuid import UUID 

 
class CdmMessageProcessor: 

    def __init__(self, 

                 consumer: KafkaConsumer, 

                 cdm_repository: CdmRepository, 

                 logger: Logger) -> None: 

 

        self._consumer =  consumer 

        self._cdm_repository = cdm_repository 

        self._logger = logger 

        self._batch_size = 50 


    # функция, которая будет вызываться по расписанию. 

    def run(self) -> None: 

            self._logger.info(f"{datetime.utcnow()}: START---------------------------------------------->") 

            for i in range(self._batch_size): 

                message = self._consumer.consume() 

                if not message: 

                    break 

                self._logger.info(f"{datetime.utcnow()}: {message}") 

                order = message['payload'] 

                if order['status'] == 'CANCELLED': 

                    self._logger.info(f"{datetime.utcnow()}: CANCELLED. NOT MY DATA!") 

                    continue 

                user_id = UUID(order['user']['id']) 

                cat_dict = {} 

                for p in order['products']: 

                    prod_id = UUID(p['id']) 

                    prod_name = p['name'] 

                    self._userproductcnt.inc(user_id, prod_id, prod_name) 

                    cat_dict[p['category']['id']] = p['category']['name'] 

                rest_id = UUID(order['restaurant']['id']) 

                rest_name = order['restaurant']['name'] 

                for (cat_id, cat_name) in cat_dict.items(): 

                    self._restcategorycnt.inc(rest_id, rest_name, UUID(cat_id), cat_name) 

 

            self._logger.info(f"{datetime.utcnow()}: FINISHED!")
