from bs4 import BeautifulSoup
import re
import json
import time

import requests
from src.services.mongo import MongoService
from src.config import CONFIG
from src.utlis.logger import get_logger


class EntityProcessor:
    def __init__(self):
        self._mongo_service = MongoService(CONFIG.MONGO.URI, CONFIG.MONGO.DB_NAME)
        self._logger = get_logger(__name__)

    @staticmethod
    def get_entity_page(edrpou):
        return requests.get(f"https://opendatabot.ua/c/{edrpou}?utm_campaign=edr-c")

    @staticmethod
    def parse_entity_page(page):
        soup = BeautifulSoup(page.content, "html.parser")
        p = re.compile("""window.__INITIAL_STATE__='(.*)'""")
        find_raw_info = p.findall(soup.prettify())
        return json.loads(find_raw_info[0].replace('\\"', '').replace('\\', ''))["pageData"]["registryCell"]

    def upsert_entity(self, edrpou, edrpou_info):
        if self._mongo_service.find_one(CONFIG.MONGO.ENTITIES_COLLECTION, {"edrpou": edrpou}):
            self._mongo_service.update(CONFIG.MONGO.ENTITIES_COLLECTION,
                                       {"edrpou": edrpou},
                                       {"edrpou": edrpou, "info": edrpou_info})
            self._logger.info(f"EDRPOU ({edrpou}) info already exist, updating")
        else:
            self._mongo_service.insert(CONFIG.MONGO.ENTITIES_COLLECTION,
                                       {"edrpou": edrpou, "info": edrpou_info})
            self._logger.info(f"EDRPOU ({edrpou}) inserted")

    def process_entity_info(self, edrpou: str):
        try:
            page = self.get_entity_page(edrpou)
            num_of_retries = 0
            while page.status_code != 200:
                if page.status_code == 429:
                    retry_after_value = page.headers.get("Retry-After")
                    if retry_after_value:
                        self._logger.info(f'Timeout for {retry_after_value} secs')
                        time.sleep(int(retry_after_value))
                    else:
                        self._logger.info('Retry-After header not found')
                        if num_of_retries >= 10:
                            raise Exception(f'Reached max number of retries {num_of_retries}')
                        num_of_retries += 1
                        time.sleep(1)
                else:
                    self._logger.info(f'Got code: {page.status_code}')
                    raise Exception('Page not found')
                page = self.get_entity_page(edrpou)
            edrpou_info = self.parse_entity_page(page)
            self._logger.info(f'EDRPOU ({edrpou}) info successfully parsed')
            self.upsert_entity(edrpou, edrpou_info)

        except Exception as ex:
            self._logger.error(f'Error parsing EPRDOU info: {ex}')


if __name__ == "__main__":
    # For testing
    entity_processor = EntityProcessor()
    entity_processor.process_entity_info('39231218')
