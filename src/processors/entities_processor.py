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
        mongo = MongoService(CONFIG.MONGO.URI, CONFIG.MONGO.DB_NAME)
        self._logger = get_logger(__name__)

    def process_data(self, edrpou: str):
        try:
            url = f"https://opendatabot.ua/c/{edrpou}?utm_campaign=edr-c"
            page = requests.get(url)
            while page.status_code != 200:
                if page.status_code == 429:
                    self._logger.info(f'Timeout for {page.headers["Retry-After"]} secs')
                    time.sleep(int(page.headers["Retry-After"]))
                else:
                    self._logger.info(f'Got code: {page.status_code}')
                    time.sleep(1)
                page = requests.get(url)
            soup = BeautifulSoup(page.content, "html.parser")
            p = re.compile("""window.__INITIAL_STATE__='(.*)'""")
            find_raw_info = p.findall(soup.prettify())
            edrpou_info = json.loads(find_raw_info[0].replace('\\"', '').replace('\\', ''))["pageData"]["registryCell"]
            self._mongo_service.delete(CONFIG.MONGO.ENTITIES_COLLECTION,
                                       {"edrpou": edrpou})
            self._mongo_service.insert(CONFIG.MONGO.ENTITIES_COLLECTION, {"edrpou": edrpou, "info": edrpou_info})
        except Exception as ex:
            self._logger.info(f'Error: {ex}')


if __name__ == "__main__":
    # For testing
    youcontol_processor = EntityProcessor()
    youcontol_processor.process_data('39231218')
