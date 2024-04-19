from bs4 import BeautifulSoup
import re
import json
import time

import requests
from services.mongo import MongoService
from config import CONFIG
from utlis.logger import get_logger


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
        return json.loads(find_raw_info[0].replace('\\"', "").replace("\\", ""))["pageData"]["registryCell"]

    @staticmethod
    def create_details_dict(parsed_info) -> dict:
        details = {}
        for val in parsed_info:
            details[val['key']] = val['subtitle']
        del details['code']
        return details

    def get_many_entities_details(self, edrpous: list):
        return [self.get_entity_details(edrpou) for edrpou in edrpous]

    def get_entity_details(self, edrpou: str):
        try:
            page = self.get_entity_page(edrpou)
            while page.status_code != 200:
                if page.status_code == 429:
                    retry_after_value = page.headers.get("Retry-After")
                    if retry_after_value:
                        self._logger.info(f"Timeout for {retry_after_value} secs")
                        time.sleep(int(retry_after_value))
                    else:
                        self._logger.info("Retry-After header not found")
                        time.sleep(5)
                else:
                    self._logger.info(f"Got code: {page.status_code}")
                    raise Exception("Page not found")
                page = self.get_entity_page(edrpou)
            unstuctured_edrpou_info = self.parse_entity_page(page)
            stuctured_edrpou_info = self.create_details_dict(unstuctured_edrpou_info)
            stuctured_edrpou_info['edrpou'] = edrpou
            self._logger.info(f"EDRPOU ({edrpou}) info successfully parsed")
            return stuctured_edrpou_info
        except Exception as ex:
            self._logger.error(f"Error parsing EPRDOU info: {ex}")
            return None


if __name__ == "__main__":
    pass
