import json
from datetime import date, datetime
from datetime import timedelta
import time

import requests
from src.services.mongo import MongoService
from src.config import CONFIG
from src.utlis.logger import get_logger
from src.processors.entities_processor import EntityProcessor


class TendersProcessor:
    """
    This class is responsible for uploading the closed tenders from Prozorro API to MongoDB.
    """

    def __init__(self):
        self._mongo_service = MongoService(CONFIG.MONGO.URI, CONFIG.MONGO.DB_NAME)
        self._entity_processor = EntityProcessor()
        self._logger = get_logger(__name__)

    @staticmethod
    def get_list_of_tenders(start_date: date, end_date: date, status: str = "complete",
                            page_number: int = 1) -> list:
        contents = requests.post(  # date_modified
            f"https://prozorro.gov.ua/api/search/tenders?"
            f"date%5Btender%5D%5Bstart%5D={start_date}"
            f"&date%5Btender%5D%5Bend%5D={end_date}"
            f"&status%5B0%5D={status}"
            f"&page={page_number}"
        ).json()
        return list(contents["data"])

    @staticmethod
    def get_tender_details_response(tender_id: str) -> requests.Response:
        return requests.get(f"https://prozorro.gov.ua/api/tenders/{tender_id}")

    def get_edrpou_from_tender(self, tender_info: dict):
        try:
            edrpou = tender_info['awards'][0]['suppliers'][0]['identifier']['id']
            self._logger.info(
                f"Got EDRPOU: {edrpou}")
        except Exception as e:
            self._logger.error(
                f"Exception occurred during EDRPOU parsing from Tender info: {e}")
            return None
        return edrpou

    def get_historical_data(
            self,
            start_date: date = date.today() - timedelta(days=7),
            end_date: date = date.today(),
            status: str = "complete",
    ) -> tuple[list, list]:
        """
        This method is responsible for getting the tenders and EDRPOU-s of performers in specific time limits(from start_date to end_date)
        from Prozorro API to MongoDB.

        :param start_date:
            Start point of data processing
        :param end_date:
            End point of data processing
        :param status:
            Status of the tenders to process
        :return:
            None
        """
        tenders_details = []
        edrpous = []
        try:
            while start_date < end_date:
                page_number = 1
                period_start_date = end_date - timedelta(days=7)
                self._logger.info(f"Getting tenders in time borders: {period_start_date} - {end_date}")
                while 1:
                    tenders = self.get_list_of_tenders(period_start_date, end_date, status, page_number)
                    if len(tenders) == 0:
                        self._logger.info(
                            f"Found 0 tenders on page {page_number} in period from {period_start_date} - {end_date}")
                        break
                    self._logger.info(f"Found {len(tenders)} tenders on page {page_number}")
                    for tender in tenders:
                        tender_response = self.get_tender_details_response(tender['tenderID'])
                        self._logger.info(f"Processing tender with ID {tender['tenderID']}")
                        if tender_response.status_code == 200:
                            try:
                                tender_details = json.loads(tender_response.content)
                                tenders_details.append(tender_details)
                                self._logger.info(f"Tender with ID {tender['tenderID']} processed")
                                if status == "complete":
                                    self._logger.info(
                                        f"Tender with ID {tender['tenderID']} is 'completed', trying to get EDRPOU")
                                    edrpou = self.get_edrpou_from_tender(tender_details)
                                    if edrpou:
                                        edrpous.append(edrpou)

                            except Exception as e:
                                self._logger.error(f"Error occured: {e}")

                        elif tender_response.status_code == 429:
                            self._logger.info(f'Timeout for {tender_response.headers["Retry-After"]} secs')
                            time.sleep(int(tender_response.headers["Retry-After"]))
                        else:
                            self._logger.info(f'Got {tender_response.status_code}')

                    page_number += 1
                end_date = period_start_date
            return tenders_details, edrpous
        except Exception as e:
            self._logger.error(f"Failed to process tenders and EDRPOU-s: {e}")


if __name__ == "__main__":
    pass
