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
                f"Currentry proccessing EDRPOU: {edrpou}")
        except Exception as e:
            self._logger.error(
                f"Exception occurred during EDRPOU parsing from Tender info: {e}")
            return None
        return edrpou

    def update_tender_info(self, tender_info):
        tender_in_coll = self._mongo_service.find_one(CONFIG.MONGO.TENDERS_COLLECTION,
                                                      {"tenderID": tender_info["tenderID"]})
        if (tender_in_coll is not None
                and datetime.fromisoformat(tender_in_coll['dateModified']) <= datetime.fromisoformat(
                    tender_info['dateModified'])
        ):
            self._mongo_service.update(CONFIG.MONGO.TENDERS_COLLECTION,
                                       {"tenderID": tender_info["tenderID"]},
                                       tender_info)
            self._logger.info(f"Tender {tender_info['tenderID']} updated")
        else:
            self._mongo_service.insert(CONFIG.MONGO.TENDERS_COLLECTION, tender_info, False)  # Doesn't work with true
            self._logger.info(f"Tender {tender_info['tenderID']} inserted")

    def process_historical_data(
            self,
            start_date: date = date(2024, 1, 1),
            end_date: date = date.today(),
            status: str = "complete",
    ) -> None:
        """
        This method is responsible for uploading the tenders in specific time limits(from start_date to end_date)
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
        try:
            while start_date < end_date:
                page_number = 1
                period_start_date = end_date - timedelta(days=7)
                self._logger.info(f"Getting tenders in time borders: {period_start_date} - {end_date}")
                while 1:
                    tenders = self.get_list_of_tenders(period_start_date, end_date, status, page_number)
                    if len(tenders) == 0:
                        break
                    self._logger.info(f"Found {len(tenders)} tenders on page {page_number}")
                    for tender in tenders:
                        tender_response = self.get_tender_details_response(tender['tenderID'])
                        self._logger.info(f"Processing tender with ID {tender['tenderID']}")
                        if tender_response.status_code == 200:
                            try:
                                tender_info = json.loads(tender_response.content)
                                self.update_tender_info(tender_info)
                                self._logger.info(
                                    f"Tender with ID {tender['tenderID']} inserted/updated in {CONFIG.MONGO.TENDERS_COLLECTION}")
                                if status == "complete":
                                    self._logger.info(
                                        f"Tender with ID {tender['tenderID']} is 'completed', trying to get EDRPOU")
                                    edrpou = self.get_edrpou_from_tender(tender_info)
                                    if edrpou:
                                        self._entity_processor.process_entity_info(edrpou)
                            except Exception as e:
                                self._logger.error(f"Error occured: {e}")

                        elif tender_response.status_code == 429:
                            self._logger.info(f'Timeout for {tender_response.headers["Retry-After"]} secs')
                            time.sleep(int(tender_response.headers["Retry-After"]))
                        else:
                            self._logger.info(f'Got {tender_response.status_code}')

                    page_number += 1
                end_date = period_start_date

        except Exception as e:
            self._logger.error(f"Failed to process tenders: {e}")

    def process_week_data(self) -> None:
        """
        This method is responsible for uploading the tenders in specific time limits(for last week)
        from Prozorro API to MongoDB.

        :return:
            None
        """
        self.process_historical_data(
            start_date=date.today() - timedelta(days=7),
            end_date=date.today(),
            status="active.tendering",
        )
        self.process_historical_data(
            start_date=date.today() - timedelta(days=7),
            end_date=date.today(),
            status="complete",
        )


if __name__ == "__main__":
    processor = TendersProcessor()
    processor.process_week_data()
