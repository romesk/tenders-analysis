import json
from datetime import date
from datetime import timedelta
import time

import requests
from src.services.mongo import MongoService
from src.config import CONFIG
from src.utlis.logger import get_logger


class TendersProcessor:
    """
    This class is responsible for uploading the closed tenders from Prozorro API to MongoDB.
    """

    def __init__(self):
        # self._mongo_service = MongoService(CONFIG.MONGO.URI, CONFIG.MONGO.DB_NAME)
        self._mongo_service = MongoService(
            "mongodb+srv://kursovaskopet:2CXuIFlwnKZDObRe@kursova-cluster.74rxrdh.mongodb.net/?retryWrites=true&w=majority&appName=kursova-cluster",
            "kursova")
        self._logger = get_logger(__name__)

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
                while 1:
                    contents = requests.post(
                        f"https://prozorro.gov.ua/api/search/tenders?"
                        f"date%5Btender%5D%5Bstart%5D={period_start_date}"
                        f"&date%5Btender%5D%5Bend%5D={end_date}"
                        f"&status%5B0%5D={status}"
                        f"&page={page_number}"
                    ).json()
                    tenders = list(contents["data"])
                    if len(tenders) == 0:
                        break
                    # TODO: Implement the logic for returning the tenders. Probalby, it should be a generator.
                    for tender in tenders:
                        tender_request = requests.get(
                            f"https://prozorro.gov.ua/api/tenders/{tender['tenderID']}",
                        )
                        if tender_request.status_code == 200:
                            try:
                                tender_info = json.loads(tender_request.content)
                                self._mongo_service.delete(CONFIG.MONGO.TENDERS_COLLECTION,
                                                           {"tenderID": tender_info["tenderID"]})
                                self._mongo_service.insert(CONFIG.MONGO.TENDERS_COLLECTION, tender_info)
                            except Exception as e:
                                self._logger.error(f"Exception occurred: {e}")

                        elif tender_request.status_code == 429:
                            self._logger.info(f'Timeout for {tender_request.headers["Retry-After"]} secs')
                            time.sleep(int(tender_request.headers["Retry-After"]))

                        else:
                            self._logger.info(f'Got {tender_request.status_code}')

                    page_number += 1
                end_date = period_start_date

        except Exception as e:
            self._logger.error(f"Exception occurred: {e}")

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
