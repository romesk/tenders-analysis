import json
from datetime import date

import requests
from dateutil.relativedelta import relativedelta
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi


class ClosedTendersProcessor:
    """
    This class is responsible for uploading the closed tenders from Prozorro API to MongoDB.
    """

    def __init__(self):
        self._config = json.load(open('config.json'))
        self._client = MongoClient(self._config['uri'], server_api=ServerApi('1'))
        self._db = self._client.get_database(self._config['db_name'])
        self._collection = self._db.get_collection(self._config['coll_name'])

    def process_historical_data(self, start_date: date = date(2024, 1, 1), end_date: date = date.today()) -> None:
        """
        This method is responsible for uploading the closed tenders in specific time limits(from start_date to end_date)
        from Prozorro API to MongoDB.

        :param start_date:
            Start point of data processing
        :param end_date:
            End point of data processing
        :return:
            None
        """
        try:
            while start_date < end_date:
                page_number = 1
                period_start_date = end_date - relativedelta(days=7)
                while 1:
                    contents = requests.post(
                        f'https://prozorro.gov.ua/api/search/tenders?date%5Btender%5D%5Bstart%5D={period_start_date}'
                        f'&date%5Btender%5D%5Bend%5D={end_date}'
                        f'&status%5B0%5D=complete&page={page_number}').json()
                    tenders = list(contents['data'])
                    if len(tenders) == 0:
                        break
                    for tender in tenders:
                        self._collection.update(tender, tender, upsert=True)
                    page_number += 1
                end_date = period_start_date
        except Exception as e:
            print(f'Exception occurred: {e}')

    def process_week_data(self) -> None:
        """
        This method is responsible for uploading the closed tenders in specific time limits(for last week)
        from Prozorro API to MongoDB.

        :return:
            None
        """
        self.process_historical_data(date.today() - relativedelta(days=7), date.today())


if __name__ == '__main__':
    pass
