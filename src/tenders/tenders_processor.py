from datetime import date
from datetime import timedelta

import requests


class TendersProcessor:
    """
    This class is responsible for uploading the closed tenders from Prozorro API to MongoDB.
    """

    def __init__(self):
        pass

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
                    # for tender in tenders:
                    # self._collection.update_one({"tenderID": tender["tenderID"]}, {'$set': tender}, upsert=True)
                    page_number += 1
                end_date = period_start_date
        except Exception as e:
            print(f"Exception occurred: {e}")

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
