import math
import time
from datetime import date, datetime
from typing import Optional

import requests

from config import CONFIG
from utlis.logger import get_logger


logger = get_logger('ProzorroProcessor')


class ProzorroProcessor:
    """
    This class is responsible for interaction with Prozorro API.
    """

    SEARCH_TENDERS_URL = f"{CONFIG.PROZORRO.API_URL}/search/tenders"
    GET_TENDER_URL = f"{CONFIG.PROZORRO.API_URL}/tenders"

    def __init__(self):
        pass
    
    @staticmethod
    def get_tender_list(start_date: date, end_date: date, status: str = "complete") -> list:
        """Get the list of tenders from Prozorro API in the specified filters.
        The method will get all the pages of tenders and return the list of tenders.

        :param start_date: start date of the tenders to get
        :param end_date: end date of the tenders to get
        :param status: status of tenders to get, defaults to "complete"
        :return: list of extracted tenders
        """

        tenders = []
        ProzorroProcessor._validate_dates(start_date, end_date)
        logger.info(f"Getting tenders from Prozorro API in time borders: {start_date} - {end_date}")
        first_page = ProzorroProcessor._get_tenders_page(start_date, end_date, status)

        logger.info(f"Found {first_page['total']} tenders")
        tenders.extend(first_page["data"])

        page_count = math.ceil(int(first_page["total"]) / int(first_page["per_page"]))

        for page_number in range(2, page_count + 1):
            tenders.extend(ProzorroProcessor._get_tenders_page(start_date, end_date, status, page_number)["data"])

        return tenders

    @staticmethod
    def _get_tenders_page(
            start_date: date,
            end_date: date,
            status: str = "complete", 
            page_number: int = 1, 
            retry_count: int = 5
        ) -> dict:
        """
        Get the page of tenders from Prozorro API.
        If the request fails with 429 status code, it will retry the request
        """

        params = {
            "page": page_number
        }
        if start_date:
            params["date[tender][start]"] = start_date
        if end_date:
            params["date[tender][end]"] = end_date
        if status:
            params["status[0]"] = status

        logger.info(f"Getting tenders from Prozorro API. Page: {page_number}")
        response = requests.post(ProzorroProcessor.SEARCH_TENDERS_URL, params=params)

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 429 and retry_count > 0:
            timeout = int(response.headers.get("Retry-After", 10))
            logger.error(f"Failed to get tenders: {response.status_code}. Retrying in {timeout} sec...")
            time.sleep(timeout)
            return ProzorroProcessor._get_tenders_page(start_date, end_date, status, page_number, retry_count - 1)
        else:
            raise Exception(f"Failed to get tenders: {response.status_code}. {response.text}")
        
    @staticmethod
    def _validate_dates(start_date: date, end_date: date):

        if (start_date and not isinstance(start_date, date)) or (end_date and not isinstance(end_date, date)):
            raise TypeError("Dates should be of type date")

        if start_date and end_date and start_date > end_date:
            raise ValueError(f"Start date should be less than end date. ({start_date} > {end_date})")
        
    @staticmethod
    def get_tender_details_response(tender_id: str) -> requests.Response:
        response = requests.get(f"{ProzorroProcessor.GET_TENDER_URL}/{tender_id}")

        if response.status_code != 200:
            raise Exception(f"Failed to get tender details: {response.status_code}. {response.text}")
        
        return response

    @staticmethod
    def get_edrpou_from_tender(tender_info: dict) -> Optional[str]:
        try:
            edrpou = tender_info['awards'][0]['suppliers'][0]['identifier']['id']
            # logger.info(f"Got EDRPOU: {edrpou}")
        except Exception as e:
            # logger.error(f"Exception occurred during EDRPOU parsing from Tender info: {e}")
            return None
        return edrpou


if __name__ == "__main__":
    pass