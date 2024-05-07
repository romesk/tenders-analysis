import json
import math
import time
from datetime import date, timedelta
from typing import Optional

import requests

from config import CONFIG
from utlis.logger import get_logger

logger = get_logger("ProzorroProcessor")

statuses = {"complete": {"pages": 10, "delta_days": 5}, "active.tendering": {"pages": 60, "delta_days": 2}}


class ProzorroProcessor:
    """
    This class is responsible for interaction with Prozorro API.
    """

    SEARCH_TENDERS_URL = f"{CONFIG.PROZORRO.API_URL}/search/tenders"
    GET_TENDER_URL = f"{CONFIG.PROZORRO.API_URL}/tenders"

    def __init__(self):
        pass

    @staticmethod
    def get_tender_list(load_date: int, status: str = "complete", limit: int = None) -> list:
        """Get the list of tenders from Prozorro API in the specified filters.
        The method will get all the pages of tenders and return the list of tenders.

        :param start_date: start date of the tenders to get
        :param end_date: end date of the tenders to get
        :param status: status of tenders to get, defaults to "complete"
        :return: list of extracted tenders
        """
        tenders = []
        processed_tenders_id = []
        date_to_load = date.today() - timedelta(days=load_date)
        while date_to_load < date.today():
            ProzorroProcessor._validate_dates(date_to_load, date_to_load)
            logger.info(f"Getting {status} tenders from Prozorro API from {date_to_load}")
            if status == "complete":
                first_page = ProzorroProcessor._get_tenders_page(None, date_to_load, status)
            else:
                first_page = ProzorroProcessor._get_tenders_page(date_to_load, None, status)

            tender_count = limit or len(first_page["data"])  # to limit per page count if limit set
            for tender in first_page["data"][:tender_count]:
                if tender["tenderID"] not in processed_tenders_id:
                    tenders.append(tender)
                    processed_tenders_id.append(tender["tenderID"])

            logger.info("Current tenders count: " + str(len(tenders)))

            # get only limit number of tenders, if limit is set
            total = int(first_page["total"]) if limit is None or int(first_page["total"]) < limit else limit
            logger.info(f"Found {total} tenders")
            page_count = math.ceil(total / int(first_page["per_page"]))
            page_count = page_count if page_count <= statuses[status]["pages"] else statuses[status]["pages"]
            logger.info(f"Getting tenders from {page_count} pages")

            for page_number in range(2, page_count + 1):
                if status == "complete":
                    page = ProzorroProcessor._get_tenders_page(None, date_to_load, status, page_number)
                else:
                    page = ProzorroProcessor._get_tenders_page(date_to_load, None, status, page_number)
                for tender in page["data"][:tender_count]:
                    if tender["tenderID"] not in processed_tenders_id:
                        tenders.append(tender)
                        processed_tenders_id.append(tender["tenderID"])
                logger.info("Current tenders count: " + str(len(tenders)))

            date_to_load += timedelta(days=1)  # get tenders from every day
        return tenders

    @staticmethod
    def _get_tenders_page(
        start_date: date, end_date: date, status: str = "complete", page_number: int = 1, retry_count: int = 5
    ) -> dict:
        """
        Get the page of tenders from Prozorro API.
        If the request fails with 429 status code, it will retry the request
        """

        params = {"page": page_number}
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
        return requests.get(f"{ProzorroProcessor.GET_TENDER_URL}/{tender_id}")

    @staticmethod
    def get_tender_details(tender_id: str):
        """
        Make a request to the Prozorro API to get detailed information about tender and return that information.
        :param tender_id: ID of tender to get details for
        :return: detailed information about tender
        """
        tender_details_response = ProzorroProcessor.get_tender_details_response(tender_id)
        if tender_details_response.status_code == 200:
            return json.loads(tender_details_response.content)
        elif tender_details_response.status_code == 429:
            timeout = int(tender_details_response.headers.get("Retry-After", 10))
            logger.error(
                f"Failed to get details for tender {tender_id}: {tender_details_response.status_code}. Retrying in {timeout} sec..."
            )
            time.sleep(timeout)
            return ProzorroProcessor.get_tender_details(tender_id)
        else:
            raise Exception(f"Failed to get tender details for {tender_id}: {tender_details_response.status_code}.")

    @staticmethod
    def get_tender_details_list(load_date: date, status: str = "complete", limit: int = None) -> list:
        """
        Get the list of tenders from Prozorro API in the specified filters
        and request detailed info about each tender from list mentioned above.
        Return only successfully retrieved tenders details.
        :param start_date: start date of the tenders to get
        :param end_date: end date of the tenders to get
        :param status: status of tenders to get, defaults to "complete"
        :return: list of extracted tenders details
        """
        tenders_without_details = ProzorroProcessor.get_tender_list(load_date, status, limit)
        tender_details_list = []
        logger.info("Getting tenders details")
        for tender in tenders_without_details:
            try:
                tender_details = ProzorroProcessor.get_tender_details(tender["tenderID"])
                tender_details_list.append(tender_details)
                # logger.info(f"Successfully retrieved tender details for tender: {tender['tenderID']}")
            except Exception as ex:
                logger.error(f"ERROR: {ex}")
        return tender_details_list

    @staticmethod
    def get_edrpou_from_tender(tender_info: dict) -> Optional[str]:
        try:
            edrpou = tender_info["awards"][0]["suppliers"][0]["identifier"]["id"]
            # logger.info(f"Got EDRPOU: {edrpou}")
        except Exception as e:
            # logger.error(f"Exception occurred during EDRPOU parsing from Tender info: {e}")
            return None
        return edrpou


if __name__ == "__main__":
    pass
