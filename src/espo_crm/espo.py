import requests

from config import CONFIG
from utlis.logger import get_logger


logger = get_logger("EspoCRM")


class EspoCRM:

    PAGE_SIZE = 10

    def __init__(self) -> None:
        self._headers = {"X-Api-Key": f"{CONFIG.ESPO.API_KEY}"}

    def get_accounts(self) -> list[dict]:
        """Get all accounts from EspoCRM. All fields are included."""

        response = self._get_accounts()
        total = response["total"]
        logger.info(f"Total accounts in EspoCRM: {total}")

        accounts = response["list"]
        pages = total // self.PAGE_SIZE + 1
        for page in range(1, pages):
            response = self._get_accounts(offset=page * self.PAGE_SIZE)
            accounts += response["list"]

        return accounts

    def _get_accounts(self, offset: int = 0, max_size: int = PAGE_SIZE) -> dict:
        """Account endpoint wrapper for EspoCRM API. Returns the json response."""

        url = f"{CONFIG.ESPO.API_URL}/Account"
        params = {"offset": offset, "maxSize": max_size}

        response = requests.get(url, headers=self._headers, params=params)

        if response.status_code != 200:
            raise Exception(
                f"Failed to get accounts from EspoCRM: {response.text}. [{response.status_code}]"
            )

        return response.json()

    def get_oppotunities(self) -> list[dict]:
        """Get all opportunities from EspoCRM. All fields are included."""

        response = self._get_opportunities()
        total = response["total"]
        logger.info(f"Total opportunities in EspoCRM: {total}")

        opportunities = response["list"]
        pages = total // self.PAGE_SIZE + 1
        for page in range(1, pages):
            response = self._get_opportunities(offset=page * self.PAGE_SIZE)
            opportunities += response["list"]

        return opportunities

    def _get_opportunities(self, offset: int = 0, max_size: int = PAGE_SIZE) -> dict:
        """Opportunity endpoint wrapper for EspoCRM API. Returns the json response."""

        url = f"{CONFIG.ESPO.API_URL}/Opportunity"
        params = {"offset": offset, "maxSize": max_size}

        response = requests.get(url, headers=self._headers, params=params)

        if response.status_code != 200:
            raise Exception(
                f"Failed to get opportunities from EspoCRM: {response.text}. [{response.status_code}]"
            )

        return response.json()

    def get_streams(self, entity_type: str, entity_id: str) -> list[dict]:
        """Get all streams from EspoCRM. All fields are included."""

        response = self._get_streams(entity_type, entity_id)
        total = response["total"]
        logger.info(
            f"Total streams for {entity_type} '{entity_id}' in EspoCRM: {total}"
        )

        streams = response["list"]
        pages = total // self.PAGE_SIZE + 1
        for page in range(1, pages):
            response = self._get_streams(offset=page * self.PAGE_SIZE)
            streams += response["list"]

        return streams

    def _get_streams(
        self,
        entity_type: str,
        entity_id: str,
        offset: int = 0,
        max_size: int = PAGE_SIZE,
    ) -> dict:
        """Stream endpoint wrapper for EspoCRM API. Returns the json response."""

        url = f"{CONFIG.ESPO.API_URL}/{entity_type}/{entity_id}/stream"
        params = {"offset": offset, "maxSize": max_size}

        response = requests.get(url, headers=self._headers, params=params)

        if response.status_code != 200:
            raise Exception(
                f"Failed to get streams from EspoCRM: {response.text}. [{response.status_code}]"
            )

        return response.json()
