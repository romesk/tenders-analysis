import requests

from config import CONFIG
from utlis.logger import get_logger


logger = get_logger("EspoCRM")


class EspoCRM:

    PAGE_SIZE = 100

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
            raise Exception(f"Failed to get accounts from EspoCRM: {response.text}. [{response.status_code}]")

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
            raise Exception(f"Failed to get opportunities from EspoCRM: {response.text}. [{response.status_code}]")

        return response.json()

    def get_streams(self, entity_type: str, entity_id: str) -> list[dict]:
        """Get all streams from EspoCRM. All fields are included."""

        response = self._get_streams(entity_type, entity_id)
        total = response["total"]
        logger.info(f"Total streams for {entity_type} '{entity_id}' in EspoCRM: {total}")

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
            raise Exception(f"Failed to get streams from EspoCRM: {response.text}. [{response.status_code}]")

        return response.json()

    def get_managers(self) -> list[dict]:
        """Get all managers from EspoCRM. All fields are included."""

        response = self._get_managers()
        total = response["total"]
        logger.info(f"Total managers in EspoCRM: {total}")

        managers = response["list"]
        pages = total // self.PAGE_SIZE + 1
        for page in range(1, pages):
            response = self._get_managers(offset=page * self.PAGE_SIZE)
            managers += response["list"]

        return managers

    def _get_managers(self, offset: int = 0, max_size: int = PAGE_SIZE) -> dict:
        """Manager endpoint wrapper for EspoCRM API. Returns the json response."""

        url = f"{CONFIG.ESPO.API_URL}/User"
        params = {
            "offset": offset,
            "maxSize": max_size,
            "userType": "internal",
            "select": "emailAddress,userName,firstName,lastName,middleName,name",
        }

        response = requests.get(url, headers=self._headers, params=params)

        if response.status_code != 200:
            raise Exception(f"Failed to get managers from EspoCRM: {response.text}. [{response.status_code}]")

        return response.json()

    def get_leads(self) -> list[dict]:
        """Get all leads from EspoCRM. All fields are included."""

        response = self._get_leads()
        total = response["total"]
        logger.info(f"Total leads in EspoCRM: {total}")

        leads = response["list"]
        pages = total // self.PAGE_SIZE + 1
        for page in range(1, pages):
            response = self._get_leads(offset=page * self.PAGE_SIZE)
            leads += response["list"]

        return leads

    def _get_leads(self, offset: int = 0, max_size: int = PAGE_SIZE) -> dict:
        """Lead endpoint wrapper for EspoCRM API. Returns the json response."""

        url = f"{CONFIG.ESPO.API_URL}/Lead"
        params = {"offset": offset, "maxSize": max_size}

        response = requests.get(url, headers=self._headers, params=params)

        if response.status_code != 200:
            raise Exception(f"Failed to get leads from EspoCRM: {response.text}. [{response.status_code}]")

        return response.json()

    def get_campaings(self) -> list[dict]:
        """Get all campaings from EspoCRM. All fields are included."""

        response = self._get_campaings()
        total = response["total"]
        logger.info(f"Total campaings in EspoCRM: {total}")

        campaings = response["list"]
        pages = total // self.PAGE_SIZE + 1
        for page in range(1, pages):
            response = self._get_campaings(offset=page * self.PAGE_SIZE)
            campaings += response["list"]

        return campaings

    def _get_campaings(self, offset: int = 0, max_size: int = PAGE_SIZE) -> dict:
        """Campaing endpoint wrapper for EspoCRM API. Returns the json response."""

        url = f"{CONFIG.ESPO.API_URL}/Campaign"
        params = {"offset": offset, "maxSize": max_size}

        response = requests.get(url, headers=self._headers, params=params)

        if response.status_code != 200:
            raise Exception(f"Failed to get campaings from EspoCRM: {response.text}. [{response.status_code}]")

        return response.json()

    def add_account(self, data: dict) -> dict:
        """Add an account to EspoCRM. Returns the json response."""

        url = f"{CONFIG.ESPO.API_URL}/Account"
        response = requests.post(url, headers=self._headers, json=data)

        if response.status_code != 200:
            raise Exception(f"Failed to add account to EspoCRM: {response.text}. [{response.status_code}]")

        return response.json()
