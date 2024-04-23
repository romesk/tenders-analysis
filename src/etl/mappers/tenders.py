from datetime import datetime
import re
import math
from abc import ABC, abstractmethod
from typing import Union
from etl.models import tenders
from etl.utils import functions


class TenderMapperV1(ABC):
    def __init__(self, tender: dict) -> None:
        self._tender = tender

    @abstractmethod
    def map(self) -> list:
        pass

    def map_to_tender_info(self) -> tenders.TenderInfo:

        classifier = self._tender["generalClassifier"]["description"]
        classifier_code = re.findall(r"\d+-\d", classifier)[0]
        dk_hierarchy = functions.get_DK_record(classifier_code)

        return tenders.TenderInfo(
            tender_id=self._tender["tenderID"],
            title=self._tender["title"],
            division=dk_hierarchy["Division"] if dk_hierarchy else None,
            group=dk_hierarchy["Group"] if dk_hierarchy else None,
            class_name=dk_hierarchy["Class"] if dk_hierarchy else None,
            category=dk_hierarchy["Category"] if dk_hierarchy else None,
            clarification=dk_hierarchy["Clarification"] if dk_hierarchy else None,
        )

    def map_to_procurement_entity(self) -> tenders.ProcurementEntity:
        entity = self._tender["procuringEntity"]
        return tenders.ProcurementEntity(
            entity_id=entity["identifier"]["id"],
            legal_name=entity["name"],
            contact_name=entity["contactPoint"]["name"],
            contact_phone=entity["contactPoint"].get("telephone", None),
        )

    def map_to_date_dim(self) -> Union[tenders.DateDim, tenders.DateDim]:
        """Return two DateDim objects for open and close time"""

        open_time = self._tender["tenderPeriod"]["startDate"]
        close_time = self._tender["tenderPeriod"]["endDate"]
        open_datetime = datetime.fromisoformat(open_time)
        close_datetime = datetime.fromisoformat(close_time)

        return (
            tenders.DateDim(
                day=open_datetime.strftime("%Y-%m-%d"),
                month=open_datetime.strftime("%Y-%m"),
                year=open_datetime.year,
                quarter=str((open_datetime.month + 2) // 3),
                day_of_week=open_datetime.weekday(),
                day_of_month=open_datetime.day,
            ),
            tenders.DateDim(
                day=close_datetime.strftime("%Y-%m-%d"),
                month=close_datetime.strftime("%Y-%m"),
                year=close_datetime.year,
                quarter=str((open_datetime.month + 2) // 3),
                day_of_week=close_datetime.weekday(),
                day_of_month=close_datetime.day,
            ),
        )

    def map_to_location(self) -> str:

        # TODO: implement location hierarch mapping
        # all 5 levels can be returned here and inserted in map() func.
        # use separate Model for each location level
        return self._tender["items"][0]["deliveryAddress"].get("locality", 'n/a')


class TenderOpenedMapperV1(TenderMapperV1):
    def __init__(self, tender: dict) -> None:
        super().__init__(tender)
        self._tender = tender

    def map(self) -> list:
        """Maps the tender from MongoDB to ClickHouse models.

        :return: list of ClickHouse models to be pushed to ClickHouse
        """

        tender_info = self.map_to_tender_info()
        procurement_entity = self.map_to_procurement_entity()
        close_date, open_date = self.map_to_date_dim()

        tender_opened = self.map_to_tender_opened(
            open_date.day, close_date.day, tender_info.tender_id, procurement_entity.entity_id
        )

        # fact must be last in the list for the correct order of insertion
        return [tender_info, procurement_entity, open_date, close_date, tender_opened]

    def map_to_tender_opened(
        self, open_date: str, close_date: str, tender_id: str, procurement_id: str
    ) -> tenders.TenderOpened:
        # open_date, close_date = self.map_to_date_dim()
        end_date = datetime.fromisoformat(self._tender["tenderPeriod"]["endDate"])
        time_to_end = math.ceil((end_date - datetime.now(end_date.tzinfo)).total_seconds() // 60)

        return tenders.TenderOpened(
            amount=self._tender["value"]["amount"],
            time_to_end=time_to_end,
            open_time_id=open_date,
            close_time_id=close_date,
            tender_id=tender_id,
            procurement_id=procurement_id,
        )


class TenderClosedMapperV1(TenderMapperV1):
    def __init__(self, tender: dict) -> None:
        super().__init__(tender)
        self._tender = tender

    def map(self) -> list:
        """Maps the tender from MongoDB to ClickHouse models.

        :return: list of ClickHouse models to be pushed to ClickHouse
        """

        tender_info = self.map_to_tender_info()
        procurement_entity = self.map_to_procurement_entity()
        close_date, open_date = self.map_to_date_dim()

        # location_l1, location_l2, location_l3, ... = self.map_to_location()
        performer = self.map_to_performer('last_location_level.id')

        tender_closed = self.map_to_tender_closed(
            open_date.day, close_date.day, tender_info.tender_id, procurement_entity.entity_id, performer.performer_id
        )

        return [tender_info, procurement_entity, open_date, close_date, location, performer, tender_closed]

    def map_to_tender_closed(
        self, open_date: str, close_date: str, tender_id: str, procurement_id: str, performer_id: str
    ) -> tenders.TenderClosed:

        end_date = datetime.fromisoformat(self._tender["tenderPeriod"]["endDate"])
        start_date = datetime.fromisoformat(self._tender["tenderPeriod"]["startDate"])

        duration = (end_date - start_date).days

        return tenders.TenderClosed(
            amount=self._tender["awards"][0]["value"]["amount"],
            duration=duration,
            participant_count=len("bids"),
            open_time_id=open_date,
            close_time_id=close_date,
            tender_id=tender_id,
            procurement_id=procurement_id,
            performer_id=performer_id,
        )

    def map_to_performer(self, location_id: str) -> tenders.Performer:
        award = self._tender["awards"][0]["suppliers"][0]

        kved_hierarchy = functions.get_KVED_record(award["identifier"]["id"])

        return tenders.Performer(
            performer_id=award["identifier"]["id"],
            organization_type=award["name"].split(" ")[0],
            location=location_id,
            section_code=kved_hierarchy["section_code"] if kved_hierarchy else None,
            name=kved_hierarchy["name"] if kved_hierarchy else None,
            partition_code=kved_hierarchy["partition_code"] if kved_hierarchy else None,
            group_code=kved_hierarchy["group_code"] if kved_hierarchy else None,
            class_code=kved_hierarchy["class_code"] if kved_hierarchy else None,
        )