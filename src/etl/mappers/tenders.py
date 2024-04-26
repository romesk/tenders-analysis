import uuid
from datetime import datetime
import re
import math
from abc import ABC, abstractmethod
from typing import Union, Tuple, Any
from etl.models import tenders
from etl.models.tenders import DateDim
from etl.utils import functions

from etl.utils.location_hierarchy_builder import build_entity_kattotg_hierarchy, build_tender_kattotg_hierarchy, get_coordinates


class TenderMapperV1(ABC):
    def __init__(self, tender: dict) -> None:
        self._tender = tender

    @abstractmethod
    def map(self) -> list:
        pass

    def map_to_tender_info(self, location_id: str) -> tenders.TenderInfo:
        classifier = self._tender["generalClassifier"]["description"]
        classifier_code = re.findall(r"\d+-\d", classifier)[0]
        dk_hierarchy = functions.get_DK_record(classifier_code)

        return tenders.TenderInfo(
            tender_id=self._tender["tenderID"],
            title=self._tender["title"],
            location=location_id,
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

    def map_to_date_dim(self) -> tuple[DateDim, DateDim]:
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

    def map_to_location(self, map_type: str = "tender"):
        # TODO: implement location hierarch mapping
        # all 5 levels can be returned here and inserted in map() func.
        # use separate Model for each location level
        try:
            if map_type == "tender":
                address, city_katottg, region_katottg = build_tender_kattotg_hierarchy(
                    self._tender
                )
            else:
                address, city_katottg, region_katottg = build_entity_kattotg_hierarchy(
                    self._tender["awards"][0]["suppliers"][0]["identifier"]["id"]
                )
        except Exception:
            return None, None, None

        if address is not None and city_katottg is not None and region_katottg is not None:
            try:
                coordinares = get_coordinates(f"{address}, місто {city_katottg[0]}, область {region_katottg[0]}, Україна")
            except Exception:
                return None, None, None
        else:
            return None, None, None
        return (
            tenders.StreetAddress(
                id=str(coordinares["lat"]) + str(coordinares["lng"]),
                address=address,
                latitude=coordinares["lat"],
                longitude=coordinares["lng"],
                city_katottg=city_katottg[1],
                city_name=city_katottg[0],
                region_katottg=region_katottg[1],
                region_name=region_katottg[0]
            ),
            tenders.City(
                city_katottg=city_katottg[1],
                city_name=city_katottg[0],
                region_katottg=region_katottg[1],
                region_name=region_katottg[0]
            ),
            tenders.Region(
                region_katottg=region_katottg[1],
                region_name=region_katottg[0],
            ),
        )


class TenderOpenedMapperV1(TenderMapperV1):
    def __init__(self, tender: dict) -> None:
        super().__init__(tender)
        self._tender = tender

    def map(self) -> list:
        """Maps the tender from MongoDB to ClickHouse models.

        :return: list of ClickHouse models to be pushed to ClickHouse
        """
        streetaddress, city, region = self.map_to_location("tender")
        tender_info = self.map_to_tender_info(streetaddress if streetaddress else 'n/a')
        procurement_entity = self.map_to_procurement_entity()
        close_date, open_date = self.map_to_date_dim()

        tender_opened = self.map_to_tender_opened(
            open_date.day, close_date.day, tender_info.tender_id, procurement_entity.entity_id
        )

        # fact must be last in the list for the correct order of insertion
        return [tender_info, procurement_entity, open_date, close_date, streetaddress, city, region, tender_opened]

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
        streetaddress1, city1, region1 = self.map_to_location("tender")
        tender_info = self.map_to_tender_info(streetaddress1.id if streetaddress1 else "n/a")
        procurement_entity = self.map_to_procurement_entity()
        close_date, open_date = self.map_to_date_dim()

        streetaddress2, city2, region2 = self.map_to_location("performer")
        performer = self.map_to_performer(streetaddress2.id if streetaddress2 else "n/a")

        tender_closed = self.map_to_tender_closed(
            open_date.day, close_date.day, tender_info.tender_id, procurement_entity.entity_id, performer.performer_id
        )

        return [
            tender_info,
            procurement_entity,
            open_date,
            close_date,
            streetaddress1,
            streetaddress2,
            city1,
            city2,
            region1,
            region2,
            performer,
            tender_closed,
        ]

    def map_to_tender_closed(
        self, open_date: str, close_date: str, tender_id: str, procurement_id: str, performer_id: str
    ) -> tenders.TenderClosed:
        end_date = datetime.fromisoformat(self._tender["tenderPeriod"]["endDate"])
        start_date = datetime.fromisoformat(self._tender["tenderPeriod"]["startDate"])

        duration = (end_date - start_date).days

        return tenders.TenderClosed(
            amount=self._tender["awards"][0]["value"]["amount"],
            duration=duration,
            participant_count=len(self._tender["bids"]),
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
            class_name=kved_hierarchy["name"] if kved_hierarchy else None,
            section_name=functions.get_kved_code_name("section_code", kved_hierarchy["section_code"]) if kved_hierarchy else None,
            partition_name=functions.get_kved_code_name("partition_code", kved_hierarchy["partition_code"]) if kved_hierarchy else None,
            group_name=functions.get_kved_code_name("group_code", kved_hierarchy["group_code"]) if kved_hierarchy else None,
            section_code=kved_hierarchy["section_code"] if kved_hierarchy else None,
            partition_code=kved_hierarchy["partition_code"] if kved_hierarchy else None,
            group_code=kved_hierarchy["group_code"] if kved_hierarchy else None,
            class_code=kved_hierarchy["class_code"] if kved_hierarchy else None,
        )
