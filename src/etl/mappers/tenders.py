import uuid
from datetime import datetime
import re
import math
from abc import ABC, abstractmethod
from config import CONFIG
from etl.models import tenders
from etl.models.tenders import DateDim
from etl.utils import functions

from etl.utils.location_hierarchy_builder import build_entity_kattotg_hierarchy, build_tender_kattotg_hierarchy, \
    get_coordinates
from services import MongoService


class TenderMapperV1(ABC):
    def __init__(self, tender: dict) -> None:
        self._tender = tender

    @abstractmethod
    def map(self) -> list:
        pass

    def map_to_tender_info(self, location_id: str) -> tenders.TenderInfo:
        classifier = self._tender["generalClassifier"]["description"]
        try:
            classifier_code = re.findall(r"\d+-\d", classifier)[0]
        except IndexError:
            classifier_code = "n/a"
        dk_hierarchy = functions.get_DK_record(classifier_code)
        division, group, class_name = "n/a", "n/a", "n/a"
        if dk_hierarchy:
            if dk_hierarchy["Division"]:
                try:
                    division = dk_hierarchy["Division"].split(" ", 1)[1]
                except:
                    pass
            if dk_hierarchy["Group"]:
                try:
                    group = dk_hierarchy["Group"].split(" ", 1)[1]
                except:
                    pass
            if dk_hierarchy["Class"]:
                try:
                    class_name = dk_hierarchy["Class"].split(" ", 1)[1]
                except:
                    pass

        return tenders.TenderInfo(
            tender_id=self._tender["tenderID"],
            title=self._tender["title"],
            location=location_id,
            division=division,
            group=group,
            class_name=class_name
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
                quarter=(open_datetime.month + 2) // 3,
                day_of_week=open_datetime.weekday(),
                day_of_month=open_datetime.day,
            ),
            tenders.DateDim(
                day=close_datetime.strftime("%Y-%m-%d"),
                month=close_datetime.strftime("%Y-%m"),
                year=close_datetime.year,
                quarter=(open_datetime.month + 2) // 3,
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
                coordinares = get_coordinates(
                    f"{address}, місто {city_katottg[0]}, область {region_katottg[0]}, Україна")
                lat = round(coordinares["lat"], 6)
                lon = round(coordinares["lng"], 6)
            except Exception:
                return None, None, None
        else:
            return None, None, None

        return (
            tenders.StreetAddress(
                id=str(lat) + str(lon),
                address=address,
                latitude=lat,
                longitude=lon,
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
        tender_info = self.map_to_tender_info(streetaddress.id if streetaddress else 'n/a')
        procurement_entity = self.map_to_procurement_entity()
        open_date, close_date = self.map_to_date_dim()

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
            amount=self._tender["value"]["amount"] if self._tender["value"] else 0,
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
        mongo = MongoService(CONFIG.MONGO.URI, CONFIG.MONGO.DB_NAME)
        streetaddress1, city1, region1 = self.map_to_location("tender")
        tender_info = self.map_to_tender_info(streetaddress1.id if streetaddress1 else "n/a")
        procurement_entity = self.map_to_procurement_entity()
        open_date, close_date = self.map_to_date_dim()

        streetaddress2, city2, region2 = self.map_to_location("performer")
        performer = self.map_to_performer(streetaddress2.id if streetaddress2 else "n/a")
        if not performer:
            try:
                performer_id = self._tender["awards"][0]["suppliers"][0]["identifier"]["id"]
            except:
                performer_id = "n/a"
        else:
            performer_id = performer.performer_id
            try:
                mongo.upsert_dk_to_kved(CONFIG.MONGO.DK_TO_KVED_COLLECTION, tender_info.division, tender_info.group,
                                        tender_info.class_name, performer.class_code)
            except:
                pass
        tender_closed = self.map_to_tender_closed(
            open_date.day, close_date.day, tender_info.tender_id, procurement_entity.entity_id, performer_id
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
            amount=self._tender["awards"][0]["value"]["amount"] if self._tender["awards"][0]["value"] else 0,
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
        edrpou_type, edrpou_name, edrpou_phone, edrpou_email, edrpou_kved = functions.get_performer_info(
            award["identifier"]["id"])
        print("EDRPOU INFO: ", edrpou_type, edrpou_name, edrpou_phone, edrpou_email, edrpou_kved)
        if not edrpou_type:
            return None
        kved_hierarchy = functions.get_KVED_record(edrpou_kved)

        return tenders.Performer(
            performer_id=award["identifier"]["id"],
            organization_type=edrpou_type if edrpou_type else "n/a",
            organization_name=edrpou_name if edrpou_name else "n/a",
            organization_phone=edrpou_phone if edrpou_phone else "n/a",
            organization_email=edrpou_email if edrpou_email else "n/a",
            location=location_id,
            class_name=kved_hierarchy["name"] if kved_hierarchy else "n/a",
            section_name=functions.get_kved_code_name("section_code",
                                                      kved_hierarchy["section_code"]) if kved_hierarchy else "n/a",
            partition_name=functions.get_kved_code_name("partition_code",
                                                        kved_hierarchy["partition_code"]) if kved_hierarchy else "n/a",
            group_name=functions.get_kved_code_name("group_code",
                                                    kved_hierarchy["group_code"]) if kved_hierarchy else "n/a",
            section_code=kved_hierarchy["section_code"] if kved_hierarchy else 'n/a',
            partition_code=kved_hierarchy["partition_code"] if kved_hierarchy else 'n/a',
            group_code=kved_hierarchy["group_code"] if kved_hierarchy else 'n/a',
            class_code=kved_hierarchy["class_code"] if kved_hierarchy else 'n/a',
        )
