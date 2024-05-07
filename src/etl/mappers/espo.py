from datetime import datetime
from etl.models import espo, tenders
from etl.utils.location_hierarchy_builder import get_coordinates, build_kattotg_hierarchy
from etl.utils import functions, clickhouse_utils


class ManagerMapperV1:
    def __init__(self, data: dict):
        self._data = data

    def map(self):
        return [self.map_to_manager()]

    def map_to_manager(self) -> espo.Manager:
        return espo.Manager(
            manager_id=self._data["id"],
            name=self._data["name"],
        )


class CampaignMapperV1:
    def __init__(self, data: dict):
        self._data = data

    def map(self):
        return [self.map_to_campaign()]

    def map_to_campaign(self) -> espo.Campaign:

        duration = datetime.fromisoformat(self._data["endDate"]) - datetime.fromisoformat(self._data["startDate"])
        return espo.Campaign(
            campaign_id=self._data["id"],
            name=self._data["name"],
            campaign_type=self._data["type"],
            start_date=self._data["startDate"],
            end_date=self._data["endDate"],
            duration_hours=int(duration.total_seconds() // 3600),
        )


class AccountMapperV1:

    def __init__(self, data: dict):
        self._data = data

    def map(self):
        streetaddress, city, region = self.map_to_location()
        performer = self.map_to_performer(streetaddress.id)
        return [streetaddress, city, region, performer]

    def map_to_performer(self, streetaddress_id: str) -> tenders.Performer:

        kved_hierarchy = functions.get_KVED_record(self._data["cKveds"][0])

        return tenders.Performer(
            performer_id=self._data["cEdrpou"],
            organization_name=self._data["name"],
            organization_type=self._data["cOrgType"],
            organization_email=self._data["emailAddress"],
            organization_phone=self._data["phoneNumber"],
            location=streetaddress_id,
            class_name=kved_hierarchy["name"] if kved_hierarchy else "n/a",
            section_name=(
                functions.get_kved_code_name("section_code", kved_hierarchy["section_code"])
                if kved_hierarchy
                else "n/a"
            ),
            partition_name=(
                functions.get_kved_code_name("partition_code", kved_hierarchy["partition_code"])
                if kved_hierarchy
                else "n/a"
            ),
            group_name=(
                functions.get_kved_code_name("group_code", kved_hierarchy["group_code"]) if kved_hierarchy else "n/a"
            ),
            section_code=kved_hierarchy["section_code"] if kved_hierarchy else "n/a",
            partition_code=kved_hierarchy["partition_code"] if kved_hierarchy else "n/a",
            group_code=kved_hierarchy["group_code"] if kved_hierarchy else "n/a",
            class_code=kved_hierarchy["class_code"] if kved_hierarchy else "n/a",
        )

    def map_to_location(self) -> tuple[tenders.StreetAddress, tenders.City, tenders.Region]:

        address, city_katottg, region_katottg = build_kattotg_hierarchy(
            self._data["billingAddressCity"], self._data["billingAddressState"], self._data["billingAddressStreet"]
        )

        city_name, city_katottg = city_katottg if city_katottg else ("n/a", "n/a")
        region_name, region_katottg = region_katottg if region_katottg else ("n/a", "n/a")

        coordinares = get_coordinates(f"{address}, місто {city_name}, область {region_name}, Україна")
        lat = round(coordinares["lat"], 6)
        lon = round(coordinares["lng"], 6)

        return (
            tenders.StreetAddress(
                id=str(lat) + str(lon),
                address=address,
                latitude=lat,
                longitude=lon,
                city_katottg=city_katottg,
                region_katottg=region_katottg,
                city_name=city_name,
                region_name=region_name,
            ),
            tenders.City(
                city_name=city_name,
                city_katottg=city_katottg,
                region_katottg=region_katottg,
                region_name=region_name,
            ),
            tenders.Region(
                region_name=region_name,
                region_katottg=region_katottg,
            ),
        )


class LeadActivityMapperV1:

    def __init__(self, data: dict, prev_data: dict = {}, clickhouse=None):
        self._data = data
        self._prev_data = prev_data
        self.clickhouse = clickhouse

    def map(self):

        manager = self.map_to_manager()
        curr_stage, prev_stage = self.map_to_stage()
        date_dim = self.map_to_date_dim()
        lead_activity = self.map_to_lead_activity(prev_stage.stage_id, curr_stage.stage_id, date_dim.day)
        return [manager, prev_stage, curr_stage, date_dim, lead_activity]

    def map_to_manager(self) -> espo.Manager:
        return espo.Manager(
            manager_id=self._data["createdById"],
            name=self._data["createdByName"],
        )

    def map_to_stage(self) -> tuple[espo.Stage, espo.Stage]:
        stage_id = self._data["id"] + self._data["status"]
        prev_stage_from_kh = clickhouse_utils.get_by_id(self.clickhouse, "Stage", self._prev_data.get("curr_stage_id", ""))
        stage_changed = self._data["status"] != prev_stage_from_kh.get("stage_name", "")
        prev_stage = espo.Stage(
            stage_id="n/a" if not stage_changed else prev_stage_from_kh["stage_id"],
            stage_name="n/a" if not stage_changed else prev_stage_from_kh["stage_name"],
            stage_success=0 if not stage_changed else prev_stage_from_kh["stage_success"],
            became_opportunity=False if not stage_changed else prev_stage_from_kh["became_opportunity"],
            activity_id="n/a" if not stage_changed else self._data["id"],
        )

        return (
            espo.Stage(
                stage_id=stage_id,
                stage_name=self._data["status"],
                stage_success=self._data["cSuccessRate"],
                became_opportunity=False,
                activity_id=self._data["id"],
            ),
            prev_stage,
        )

    def map_to_date_dim(self) -> tenders.DateDim:
        action_datetime = datetime.fromisoformat(self._data["createdAt"])
        return tenders.DateDim(
            day=action_datetime.strftime("%Y-%m-%d"),
            month=action_datetime.strftime("%Y-%m"),
            year=action_datetime.year,
            quarter=(action_datetime.month + 2) // 3,
            day_of_week=action_datetime.weekday(),
            day_of_month=action_datetime.day,
        )

    def map_to_lead_activity(self, prev_stage_id: str, curr_stage_id: str, time_id: str) -> espo.LeadActivity:
        return espo.LeadActivity(
            id=self._data["id"],
            success_rate=self._data.get("cSuccessRate", 0),
            time_from_prev_stage=self._data.get("timeFromPrevStage", 0),
            activities_from_last_stage=self._data.get("activitiesFromLastStage", 0),
            feedback_from_last_stage=self._data.get("cFeedbackFromLastStage", 0),
            manager_id=self._data["createdById"],
            performer_id=self._data["cEdrpou"],
            campaing_id=self._data["campaignId"],
            channel_id=self._data["cChannel"],
            prev_stage_id=prev_stage_id,
            curr_stage_id=curr_stage_id,
            time_id=time_id,
        )


class SaleActivityMapperV1:

    def __init__(self):
        pass

    def map(self, data):
        return {
            "name": data["name"],
            "email": data["email"],
            "phone": data["phone"],
            "address": data["address"],
            "product": data["product"],
            "quantity": data["quantity"],
            "price": data["price"],
        }
