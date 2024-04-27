from datetime import datetime
from typing import Union
from etl.models import espo, tenders


class LeadActivityMapperV1:

    def __init__(self, data: dict):
        self._data = data

    def map(self):

        manager = self.map_to_manager()
        campaign = self.map_to_campaign()
        channel = self.map_to_channel()
        prev_stage, curr_stage = self.map_to_stage()
        date_dim = self.map_to_date_dim()
        lead_activity = self.map_to_lead_activity(
            manager.manager_id,
            campaign.campaign_id,
            channel.channel_id,
            prev_stage.stage_id,
            curr_stage.stage_id,
            date_dim.day,
        )
        return [manager, campaign, channel, prev_stage, curr_stage, date_dim, lead_activity]

    def map_to_manager(self) -> espo.Manager:
        return espo.Manager(
            manager_id=self._data["createdById"],
            name=self._data["createdByName"],
        )

    def map_to_campaign(self) -> espo.Campaign:
        # TODO: add getting campaings from espo to oltp
        return espo.Campaign(
            campaign_id=self._data.get("campaignId", "1"),
            campaign_type=self._data.get("campaignName", "main"),
            start_date="n/a",
            end_date="n/a",
            duration_hours=0,
        )

    def map_to_channel(self) -> espo.Channel:
        # TODO: add channel attribute to espo
        return espo.Channel(
            channel_id=self._data.get("channelId", "1"),
            channel_name=self._data.get("channelName", "email"),
        )

    def map_to_stage(self) -> Union[espo.Stage, espo.Stage]:

        return espo.Stage(
            stage_id=self._data["stage"],
            stage_name=self._data["stage"],
        ), espo.Stage(
            stage_id=self._data["lastStage"],
            stage_name=self._data["lastStage"],
        )

    def map_to_date_dim(self) -> tenders.DateDim:
        action_datetime = datetime.fromisoformat(self._data["createdAt"])
        return tenders.DateDim(
            day=action_datetime.strftime("%Y-%m-%d"),
            month=action_datetime.strftime("%Y-%m"),
            year=action_datetime.year,
            quarter=str((action_datetime.month + 2) // 3),
            day_of_week=action_datetime.weekday(),
            day_of_month=action_datetime.day,
        )

    def map_to_lead_activity(
        self, manager_id: str, campaign_id: str, channel_id: str, prev_stage_id: str, curr_stage_id: str, time_id: str
    ) -> espo.LeadActivity:
        return espo.LeadActivity(
            success_rate=self._data.get("probability", 0),
            time_from_prev_stage=self._data.get("timeFromPrevStage", 0),
            activities_from_last_stage=self._data.get("activitiesFromLastStage", 0),
            feedback_from_last_stage=self._data.get("feedbackFromLastStage", 0),
            manager_id=manager_id,
            performer_id='n/s',
            campaign_id=campaign_id,
            channel_id=channel_id,
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
