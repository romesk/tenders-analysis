from dataclasses import dataclass


@dataclass
class SaleActivity:
    price: float
    time_spent: int
    estimated_profit: float
    manager_seniority: str
    result_id: str
    tender_id: str
    manager_id: str
    performer_id: str
    start_all_time_id: str
    end_all_time_id: str


@dataclass
class Result:
    result_id: str
    stage: str


@dataclass
class Manager:
    manager_id: str
    name: str


@dataclass
class LeadActivity:
    success_rate: int
    time_from_prev_stage: int
    activities_from_last_stage: int
    feedback_from_last_stage: int
    manager_id: str
    performer_id: str
    campaign_id: str
    channel_id: str
    prev_stage_id: str
    curr_stage_id: str
    time_id: str


@dataclass
class Campaign:
    campaign_id: str
    campaign_type: str
    start_date: str
    end_date: str
    duration_hours: int

    # replace None with empty values
    def __post_init__(self):
        for field in self.__dataclass_fields__:
            if getattr(self, field) is None:
                # check expected type and assign default value
                if self.__dataclass_fields__[field].type == str:
                    setattr(self, field, "n/a")
                elif self.__dataclass_fields__[field].type == int:
                    setattr(self, field, -1)


@dataclass
class Channel:
    channel_id: str
    channel_name: str


@dataclass
class Stage:
    stage_id: str
    stage_name: str
