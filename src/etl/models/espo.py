from dataclasses import dataclass


@dataclass
class SaleActivity:
    price: float
    time_spent: int
    estimated_profit: float
    result_id: str
    tender_id: str
    manager_id: str
    performer_id: str
    end_all_time_id: str

    def __post_init__(self):
        for field in self.__dataclass_fields__:
            if getattr(self, field) is None:
                # check expected type and assign default value
                if self.__dataclass_fields__[field].type == str:
                    setattr(self, field, "n/a")
                elif self.__dataclass_fields__[field].type == int:
                    setattr(self, field, -1)


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
    id: str
    success_rate: int
    time_from_prev_stage: int
    activities_from_last_stage: int
    feedback_from_last_stage: int
    manager_id: str
    performer_id: str
    campaing_id: str
    channel_id: str
    prev_stage_id: str
    curr_stage_id: str
    time_id: str

    def __post_init__(self):
        for field in self.__dataclass_fields__:
            if getattr(self, field) is None:
                # check expected type and assign default value
                if self.__dataclass_fields__[field].type == str:
                    setattr(self, field, "n/a")
                elif self.__dataclass_fields__[field].type == int:
                    setattr(self, field, -1)


@dataclass
class Campaign:
    campaign_id: str
    campaign_type: str
    name: str
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
    stage_success: int
    became_opportunity: bool
    activity_id: str
