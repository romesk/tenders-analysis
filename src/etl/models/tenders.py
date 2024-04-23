from dataclasses import dataclass


@dataclass
class TenderOpened:
    amount: float
    time_to_end: float
    open_time_id: str
    close_time_id: str
    tender_id: str
    procurement_id: str


@dataclass
class TenderClosed:
    amount: int
    duration: int
    participant_count: int
    open_time_id: str
    close_time_id: str
    tender_id: str
    procurement_id: str
    performer_id: str


@dataclass
class TenderInfo:
    tender_id: str
    title: str
    division: str
    group: str
    class_name: str
    category: str
    clarification: str


@dataclass
class ProcurementEntity:
    entity_id: str
    legal_name: str
    contact_name: str
    contact_phone: str


@dataclass
class DateDim:
    day: str
    month: str
    year: int
    quarter: str
    day_of_week: int
    day_of_month: int

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
class Performer:
    performer_id: str
    organization_type: str
    location: str
    section_code: str
    name: str
    partition_code: str
    group_code: str
    class_code: str

    def __post_init__(self):
        for field in self.__dataclass_fields__:
            if getattr(self, field) is None:
                # check expected type and assign default value
                if self.__dataclass_fields__[field].type == str:
                    setattr(self, field, "n/a")
                elif self.__dataclass_fields__[field].type == int:
                    setattr(self, field, -1)


@dataclass
class StreetAddress:
    id: str
    address: str
    longitude: float
    latitude: float
    city_katottg: str
    region_katottg: str


@dataclass
class City:
    city_katottg: str
    city_name: str
    region_katottg: str


@dataclass
class Region:
    region_katottg: str
    region_name: str
