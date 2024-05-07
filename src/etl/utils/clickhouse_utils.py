from dataclasses import asdict
from services.clickhouse import ClickhouseService
from etl.models import tenders, espo
from utlis.logger import get_logger

# TODO: replace table names with constants
# TODO: make batch insertions
logger = get_logger("clickhouse-utils")


def get_by_id(clickhouse: ClickhouseService, table: str, id: str) -> dict:
    """Get a record by id from ClickHouse"""
    try:
        id_column = 'id'
        if table == 'Stage':
            id_column = 'stage_id'
        return clickhouse.query(f"SELECT * FROM {table} WHERE {id_column} = '{id}'").iloc[0].to_dict()
    except AttributeError:
        logger.error(f"Not found: {table} - {id}")
        return {}


def insert_clickhouse_model(clickhouse: ClickhouseService, model: any) -> None:
    """Insert a ClickHouse model into ClickHouse"""

    inserter = INSERTERS.get(model.__class__.__name__)
    if not inserter:
        logger.error(f"No inserter found for model: {model.__class__.__name__}")
        return
    inserter(clickhouse, model)


def insert_tender_opened(clickhouse: ClickhouseService, model: tenders.TenderOpened) -> None:
    """Insert a tender opened into ClickHouse"""

    logger.info(f"Inserting tender opened: {model.tender_id}")
    items = asdict(model)
    clickhouse.insert("TenderOpened", [list(items.values())], list(items.keys()))


def insert_tender_closed(clickhouse: ClickhouseService, model: dict) -> None:
    """Insert a tender closed into ClickHouse"""

    logger.info(f"Inserting tender closed: {model.tender_id}")
    items = asdict(model)
    clickhouse.insert("TenderClosed", [list(items.values())], list(items.keys()))
    try:
        clickhouse.remove("TenderOpened", "tender_id", model.tendeid)
    except:
        pass


def insert_tender_info(clickhouse: ClickhouseService, model: tenders.TenderInfo) -> None:
    """Insert a tender info into ClickHouse"""

    logger.info(f"Inserting tender info: {model.tender_id}")
    items = asdict(model)
    clickhouse.insert("TenderInfo", [list(items.values())], list(items.keys()))


def insert_procurement_entity(clickhouse: ClickhouseService, model: tenders.ProcurementEntity) -> None:
    """Insert a procurement entity into ClickHouse"""

    logger.info(f"Inserting procurement entity: {model.entity_id}")
    values = [v or "n/a" for v in asdict(model).values()]
    clickhouse.insert("ProcurementEntity", [values])


def insert_date_dim(clickhouse: ClickhouseService, model: tenders.DateDim) -> None:
    """Insert a date dimension into ClickHouse"""

    logger.info(f"Inserting date dimension: {model.day}")
    # items = {k: v or "n/a" for k, v in asdict(model).items()}
    items = asdict(model)
    clickhouse.insert("DateDim", [list(items.values())], list(items.keys()))


def insert_performer(clickhouse: ClickhouseService, model: tenders.Performer) -> None:
    """Insert a performer into ClickHouse"""

    logger.info(f"Inserting performer: {model.performer_id}")
    items = asdict(model)
    clickhouse.insert("Performer", [list(items.values())], list(items.keys()))


def insert_streetaddress(clickhouse: ClickhouseService, model: tenders.StreetAddress) -> None:
    """Insert a performer into ClickHouse"""

    logger.info(f"Inserting StreetAddress: {model.id}")
    items = asdict(model)
    clickhouse.insert("StreetAddress", [list(items.values())], list(items.keys()))


def insert_city(clickhouse: ClickhouseService, model: tenders.City) -> None:
    """Insert a performer into ClickHouse"""

    logger.info(f"Inserting City: {model.city_katottg}")
    items = asdict(model)
    clickhouse.insert("City", [list(items.values())], list(items.keys()))


def insert_region(clickhouse: ClickhouseService, model: tenders.Region) -> None:
    """Insert a Region into ClickHouse"""

    logger.info(f"Inserting Region: {model.region_katottg}")
    items = asdict(model)
    clickhouse.insert("Region", [list(items.values())], list(items.keys()))


def insert_manager(clickhouse: ClickhouseService, model: espo.Manager) -> None:
    """Insert a performer into ClickHouse"""

    logger.info(f"Inserting Manager: {model.manager_id}")
    items = asdict(model)
    clickhouse.insert("Manager", [list(items.values())], list(items.keys()))


def insert_campaign(clickhouse: ClickhouseService, model: espo.Campaign) -> None:
    """Insert a performer into ClickHouse"""

    logger.info(f"Inserting campaign: {model.campaign_id}")
    items = asdict(model)
    clickhouse.insert("Campaign", [list(items.values())], list(items.keys()))


def insert_channel(clickhouse: ClickhouseService, model: espo.Channel) -> None:
    """Insert a performer into ClickHouse"""

    logger.info(f"Inserting channel: {model.channel_id}")
    items = asdict(model)
    clickhouse.insert("Channel", [list(items.values())], list(items.keys()))


def insert_stage(clickhouse: ClickhouseService, model: espo.Stage) -> None:
    """Insert a performer into ClickHouse"""

    logger.info(f"Inserting stage: {model.stage_id}")
    items = asdict(model)
    clickhouse.insert("Stage", [list(items.values())], list(items.keys()))


def insert_lead_activity(clickhouse: ClickhouseService, model: espo.LeadActivity) -> None:
    """Insert a performer into ClickHouse"""

    logger.info(f"Inserting lead activity: {model.time_id}")
    items = asdict(model)
    clickhouse.insert("LeadActivity", [list(items.values())], list(items.keys()))


INSERTERS = {
    tenders.TenderOpened.__name__: insert_tender_opened,
    tenders.TenderClosed.__name__: insert_tender_closed,
    tenders.TenderInfo.__name__: insert_tender_info,
    tenders.ProcurementEntity.__name__: insert_procurement_entity,
    tenders.DateDim.__name__: insert_date_dim,
    tenders.Performer.__name__: insert_performer,
    tenders.StreetAddress.__name__: insert_streetaddress,
    tenders.City.__name__: insert_city,
    tenders.Region.__name__: insert_region,
    espo.Manager.__name__: insert_manager,
    espo.Campaign.__name__: insert_campaign,
    espo.Channel.__name__: insert_channel,
    espo.Stage.__name__: insert_stage,
    espo.LeadActivity.__name__: insert_lead_activity,
}
