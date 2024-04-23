from dataclasses import asdict
from services.clickhouse import ClickhouseService
from etl.models import tenders
from utlis.logger import get_logger

# TODO: replace table names with constants
# TODO: make batch insertions
logger = get_logger("clickhouse-utils")


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


INSERTERS = {
    tenders.TenderOpened.__name__: insert_tender_opened,
    tenders.TenderClosed.__name__: insert_tender_closed,
    tenders.TenderInfo.__name__: insert_tender_info,
    tenders.ProcurementEntity.__name__: insert_procurement_entity,
    tenders.DateDim.__name__: insert_date_dim,
    tenders.Performer.__name__: insert_performer,
    # tenders.Location.__name__: insert_location,
}
