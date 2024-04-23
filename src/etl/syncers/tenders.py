from config import CONFIG
from models.operations import LogOperation
from etl.mappers.tenders import TenderOpenedMapperV1, TenderClosedMapperV1
from etl.utils import clickhouse_utils
from services import MongoService, ClickhouseService
from utlis.logger import get_logger


logger = get_logger("tedners-syncer")

mappers = {"OPENED": TenderOpenedMapperV1, "CLOSED": TenderClosedMapperV1}


def sync_tenders(
    run_id: str, mongo: MongoService, clickhouse: ClickhouseService, operations: dict[LogOperation, list]
) -> None:

    logger.info(f"Syncing tenders for run: {run_id}")

    inserted = operations.get(LogOperation.INSERT.value, [])
    updated = operations.get(LogOperation.UPDATE.value, [])
    deleted = operations.get(LogOperation.DELETE.value, [])

    logger.info(f"\n\tInsert: {len(inserted)}\n\tUpdate: {len(updated)}\n\tDelete: {len(deleted)}")

    if inserted:
        inserted_tenders = [
            tender for tender in mongo.find(CONFIG.MONGO.TENDERS_COLLECTION, {"_id": {"$in": inserted}})
        ]
        sync_inserted(run_id, clickhouse, inserted_tenders)

    if updated:
        updated_tenders = [tender for tender in mongo.find(CONFIG.MONGO.TENDERS_COLLECTION, {"_id": {"$in": updated}})]
        sync_updated(run_id, clickhouse, updated_tenders)

    if deleted:
        raise NotImplementedError("Deletion of tenders is not supported yet.")


def sync_inserted(run_id: str, clickhouse: ClickhouseService, inserted: list[dict]) -> None:

    for tender in inserted:
        status = "CLOSED" if tender.get("status") == "complete" else "OPENED"

        mapper = mappers[status](tender)
        mapped_data = mapper.map()

        for model in mapped_data:
            clickhouse_utils.insert_clickhouse_model(clickhouse, model)


def sync_updated(run_id: str, clickhouse: ClickhouseService, updated: list[dict]) -> None:

    for tender in updated:
        status = "CLOSED" if tender.get("status") == "complete" else "OPENED"

        mapper = mappers[status](tender)
        mapped_data = mapper.map()

        for model in mapped_data:
            clickhouse_utils.insert_clickhouse_model(clickhouse, model)
