from services import MongoService, ClickhouseService
from models.operations import LogOperation
from utlis.logger import get_logger
from etl.mappers import espo
from etl.utils import clickhouse_utils
from config import CONFIG


mappers = {
    "LEAD": espo.LeadActivityMapperV1,
    "SALE": espo.SaleActivityMapperV1,
}
logger = get_logger("espo-syncer")


def sync_espo_activity(
    run_id: str, mongo: MongoService, clickhouse: ClickhouseService, operations: dict[LogOperation, list]
):

    logger.info(f"Syncing espo activity for run: {run_id}")

    inserted = operations.get(LogOperation.INSERT.value, [])
    updated = operations.get(LogOperation.UPDATE.value, [])
    deleted = operations.get(LogOperation.DELETE.value, [])

    logger.info(f"\n\tInsert: {len(inserted)}\n\tUpdate: {len(updated)}\n\tDelete: {len(deleted)}")

    if inserted:
        inserted_activities = [
            activity for activity in mongo.find(CONFIG.MONGO.OPPORTUNITIES_COLLECTION, {"_id": {"$in": inserted}})
        ]
        sync_inserted(run_id, clickhouse, inserted_activities)


def sync_inserted(run_id: str, clickhouse: ClickhouseService, inserted: list[dict]) -> None:

    for activity in inserted:
        activity_type = "SALE" if activity.get("stage") == "Closed Won" else "LEAD"
        mapper = mappers[activity_type](activity)
        mapped_data = mapper.map()

        for model in mapped_data:
            clickhouse_utils.insert_clickhouse_model(clickhouse, model)
