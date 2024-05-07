from services import MongoService, ClickhouseService
from models.operations import LogOperation
from utlis.logger import get_logger
from etl.mappers import espo
from etl.utils import clickhouse_utils
from config import CONFIG


logger = get_logger("espo-syncer")


def sync_espo_accounts(run_id: str, mongo: MongoService, clickhouse: ClickhouseService, operations: dict[str, list]):

    logger.info(f"Syncing espo accounts for run: {run_id}")

    inserted = operations.get(LogOperation.INSERT.value, [])
    updated = operations.get(LogOperation.UPDATE.value, [])
    deleted = operations.get(LogOperation.DELETE.value, [])

    logger.info(f"\n\tInsert: {len(inserted)}\n\tUpdate: {len(updated)}\n\tDelete: {len(deleted)}")

    if inserted or updated:
        inserted_accounts = [
            account for account in mongo.find(CONFIG.MONGO.ACCOUNTS_COLLECTION, {"_id": {"$in": inserted}})
        ]
        sync_inserted(run_id, clickhouse, inserted_accounts, espo.AccountMapperV1)


def sync_espo_campaigns(run_id: str, mongo: MongoService, clickhouse: ClickhouseService, operations: dict[str, list]):

    logger.info(f"Syncing espo campaigns for run: {run_id}")

    inserted = operations.get(LogOperation.INSERT.value, [])
    updated = operations.get(LogOperation.UPDATE.value, [])
    deleted = operations.get(LogOperation.DELETE.value, [])

    logger.info(f"\n\tInsert: {len(inserted)}\n\tUpdate: {len(updated)}\n\tDelete: {len(deleted)}")

    if inserted or updated:
        inserted_campaigns = [
            campaign for campaign in mongo.find(CONFIG.MONGO.CAMPAIGNS_COLLECTION, {"_id": {"$in": inserted}})
        ]
        sync_inserted(run_id, clickhouse, inserted_campaigns, espo.CampaignMapperV1)


def sync_espo_managers(run_id: str, mongo: MongoService, clickhouse: ClickhouseService, operations: dict[str, list]):
    logger.info(f"Syncing espo managers for run: {run_id}")

    inserted = operations.get(LogOperation.INSERT.value, [])
    updated = operations.get(LogOperation.UPDATE.value, [])
    deleted = operations.get(LogOperation.DELETE.value, [])

    logger.info(f"\n\tInsert: {len(inserted)}\n\tUpdate: {len(updated)}\n\tDelete: {len(deleted)}")

    if inserted or updated:
        inserted_managers = [
            manager for manager in mongo.find(CONFIG.MONGO.MANAGERS_COLLECTION, {"_id": {"$in": inserted}})
        ]
        sync_inserted(run_id, clickhouse, inserted_managers, espo.ManagerMapperV1)


def sync_espo_activity(
    run_id: str, mongo: MongoService, clickhouse: ClickhouseService, operations: dict[str, list]
):

    logger.info(f"Syncing espo activity for run: {run_id}")

    inserted = operations.get(LogOperation.INSERT.value, [])
    updated = operations.get(LogOperation.UPDATE.value, [])
    deleted = operations.get(LogOperation.DELETE.value, [])

    logger.info(f"\n\tInsert: {len(inserted)}\n\tUpdate: {len(updated)}\n\tDelete: {len(deleted)}")

    if inserted:
        inserted_activities = [
            activity for activity in mongo.find(CONFIG.MONGO.LEADS_COLLECTION, {"_id": {"$in": inserted}})
        ]
        sync_inserted(run_id, clickhouse, inserted_activities, espo.LeadActivityMapperV1)
    if updated:
        updated_activities = [
            activity for activity in mongo.find(CONFIG.MONGO.LEADS_COLLECTION, {"id": {"$in": updated}})
        ]

        for activity in updated_activities:
            previous_data = clickhouse_utils.get_by_id(clickhouse, "LeadActivity", activity["id"])
            activity_mapper = espo.LeadActivityMapperV1(activity, previous_data, clickhouse)
            mapped_data = activity_mapper.map()

            for model in mapped_data:
                clickhouse_utils.insert_clickhouse_model(clickhouse, model)


def sync_inserted(run_id: str, clickhouse: ClickhouseService, inserted: list[dict], mapper) -> None:

    for entity in inserted:
        entity_mapper = mapper(entity)
        mapped_data = entity_mapper.map()

        for model in mapped_data:
            clickhouse_utils.insert_clickhouse_model(clickhouse, model)
