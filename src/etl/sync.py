from services import MongoService, ClickhouseService
from etl.utils.functions import get_run_logs_per_run, group_results_by_collection_name, group_rows_by_operation_type
from etl.syncers import sync_tenders
from utlis.logger import get_logger
from etl.utils import functions


logger = get_logger("etl-sync")


collection_syncers = {"tenders": sync_tenders}


def start_sync(run_ids_to_process: list[str], mongo: MongoService, clickhouse: ClickhouseService) -> None:

    for run_id in run_ids_to_process:
        logger.info(f"Processing run: {run_id}")

        actions = get_run_logs_per_run(mongo, run_id)

        if not len(list(actions.clone())):
            logger.info(f"No results for run: {run_id}")
            continue

        collection_grouped = group_results_by_collection_name(actions)

        # group by operation type (insert, update, delete)
        for collection_name, rows in collection_grouped.items():
            operation_grouped = group_rows_by_operation_type(rows)
            collection_syncer = collection_syncers.get(collection_name)

            if not collection_syncer:
                logger.error(f"No syncer found for collection: {collection_name}")
                continue

            collection_syncer(run_id, mongo, clickhouse, operation_grouped)

        functions.mark_run_as_synced(mongo, run_id)
