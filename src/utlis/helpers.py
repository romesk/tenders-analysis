import datetime
from pymongo.results import InsertManyResult, InsertOneResult, UpdateResult
import pymongo

from config import CONFIG
from models.operations import LogOperation
from services.mongo import MongoService
from utlis.logger import get_logger


logger = get_logger("helpers")


def add_results_to_run_table(
    run_id: str, mongo: MongoService, results: InsertOneResult | InsertManyResult, collection_name: str
) -> InsertManyResult:
    """Add the results of the operation to the run table

    :param run_id: id of the run
    :param mongo: MongoService instance
    :param results: results of the mongo operation
    :param collection_name: name of the mongo collection
    :raises NotImplementedError: if the result type is not supported
    """

    if isinstance(results, (InsertManyResult, InsertOneResult)):
        operation = LogOperation.INSERT
    elif isinstance(results, UpdateResult):
        operation = LogOperation.UPDATE
    else:
        raise NotImplementedError(f"Unsupported result type: {type(results)}")

    if operation == LogOperation.INSERT:
        inserted_ids = results.inserted_ids if isinstance(results, InsertManyResult) else [results.inserted_id]
        operation = "insert"
    elif operation == LogOperation.UPDATE:
        inserted_ids = [results.upserted_id]
        operation = "update"
    else:
        raise NotImplementedError(f"Unsupported operation: {operation}")

    rows = []

    for _id in inserted_ids:
        insert_row = {
            "run_id": run_id,
            "collection": collection_name,
            "operation": operation,
            "object_id": _id,
        }
        rows.append(insert_row)

    logger.info(
        f"Adding {len(rows)} rows to the run table. Run ID: {run_id}, Collection: {collection_name}, Operation: {operation}"
    )

    return mongo.insert_many(CONFIG.MONGO.RUNS_LOGS_COLLECTION, rows, use_schema_versioning=False)


def add_new_run_to_table(
    mongo: MongoService, run_id: str, run_type: str, start_date: datetime.datetime
) -> InsertOneResult:
    """Add a new run to the run table

    :param mongo: MongoService instance
    :param run_id: id of the run
    :param start_date: start date of the run
    :return: result of the operation
    """
    now = datetime.datetime.now()
    run_row = {
        "run_id": run_id,
        "type": run_type,
        "synced_with_olap": False,
        "start": start_date,
        "end": now,
    }

    logger.info(f"Adding a new run to the run table. Run ID: {run_id}, Start: {start_date}, End: {now}")

    return mongo.insert(CONFIG.MONGO.RUNS_COLLECTION, run_row, use_schema_versioning=False)


def get_unprocessed_runs(mongo: MongoService) -> list[str]:
    """Get the list of unprocessed runs

    :param mongo: MongoService instance
    :param run_type: type of the run
    :return: list of unprocessed runs
    """
    # where type is not "etl" and synced_with_olap is False
    query = {"type": {"$ne": "etl"}, "synced_with_olap": False}
    runs = mongo.find(CONFIG.MONGO.RUNS_COLLECTION, query).sort("start", pymongo.ASCENDING)
    return [run["run_id"] for run in runs]
