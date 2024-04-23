from bson.objectid import ObjectId
from pymongo.cursor import Cursor
from config import CONFIG
from services import MongoService


def get_run_logs_per_run(mongo: MongoService, run_id: str) -> Cursor:
    """Get all the logs for a specific run"""
    return mongo.find(CONFIG.MONGO.RUNS_LOGS_COLLECTION, {"run_id": run_id})


def mark_run_as_synced(mongo: MongoService, run_id: str) -> None:
    """Mark a run as synced with OLAP"""
    mongo.update(
        CONFIG.MONGO.RUNS_COLLECTION,
        {"run_id": run_id},
        {"synced_with_olap": True},
    )


def group_results_by_collection_name(cursor: Cursor) -> dict[str, list[dict]]:

    results = {}
    for row in cursor:
        collection = row["collection"]
        results[collection] = results.get(collection, []) + [row]

    return results


def group_rows_by_operation_type(rows: list[dict]) -> dict[str, list[ObjectId]]:

    results = {}
    for row in rows:
        operation = row["operation"]
        results[operation] = results.get(operation, []) + [row["object_id"]]

    return results


def get_DK_record(code: str) -> dict:

    mongo = MongoService(CONFIG.MONGO.URI, CONFIG.MONGO.DB_NAME)
    try:
        result = mongo.find_one(CONFIG.MONGO.DK_COLLECTION, {"Class": {"$regex": code}})
    finally:
        mongo.close()

    return result


def get_KVED_record(edrpou: str) -> dict:

    mongo = MongoService(CONFIG.MONGO.URI, CONFIG.MONGO.DB_NAME)
    result = {}
    try:
        entity = mongo.find_one(CONFIG.MONGO.ENTITIES_COLLECTION, {"edrpou": edrpou})
        kved = entity["primaryActivity"][0]["value"].split(" ")[0] if entity else None

        if kved:
            result = mongo.find_one(CONFIG.MONGO.KVEDS_COLLECTION, {"class_code": kved})
    finally:
        mongo.close()

    return result


def get_kved_code_name(name: str, code: str) -> str:

    if not code:
        return 'n/a'

    mongo = MongoService(CONFIG.MONGO.URI, CONFIG.MONGO.DB_NAME)
    try:
        result = mongo.find_one(CONFIG.MONGO.KVEDS_COLLECTION, {name: code})
    finally:
        mongo.close()

    return result["name"] if result else 'n/a'