from typing import Tuple, Any

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


def get_KVED_record(edrpou_kved: str) -> dict:
    mongo = MongoService(CONFIG.MONGO.URI, CONFIG.MONGO.DB_NAME)
    if not edrpou_kved:
        return None
    try:
        result = mongo.find_one(CONFIG.MONGO.KVEDS_COLLECTION, {"class_code": edrpou_kved})
    finally:
        mongo.close()

    return result


def get_kved_code_name(name: str, code: str) -> str:
    if not code:
        return 'n/a'

    mongo = MongoService(CONFIG.MONGO.URI, CONFIG.MONGO.DB_NAME)

    # TODO: Refactor this
    query = {}
    if name == "section_code":
        query['section_code'] = code
        query['partition_code'] = None
        query['group_code'] = None
        query['class_code'] = None
    elif name == "partition_code":
        query['partition_code'] = code
        query['group_code'] = None
        query['class_code'] = None
    elif name == "group_code":
        query['group_code'] = code
        query['class_code'] = None
    else:
        query['class_code'] = code

    try:
        result = mongo.find_one(CONFIG.MONGO.KVEDS_COLLECTION, query)
        print(list(result))
    finally:
        mongo.close()

    return result["name"] if result else 'n/a'


def get_performer_info(edrpou: str):
    mongo = MongoService(CONFIG.MONGO.URI, CONFIG.MONGO.DB_NAME)
    edrpou = mongo.find_one(CONFIG.MONGO.ENTITIES_COLLECTION, {'edrpou': edrpou})
    if edrpou:
        edrpou_name_info = edrpou['fullName'].split(" «")
        try:
            edrpou_type = edrpou_name_info[0].strip()
        except:
            edrpou_type = "n/a"
        try:
            edrpou_name = edrpou_name_info[1].replace("»", "").strip()
        except:
            edrpou_name = "n/a"
        try:
            edrpou_phone = edrpou["phones"][0]["value"]
        except:
            edrpou_phone = "n/a"
        try:
            edrpou_email = edrpou["email"]
        except:
            edrpou_email = "n/a"
        try:
            edrpou_kved = edrpou["primaryActivity"][0]["value"].split(" ")[0].strip()
            first_kved_part = edrpou_kved.split(".")[0]
            second_kved_part = edrpou_kved.split(".")[1]
            if len(first_kved_part) == 2 and first_kved_part[0] == '0':
                edrpou_kved = edrpou_kved[1:]
            if len(second_kved_part) == 2 and second_kved_part[-1] == '0':
                edrpou_kved = edrpou_kved[:-1]
        except:
            edrpou_kved = "n/a"
    else:
        return None, None, None, None, None
    return edrpou_type, edrpou_name, edrpou_phone, edrpou_email, edrpou_kved
