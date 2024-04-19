from datetime import datetime
import json
import argparse

from config import CONFIG
from services.mongo import MongoService
from utlis.logger import get_logger
from utlis.tools import generate_run_id
from utlis.helpers import add_results_to_run_table, add_new_run_to_table


logger = get_logger("static_files_load")


def load_kveds(run_id: str, mongo: MongoService, force: bool = False) -> None:

    logger.info("Loading KVEDs")

    with open(CONFIG.FILES.KVEDS_PATH, "r") as f:
        kveds = f.read()

    kveds = json.loads(kveds)
    mongo.create_collection_with_unique_index(
        CONFIG.MONGO.KVEDS_COLLECTION, ["name", "group_code", "class_code", "partition_code"], force
    )
    logger.info(f"Inserting {len(kveds)} KVEDs into {CONFIG.MONGO.KVEDS_COLLECTION}")
    insert_result = mongo.insert_many(CONFIG.MONGO.KVEDS_COLLECTION, kveds)
    add_results_to_run_table(run_id, mongo, insert_result, CONFIG.MONGO.KVEDS_COLLECTION)

    logger.info("KVEDs loaded")


def load_katottg(run_id: str, mongo: MongoService, force: bool = False) -> None:

    logger.info("Loading KATOTTG")

    with open(CONFIG.FILES.KATOTTG_FILENAME, "r") as f:
        katottg = f.read()

    katottg = json.loads(katottg)
    mongo.create_collection_with_unique_index(
        CONFIG.MONGO.KATOTTG_COLLECTION, ["level1", "level2", "level3", "level4", "name"], force
    )
    logger.info(f"Inserting {len(katottg)} KATOTTG into {CONFIG.MONGO.KATOTTG_COLLECTION}")
    insert_result = mongo.insert_many(CONFIG.MONGO.KATOTTG_COLLECTION, katottg)
    add_results_to_run_table(run_id, mongo, insert_result, CONFIG.MONGO.KATOTTG_COLLECTION)

    logger.info("KATOTTG loaded")


def load_dk(run_id: str, mongo: MongoService, force: bool = False) -> None:

    logger.info("Loading DK")

    with open(CONFIG.FILES.DK_FILENAME, "r") as f:
        dk = f.read()

    dk = json.loads(dk)
    mongo.create_collection_with_unique_index(
        CONFIG.MONGO.DK_COLLECTION, ["Division", "Group", "Class", "Category", "Clarification"], force
    )
    logger.info(f"Inserting {len(dk)} DK into {CONFIG.MONGO.DK_COLLECTION}")
    insert_result = mongo.insert_many(CONFIG.MONGO.DK_COLLECTION, dk)
    add_results_to_run_table(run_id, mongo, insert_result, CONFIG.MONGO.DK_COLLECTION)

    logger.info("DK loaded")


def run(force: bool = False):
    """
    Loads static files into MongoDB. For one-time use only.
    Files cannot be loaded if the collections already exist.
    """
    # TODO: load from cloud, not from local files

    run_id = generate_run_id()
    start_time = datetime.now()

    logger.info(f"Starting static files load. Run ID: {run_id}")
    mongo = MongoService(CONFIG.MONGO.URI, CONFIG.MONGO.DB_NAME)

    try:
        load_kveds(mongo, force)
        load_katottg(mongo, force)
        load_dk(mongo, force)

    except Exception as e:
        logger.error(f"Failed loading static files: {e}")
        raise e
    finally:
        add_new_run_to_table(mongo, run_id, "static_files_load", start_date=start_time)
        mongo.close()

    logger.info("Static files load finished.")


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force load static files even if collections already exist",
    )
    args = parser.parse_args()

    run(args.force)
