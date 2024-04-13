import json
import argparse
from config import CONFIG
from services.mongo import MongoService
from utlis.logger import get_logger


logger = get_logger("static_files_load")


def load_kveds(mongo: MongoService, force: bool = False) -> None:

    logger.info("Loading KVEDs")

    with open(CONFIG.FILES.KVEDS_PATH, "r") as f:
        kveds = f.read()

    kveds = json.loads(kveds)
    mongo.create_collection_with_unique_index(
        CONFIG.MONGO.KVEDS_COLLECTION,
        ["name", "group_code", "class_code", "partition_code"],
        force
    )
    logger.info(f"Inserting {len(kveds)} KVEDs into {CONFIG.MONGO.KVEDS_COLLECTION}")
    mongo.insert_many(CONFIG.MONGO.KVEDS_COLLECTION, kveds)

    logger.info("KVEDs loaded")


def load_katottg(mongo: MongoService, force: bool = False) -> None:

    logger.info("Loading KATOTTG")

    with open(CONFIG.FILES.KATOTTG_FILENAME, "r") as f:
        katottg = f.read()

    katottg = json.loads(katottg)
    mongo.create_collection_with_unique_index(
        CONFIG.MONGO.KATOTTG_COLLECTION,
        ["level1", "level2", "level3", "level4", "name"],
        force
    )
    logger.info(
        f"Inserting {len(katottg)} KATOTTG into {CONFIG.MONGO.KATOTTG_COLLECTION}"
    )
    mongo.insert_many(CONFIG.MONGO.KATOTTG_COLLECTION, katottg)

    logger.info("KATOTTG loaded")


def load_dk(mongo: MongoService, force: bool = False) -> None:

    logger.info("Loading DK")

    with open(CONFIG.FILES.DK_FILENAME, "r") as f:
        dk = f.read()

    dk = json.loads(dk)
    mongo.create_collection_with_unique_index(
        CONFIG.MONGO.DK_COLLECTION,
        ["Division", "Group", "Class", "Category", "Clarification"],
        force
    )
    logger.info(f"Inserting {len(dk)} DK into {CONFIG.MONGO.DK_COLLECTION}")
    mongo.insert_many(CONFIG.MONGO.DK_COLLECTION, dk)

    logger.info("DK loaded")


def run(force: bool = False):
    """
    Loads static files into MongoDB. For one-time use only.
    Files cannot be loaded if the collections already exist.
    """
    # TODO: load from cloud, not from local files
    mongo = MongoService(CONFIG.MONGO.URI, CONFIG.MONGO.DB_NAME)

    try:
        load_kveds(mongo, force)
        load_katottg(mongo, force)
        load_dk(mongo, force)
    except Exception as e:
        logger.error(f"Failed loading static files: {e}")
        raise e
    finally:
        mongo.close()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force load static files even if collections already exist",
    )
    args = parser.parse_args()

    run(args.force)
