import json
from config import CONFIG
from services.mongo import MongoService
from utlis.logger import get_logger


logger = get_logger("static_files_load")


def load_kveds(mongo: MongoService) -> None:

    logger.info("Loading KVEDs")

    with open(CONFIG.FILES.KVEDS_PATH, "r") as f:
        kveds = f.read()

    kveds = json.loads(kveds)
    mongo.create_collection_with_unique_index(
        CONFIG.MONGO.KVEDS_COLLECTION,
        ["name", "group_code", "class_code", "partition_code"],
    )
    logger.info(f"Inserting {len(kveds)} KVEDs into {CONFIG.MONGO.KVEDS_COLLECTION}")
    mongo.insert_many(CONFIG.MONGO.KVEDS_COLLECTION, kveds)

    logger.info("KVEDs loaded")


def load_katottg(mongo: MongoService) -> None:

    logger.info("Loading KATOTTG")

    with open(CONFIG.FILES.KATOTTG_FILENAME, "r") as f:
        katottg = f.read()

    katottg = json.loads(katottg)
    mongo.create_collection_with_unique_index(
        CONFIG.MONGO.KATOTTG_COLLECTION,
        ["level1", "level2", "level3", "level4", "name"],
    )
    logger.info(
        f"Inserting {len(katottg)} KATOTTG into {CONFIG.MONGO.KATOTTG_COLLECTION}"
    )
    mongo.insert_many(CONFIG.MONGO.KATOTTG_COLLECTION, katottg)

    logger.info("KATOTTG loaded")


def load_dk(mongo: MongoService) -> None:

    logger.info("Loading DK")

    with open(CONFIG.FILES.DK_FILENAME, "r") as f:
        dk = f.read()

    dk = json.loads(dk)
    mongo.create_collection_with_unique_index(
        CONFIG.MONGO.DK_COLLECTION,
        ["Division", "Group", "Class", "Category", "Clarification"],
    )
    logger.info(f"Inserting {len(dk)} DK into {CONFIG.MONGO.DK_COLLECTION}")
    mongo.insert_many(CONFIG.MONGO.DK_COLLECTION, dk)

    logger.info("DK loaded")


def run():
    """
    Loads static files into MongoDB. For one-time use only.
    Files cannot be loaded if the collections already exist.
    """
    mongo = MongoService(CONFIG.MONGO.URI, CONFIG.MONGO.DB_NAME)

    try:
        load_kveds(mongo)
        load_katottg(mongo)
        load_dk(mongo)
    except Exception as e:
        logger.error(f"Failed loading static files: {e}")
        raise e
    finally:
        mongo.close()
        print("Done")


if __name__ == "__main__":
    run()
