from datetime import datetime

from src.config import CONFIG
from src.services import MongoService
from src.utlis.logger import get_logger

logger = get_logger(__name__)


def upsert_tender_details(mongo: MongoService, tender_info):
    tender_in_coll = mongo.find_one(CONFIG.MONGO.TENDERS_COLLECTION,
                                    {"tenderID": tender_info["tenderID"]})
    if (tender_in_coll is not None
            and datetime.fromisoformat(tender_in_coll['dateModified']) <= datetime.fromisoformat(
                tender_info['dateModified'])
    ):
        mongo.update(CONFIG.MONGO.TENDERS_COLLECTION,
                     {"tenderID": tender_info["tenderID"]},
                     tender_info)
        logger.info(f"Tender {tender_info['tenderID']} updated")
    else:
        mongo.insert(CONFIG.MONGO.TENDERS_COLLECTION, tender_info, False)
        logger.info(f"Tender {tender_info['tenderID']} inserted")


def upsert_entity_details(mongo: MongoService, edrpou_info):
    if mongo.find_one(CONFIG.MONGO.ENTITIES_COLLECTION, {"edrpou": edrpou_info["edrpou"]}):
        mongo.update(CONFIG.MONGO.ENTITIES_COLLECTION,
                     {"edrpou": edrpou_info["edrpou"]},
                     edrpou_info)
        logger.info(f"EDRPOU ({edrpou_info['edrpou']}) info already exist, updating")
    else:
        mongo.insert(CONFIG.MONGO.ENTITIES_COLLECTION,
                     edrpou_info,
                     False)
    logger.info(f"EDRPOU ({edrpou_info['edrpou']}) inserted")
