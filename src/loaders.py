from datetime import date, timedelta

from services import MongoService
from config import CONFIG
from utlis.helpers import add_results_to_run_table
from utlis.logger import get_logger
from processors import ProzorroProcessor, EntityProcessor, EspoCRM


logger = get_logger("data_load")


def load_last_month_data(mongo: MongoService, run_id: str) -> None:
    load_data(mongo, run_id=run_id, start_date=date.today() - timedelta(days=30))


def load_last_week_data(mongo: MongoService, run_id: str) -> None:
    load_data(mongo, run_id=run_id)


def load_data(
    mongo: MongoService, run_id: str, start_date: date = date.today() - timedelta(days=5), end_date: date = None
) -> None:
    try:
        prozorro = ProzorroProcessor()
        entities_processor = EntityProcessor()

        for status in ["complete", "active.tendering"]:

            logger.info(f"Getting {status} tenders info and ERDPOU-s")
            tenders = prozorro.get_tender_details_list(start_date=start_date, end_date=end_date, status=status)

            logger.info(f"Received {len(tenders)} tenders with status {status}")
            edrpous = list(filter(None, [prozorro.get_edrpou_from_tender(tender) for tender in tenders]))

            logger.info(f"Getting info about {len(edrpous)} EDRPOU-s")
            entities_details = entities_processor.get_many_entities_details(edrpous)

            logger.info(f"Uploading {len(tenders)} tenders with status {status} to Mongo")
            upsert_results = mongo.upsert_many_tender_details(tenders)
            for result in upsert_results:
                add_results_to_run_table(run_id, mongo, result, CONFIG.MONGO.TENDERS_COLLECTION)
            logger.info(f"{status} tenders were uploaded to Mongo")

            logger.info("Uploading entities to Mongo")
            upsert_results = mongo.upsert_many_entity_details(entities_details)
            for result in upsert_results:
                add_results_to_run_table(run_id, mongo, result, CONFIG.MONGO.ENTITIES_COLLECTION)
            logger.info("Entities were uploaded to Mongo")

    except Exception as e:
        logger.error(f"Failed to upload tenders and EDRPOU-s: {e}")


def load_espo_data(mongo: MongoService, run_id: str) -> None:
    logger.info("Loading EspoCRM data")
    espo = EspoCRM()

    logger.info("Loading accounts...")
    accounts = espo.get_accounts()
    insert_results = mongo.upsert_many_espo_details(CONFIG.MONGO.ACCOUNTS_COLLECTION, accounts)
    for result in insert_results:
        add_results_to_run_table(run_id, mongo, result, CONFIG.MONGO.ACCOUNTS_COLLECTION)

    logger.info("Loading opportunities...")
    opportunities = espo.get_oppotunities()
    insert_results = mongo.upsert_many_espo_details(CONFIG.MONGO.OPPORTUNITIES_COLLECTION, opportunities)
    for result in insert_results:
        add_results_to_run_table(run_id, mongo, result, CONFIG.MONGO.OPPORTUNITIES_COLLECTION)

    logger.info("Loading streams...")
    account_streams = [espo.get_streams("Account", acc["id"]) for acc in accounts]
    opportunity_streams = [espo.get_streams("Opportunity", opp["id"]) for opp in opportunities]

    streams = account_streams + opportunity_streams
    flat_streams = [item for sublist in streams for item in sublist]

    insert_results = mongo.upsert_many_espo_details(CONFIG.MONGO.STREAMS_COLLECTION, flat_streams)
    for result in insert_results:
        add_results_to_run_table(run_id, mongo, result, CONFIG.MONGO.STREAMS_COLLECTION)