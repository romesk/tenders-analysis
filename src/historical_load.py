from datetime import date, timedelta

from espo_crm.espo import EspoCRM
from services import MongoService
from config import CONFIG
from src.upserters import upsert_tender_details, upsert_entity_details
from utlis.logger import get_logger
from processors.tenders_processor import TendersProcessor
from processors.entities_processor import EntityProcessor

logger = get_logger("historical_load")


def load_last_month_data(mongo: MongoService) -> None:
    load_history_data(mongo, start_date=date.today(), end_date=date.today() - timedelta(days=30))


def load_last_week_data(mongo: MongoService) -> None:
    load_history_data(mongo)


def load_history_data(mongo: MongoService, start_date: date = date.today() - timedelta(days=7),
                      end_date: date = date.today()) -> None:
    try:
        tenders_processor = TendersProcessor()
        entities_processor = EntityProcessor()
        logger.info(f"Getting tenders for period from {start_date} to {end_date}")
        for status in ['complete', 'active.tendering']:

            logger.info(f"Getting {status} tenders info and ERDPOU-s")
            tenders_details, edrpous = tenders_processor.get_historical_data(start_date=start_date,
                                                                             end_date=end_date,
                                                                             status=status)

            logger.info(f"Getting info about {len(edrpous)} EDRPOU-s")
            entities_details = entities_processor.get_many_entities_details(edrpous)

            logger.info(f"Uploading {len(tenders_details)} tenders with status {status} to Mongo")
            [upsert_tender_details(mongo, tender) for tender in tenders_details]
            logger.info(f"{status} tenders were uploaded to Mongo")

            logger.info(f"Uploading entities to Mongo")
            [upsert_entity_details(mongo, entity) for entity in entities_details if entity is not None]
            logger.info(f"Entities were uploaded to Mongo")

    except Exception as e:
        logger.error(f"Failed to upload tenders and EDRPOU-s: {e}")


def load_espo_data(mongo: MongoService) -> None:
    logger.info("Loading EspoCRM data")
    espo = EspoCRM()

    logger.info("Loading accounts...")
    accounts = espo.get_accounts()
    mongo.insert_many(CONFIG.MONGO.ACCOUNTS_COLLECTION, accounts)

    logger.info("Loading opportunities...")
    opportunities = espo.get_oppotunities()
    mongo.insert_many(CONFIG.MONGO.OPPORTUNITIES_COLLECTION, opportunities)

    logger.info("Loading streams...")
    account_streams = [espo.get_streams("Account", acc["id"]) for acc in accounts]
    opportunity_streams = [
        espo.get_streams("Opportunity", opp["id"]) for opp in opportunities
    ]

    streams = account_streams + opportunity_streams
    flat_streams = [item for sublist in streams for item in sublist]

    mongo.insert_many(CONFIG.MONGO.STREAMS_COLLECTION, flat_streams)


def run() -> None:
    mongo = MongoService(CONFIG.MONGO.URI, CONFIG.MONGO.DB_NAME)

    try:
        load_last_week_data(mongo)
        load_espo_data(mongo)
    finally:
        mongo.close()


if __name__ == "__main__":
    run()
