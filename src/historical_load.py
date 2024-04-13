from datetime import date, timedelta

from espo_crm.espo import EspoCRM
from services import MongoService
from config import CONFIG
from utlis.logger import get_logger
from processors.tenders_processor import TendersProcessor


logger = get_logger("historical_load")


def load_tenders_data(mongo: MongoService) -> None:
    logger.info("Loading tenders data")
    tenders_processor = TendersProcessor()
    tenders_processor.process_historical_data(
        start_date=date.today(), end_date=date.today() - timedelta(days=30)
    )


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
        load_tenders_data(mongo)
        load_espo_data(mongo)
    finally:
        mongo.close()


if __name__ == "__main__":
    run()
