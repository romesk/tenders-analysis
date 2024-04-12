from services import MongoService
from config import CONFIG
from utlis.logger import get_logger
from datetime import date, timedelta

from src.tenders.tenders_processor import TendersProcessor

logger = get_logger(__name__)


def run() -> None:
    mongo = MongoService(CONFIG.MONGO.URI, CONFIG.MONGO.DB_NAME)
    tenders_processor = TendersProcessor()
    logger.info("Connected to MongoDB")

    try:
        tenders_processor.process_historical_data(start_date=date.today(), end_date=date.today() - timedelta(days=30))
    finally:
        mongo.close()


if __name__ == "__main__":
    run()
