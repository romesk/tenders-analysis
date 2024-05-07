from loaders import load_data, load_espo_data
from datetime import datetime, date, timedelta

from services import MongoService
from config import CONFIG
from utlis.helpers import add_new_run_to_table
from utlis.logger import get_logger
from utlis.tools import generate_run_id

logger = get_logger("historical_load")


def run() -> None:
    run_id = generate_run_id()

    logger.info(f"Starting historical load. Run ID: {run_id}")
    start_time = datetime.now()
    mongo = MongoService(CONFIG.MONGO.URI, CONFIG.MONGO.DB_NAME)

    try:
        load_data(mongo, run_id, 15, limit=10)
        load_espo_data(mongo, run_id)
    finally:
        add_new_run_to_table(mongo, run_id, "historical_load", start_date=start_time)
        mongo.close()

    logger.info("Historical load finished.")


if __name__ == "__main__":
    run()
    # mongo = MongoService(CONFIG.MONGO.URI, CONFIG.MONGO.DB_NAME)
    # mongo.delete_all(CONFIG.MONGO.TENDERS_COLLECTION)
    # mongo.delete_all(CONFIG.MONGO.ENTITIES_COLLECTION)
    # mongo.delete_all(CONFIG.MONGO.ACCOUNTS_COLLECTION)
    # mongo.delete_all(CONFIG.MONGO.STREAMS_COLLECTION)
    # mongo.delete_all(CONFIG.MONGO.OPPORTUNITIES_COLLECTION)
    # mongo.delete_all(CONFIG.MONGO.RUNS_COLLECTION)
    # mongo.delete_all(CONFIG.MONGO.RUNS_LOGS_COLLECTION)
