from loaders import load_last_week_data, load_espo_data
from datetime import datetime

from services import MongoService
from config import CONFIG
from utlis.helpers import add_new_run_to_table
from utlis.logger import get_logger
from utlis.tools import generate_run_id

logger = get_logger("incremental_load")


def run() -> None:
    run_id = generate_run_id()

    logger.info(f"Starting incremental load. Run ID: {run_id}")
    start_time = datetime.now()
    mongo = MongoService(CONFIG.MONGO.URI, CONFIG.MONGO.DB_NAME)

    try:
        load_last_week_data(mongo, run_id, limit=20)
        load_espo_data(mongo, run_id)
    finally:
        add_new_run_to_table(mongo, run_id, "incremental_load", start_date=start_time)
        mongo.close()

    logger.info("Incremental load finished.")


if __name__ == "__main__":
    run()
