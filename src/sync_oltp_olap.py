import datetime
from config import CONFIG
from services import MongoService, ClickhouseService
from utlis.helpers import add_new_run_to_table, get_unprocessed_runs
from utlis.logger import get_logger
from utlis.tools import generate_run_id
from etl.sync import start_sync


logger = get_logger("sync-oltp-olap")


def run():
    run_id = generate_run_id()
    start_time = datetime.datetime.now()

    logger.info(f"Starting sync OLTP to OLAP. Run ID: {run_id}")

    mongo = MongoService(CONFIG.MONGO.URI, CONFIG.MONGO.DB_NAME)

    clickhouse = ClickhouseService(CONFIG.CLICKHOUSE.HOST, CONFIG.CLICKHOUSE.USER, CONFIG.CLICKHOUSE.PASSWORD)

    try:
        run_ids_to_process = get_unprocessed_runs(mongo)
        start_sync(run_ids_to_process, mongo, clickhouse)
    finally:
        # add this run to the run table
        # add_new_run_to_table(mongo, run_id, "etl", start_time)
        mongo.close()

    end_time = datetime.datetime.now()
    logger.info(f"Sync finished in: {end_time - start_time}")


if __name__ == "__main__":
    run()
