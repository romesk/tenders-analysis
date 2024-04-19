import datetime
from services import MongoService
from utlis.helpers import add_new_run_to_table, get_unprocessed_runs
from utlis.tools import generate_run_id


def run():
    run_id = generate_run_id()
    start_time = datetime.datetime.now()
    mongo = MongoService()

    try:
        run_ids_to_process = get_unprocessed_runs()

    finally:
        # add this run to the run table
        add_new_run_to_table(mongo, run_id, "etl", start_time)
        mongo.close_connection()


if __name__ == "__main__":
    run()
