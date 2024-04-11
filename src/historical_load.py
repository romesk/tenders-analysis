from services import MongoService
from config import CONFIG
from utlis.logger import get_logger


logger = get_logger(__name__)


def run() -> None:

    mongo = MongoService(CONFIG.MONGO.URI, CONFIG.MONGO.DB_NAME)
    logger.info("Connected to MongoDB")

    try:
        pass
    finally:
        mongo.close()


if __name__ == "__main__":
    run()
