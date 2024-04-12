import os
from dotenv import load_dotenv


class CONFIG:
    def __init__(self) -> None:
        load_dotenv()

    class MONGO:
        URI = os.getenv("MONGO_URI")
        DB_NAME = os.getenv("MONGO_DB_NAME")

        KVEDS_COLLECTION = "kveds"
        KATOTTG_COLLECTION = "katottg"
        DK_COLLECTION = "dk"
        TENDERS_COLLECTION = "tenders"
        ENTITIES_COLLECTION = "entities"

    class FILES:
        CWD = os.getcwd()
        FILES_DIR = os.path.join(CWD, "files")

        KVEDS_PATH = os.path.join(FILES_DIR, "kved_parsed.json")
        KATOTTG_FILENAME = os.path.join(FILES_DIR, "katottg_parsed.json")
        DK_FILENAME = os.path.join(FILES_DIR, "dk_parsed.json")
