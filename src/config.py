import os
from dotenv import load_dotenv


class CONFIG:
    def __init__(self) -> None:
        load_dotenv(override=True)

    class MONGO:
        URI = os.getenv("MONGO_URI")
        DB_NAME = os.getenv("MONGO_DB_NAME")

        SCHEMA_VERSIONING_COLLECTION = "schema_versioning"
        RUNS_LOGS_COLLECTION = "runs_logs"
        RUNS_COLLECTION = "runs"

        # static collections
        KVEDS_COLLECTION = "kveds"
        KATOTTG_COLLECTION = "katottg"
        DK_COLLECTION = "dk"
        TENDERS_COLLECTION = "tenders"

        # tender collection
        TENDERS_COLLECTION = "tenders"

        # entities collection
        ENTITIES_COLLECTION = "entities"
        DK_TO_KVED_COLLECTION = "dk_to_kved"

        # EspoCRM collections
        ACCOUNTS_COLLECTION = "espo_accounts"
        OPPORTUNITIES_COLLECTION = "espo_opportunities"
        STREAMS_COLLECTION = "espo_streams"

    class CLICKHOUSE:
        HOST = os.getenv("CLICKHOUSE_HOST")
        USER = os.getenv("CLICKHOUSE_USER")
        PASSWORD = os.getenv("CLICKHOUSE_PASS")

    class FILES:
        CWD = os.getcwd()
        FILES_DIR = os.path.join(CWD, "files")

        KVEDS_PATH = os.path.join(FILES_DIR, "kved_parsed.json")
        KATOTTG_FILENAME = os.path.join(FILES_DIR, "katottg_parsed.json")
        DK_FILENAME = os.path.join(FILES_DIR, "dk_parsed.json")

    class ESPO:
        API_URL = os.getenv("ESPO_API_URL").rstrip("/")
        API_KEY = os.getenv("ESPO_API_KEY")

    class PROZORRO:
        API_URL = "https://prozorro.gov.ua/api"
