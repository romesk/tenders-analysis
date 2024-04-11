import os
from dotenv import load_dotenv

class CONFIG:
    def __init__(self) -> None:
        load_dotenv()

    class MONGO:
        URI = os.getenv('MONGO_URI')
        DB_NAME = os.getenv('MONGO_DB_NAME')
