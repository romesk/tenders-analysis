import datetime
from typing import Iterable
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from datetime import datetime

from config import CONFIG
from utlis.logger import get_logger


logger = get_logger("MongoService")


class MongoService:
    """
    This class is responsible for the interaction with MongoDB.
    """

    def __init__(self, uri: str, db_name: str):
        self.client = MongoClient(uri.strip(), server_api=ServerApi("1"))
        self.db = self.client[db_name]
        self._schema_collection = CONFIG.MONGO.SCHEMA_VERSIONING_COLLECTION

    def insert(self, collection: str, data, use_schema_versioning: bool = True):
        if use_schema_versioning:
            schema_version = self.get_schema_version(collection)
            data = [{**item, "schema_version": schema_version} for item in data]
        self.db[collection].insert_one(data)

    def insert_many(
            self, collection: str, data: Iterable, use_schema_versioning: bool = True
    ):
        if use_schema_versioning:
            schema_version = self.get_schema_version(collection)
            data = [{**item, "schema_version": schema_version} for item in data]

        self.db[collection].insert_many(data)

    def find(self, collection: str, query):
        return self.db[collection].find(query)

    def find_one(self, collection: str, query):
        return self.db[collection].find_one(query)

    def update(self, collection: str, query, data):
        self.db[collection].update_one(query, {"$set": data})

    def delete(self, collection: str, query):
        self.db[collection].delete_one(query)

    def delete_all(self, collection: str):
        self.db[collection].delete_many({})

    def close(self):
        self.client.close()

    def get_collections(self) -> list[str]:
        return self.db.list_collection_names()

    def create_collection_with_unique_index(
            self, collection_name: str, unique_columns: list[str]
    ) -> bool:
        if collection_name in self.get_collections():
            raise Exception(f"Collection {collection_name} already exists")

        # create collection with unique index
        self.db.create_collection(collection_name)
        self.db[collection_name].create_index(
            [(col, 1) for col in unique_columns], unique=True, name="unique_index"
        )
        return True

    def get_schema_version(self, collection: str):
        """
        Get the schema version for a collection.
        If the shema verion is not found, create a new one with version 1.
        """
        # get the schema version for the collection and max active_from date
        schema = (
            self.find(
                self._schema_collection,
                {"collection": collection},
            )
            .sort("active_from", -1)
            .limit(1)
        )
        result = list(schema)
        if not result:
            logger.info(
                f"Schema version not found for collection {collection}. Creating new one."
            )
            self.insert(
                self._schema_collection,
                {
                    "collection": collection,
                    "version": 1,
                    "active_from": datetime.datetime.now(),
                },
                use_schema_versioning=False,
            )
            return 1
        return result[0]["version"]

    def upsert_tender_details(self, tender_info):
        tender_in_coll = self.find_one(CONFIG.MONGO.TENDERS_COLLECTION,
                                       {"tenderID": tender_info["tenderID"]})
        if (tender_in_coll is not None
                and datetime.fromisoformat(tender_in_coll['dateModified']) <= datetime.fromisoformat(
                    tender_info['dateModified'])
        ):
            self.update(CONFIG.MONGO.TENDERS_COLLECTION,
                        {"tenderID": tender_info["tenderID"]},
                        tender_info)
            logger.info(f"Tender {tender_info['tenderID']} updated")
        else:
            self.insert(CONFIG.MONGO.TENDERS_COLLECTION, tender_info, False)
            logger.info(f"Tender {tender_info['tenderID']} inserted")

    def upsert_entity_details(self, edrpou_info):
        if self.find_one(CONFIG.MONGO.ENTITIES_COLLECTION, {"edrpou": edrpou_info["edrpou"]}):
            self.update(CONFIG.MONGO.ENTITIES_COLLECTION,
                        {"edrpou": edrpou_info["edrpou"]},
                        edrpou_info)
            logger.info(f"EDRPOU ({edrpou_info['edrpou']}) info already exist, updating")
        else:
            self.insert(CONFIG.MONGO.ENTITIES_COLLECTION,
                        edrpou_info,
                        False)
            logger.info(f"EDRPOU ({edrpou_info['edrpou']}) inserted")

    def upsert_many_tender_details(self, tenders_details):
        for tender in tenders_details:
            if tender is not None:
                self.upsert_tender_details(tender)

    def upsert_many_entity_details(self, entities_details):
        for entity in entities_details:
            if entity is not None:
                self.upsert_entity_details(entity)
