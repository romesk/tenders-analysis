from datetime import datetime
from typing import Iterable
from pymongo.results import InsertManyResult, InsertOneResult, UpdateResult
from pymongo import MongoClient
from pymongo.server_api import ServerApi

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

    def insert(self, collection: str, data, use_schema_versioning: bool = True) -> InsertOneResult:
        if use_schema_versioning:
            schema_version = self.get_schema_version(collection)
            data = [{**item, "schema_version": schema_version} for item in data]
        return self.db[collection].insert_one(data)

    def insert_many(self, collection: str, data: Iterable, use_schema_versioning: bool = True) -> InsertManyResult:
        if use_schema_versioning:
            schema_version = self.get_schema_version(collection)
            data = [{**item, "schema_version": schema_version} for item in data]

        return self.db[collection].insert_many(data)

    def find(self, collection: str, query):
        return self.db[collection].find(query)

    def find_one(self, collection: str, query):
        return self.db[collection].find_one(query)

    def update(self, collection: str, query, data) -> UpdateResult:
        return self.db[collection].update_one(query, {"$set": data})

    def delete(self, collection: str, query):
        self.db[collection].delete_one(query)

    def delete_all(self, collection: str):
        self.db[collection].delete_many({})

    def close(self):
        self.client.close()

    def get_collections(self) -> list[str]:
        return self.db.list_collection_names()

    def create_collection_with_unique_index(
        self, collection_name: str, unique_columns: list[str], force: bool = False
    ) -> bool:
        if collection_name in self.get_collections() and not force:
            raise Exception(f"Collection {collection_name} already exists")

        if force:
            logger.info(f"Collection {collection_name} already exists. Dropping...")
            self.db.drop_collection(collection_name)

        # create collection with unique index
        self.db.create_collection(collection_name, check_exists=False)
        self.db[collection_name].create_index([(col, 1) for col in unique_columns], unique=True, name="unique_index")
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
            logger.info(f"Schema version not found for collection {collection}. Creating new one.")
            self.insert(
                self._schema_collection,
                {
                    "collection": collection,
                    "version": 1,
                    "active_from": datetime.now(),
                },
                use_schema_versioning=False,
            )
            return 1
        return result[0]["version"]

    def upsert_tender_details(self, received_tender: dict) -> InsertOneResult | UpdateResult:
        tender_in_coll = self.find_one(CONFIG.MONGO.TENDERS_COLLECTION, {"tenderID": received_tender["tenderID"]})
        res = None
        if tender_in_coll is None:
            res = self.insert(CONFIG.MONGO.TENDERS_COLLECTION, received_tender, False)
            logger.info(f"Tender {received_tender['tenderID']} inserted")
        elif datetime.fromisoformat(tender_in_coll["dateModified"]) < datetime.fromisoformat(
            received_tender["dateModified"]
        ):
            res = self.update(
                CONFIG.MONGO.TENDERS_COLLECTION, {"tenderID": received_tender["tenderID"]}, received_tender
            )
            logger.info(f"Tender {received_tender['tenderID']} info updated")
        # else:
        #     logger.info(f"No need for update: TenderID {received_tender['tenderID']}")
        return res

    def upsert_entity_details(self, received_entity) -> InsertOneResult | UpdateResult:
        entity_in_coll = self.find_one(CONFIG.MONGO.ENTITIES_COLLECTION, {"edrpou": received_entity["edrpou"]})
        res = None
        if entity_in_coll is None:
            res = self.insert(CONFIG.MONGO.ENTITIES_COLLECTION, received_entity, False)
            logger.info(f"Entity with EDRPOU {received_entity['edrpou']} inserted")
        elif datetime.fromisoformat(entity_in_coll["lastTime"]["dateTime"]) < datetime.fromisoformat(
            received_entity["lastTime"]["dateTime"]
        ):
            res = self.update(CONFIG.MONGO.ENTITIES_COLLECTION, {"edrpou": received_entity["edrpou"]}, received_entity)
            logger.info(f"Entity with EDRPOU ({received_entity['edrpou']}) updated")
        else:
            logger.info(f"No need for update: EDRPOU ({received_entity['edrpou']})")
        return res

    def upsert_espo_details(self, collection_name, received_espo) -> InsertOneResult | UpdateResult:
        espo_in_coll = self.find_one(collection_name, {"id": received_espo["id"]})
        res = None
        if espo_in_coll is None:
            res = self.insert(collection_name, received_espo, False)
            logger.info(f"ESPO {collection_name}: {received_espo['id']} inserted")
        elif datetime.fromisoformat(espo_in_coll["modifiedAt"]) < datetime.fromisoformat(received_espo["modifiedAt"]):
            res = self.update(collection_name, {"id": received_espo["id"]}, received_espo)
            logger.info(f"ESPO {collection_name}: ({received_espo['id']}) updated")
        else:
            logger.info(f"No need for update: ESPO {collection_name} ({received_espo['id']})")
        return res

    def upsert_many_tender_details(self, tenders_details):
        res = []
        for tender in tenders_details:
            if tender is not None:
                try:
                    res.append(self.upsert_tender_details(tender))
                except Exception as e:
                    logger.error(f"Failed to upload tender info: {e}")
        return list(filter(None, res))

    def upsert_many_entity_details(self, entities_details):
        res = []
        for entity in entities_details:
            if entity is not None:
                try:
                    res.append(self.upsert_entity_details(entity))
                except Exception as e:
                    logger.error(f"Failed to upload entity info: {e}")
        return list(filter(None, res))

    def upsert_many_espo_details(self, collection_name, espo_details):
        res = []
        for espo in espo_details:
            if espo is not None:
                try:
                    res.append(self.upsert_espo_details(collection_name, espo))
                except Exception as e:
                    logger.error(f"Failed to upload espo info: {e}")
        return list(filter(None, res))
