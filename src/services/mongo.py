from typing import Iterable
from pymongo import MongoClient
from pymongo.server_api import ServerApi


class MongoService:
    """
    This class is responsible for the interaction with MongoDB.
    """

    def __init__(self, uri: str, db_name: str):
        self.client = MongoClient(uri.strip(), server_api=ServerApi("1"))
        self.db = self.client[db_name]

    def insert(self, collection: str, data):
        self.db[collection].insert_one(data)

    def insert_many(self, collection: str, data: Iterable):
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
