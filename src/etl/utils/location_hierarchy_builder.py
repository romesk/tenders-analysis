import json
from typing import Tuple, Any, Union
from uuid import UUID

import requests

from config import CONFIG
from services import MongoService


kyiv_kattotg = "UA80000000000093317"


def build_entity_kattotg_hierarchy(edrpou: str):
    mongo = MongoService(CONFIG.MONGO.URI, CONFIG.MONGO.DB_NAME)
    entity = mongo.find_one(CONFIG.MONGO.ENTITIES_COLLECTION, {"edrpou": edrpou})
    if not entity:
        return None, None, None
    last_level_katottg = entity["address"][0]["creator"]["link"]
    city_region = mongo.find_one(CONFIG.MONGO.KATOTTG_COLLECTION, {"level5": last_level_katottg})
    if city_region and city_region["level1"] == kyiv_kattotg:
        return entity["address"][0]["creator"]["boldText"], ("Київ", kyiv_kattotg), ("Київ", kyiv_kattotg)
    elif city_region:
        city = mongo.find_one(
            CONFIG.MONGO.KATOTTG_COLLECTION,
            {
                "level1": city_region["level1"],
                "level2": city_region["level2"],
                "level3": city_region["level3"],
                "level4": city_region["level4"],
                "level5": None,
            },
        )
        region = mongo.find_one(
            CONFIG.MONGO.KATOTTG_COLLECTION,
            {"level1": city_region["level1"], "level2": None, "level3": None, "level4": None, "level5": None},
        )
    else:
        city = mongo.find_one(CONFIG.MONGO.KATOTTG_COLLECTION, {"level4": last_level_katottg, "level5": None})
        region = mongo.find_one(
            CONFIG.MONGO.KATOTTG_COLLECTION,
            {"level1": city["level1"], "level2": None, "level3": None, "level4": None, "level5": None},
        )

    return (
        entity["address"][0]["creator"]["boldText"],
        (region["name"], region["level1"]),
        (city["name"], city["level4"]),
    )


def build_tender_kattotg_hierarchy(
    tender,
):
    mongo = MongoService(CONFIG.MONGO.URI, CONFIG.MONGO.DB_NAME)
    tender_city_name = tender["items"][0]["deliveryAddress"]["locality"].replace(".", " ").split()[-1]
    tender_region_name = tender["items"][0]["deliveryAddress"]["region"].replace(".", " ").split()[0]
    tender_address = tender["items"][0]["deliveryAddress"]["streetAddress"]
    if tender_city_name == "Київ" and tender_region_name == "Київська":
        return ("Київ", kyiv_kattotg), ("Київ", kyiv_kattotg)

    region = mongo.find_one(
        CONFIG.MONGO.KATOTTG_COLLECTION,
        {"name": tender_region_name, "level2": None, "level3": None, "level4": None, "level5": None},
    )
    city = mongo.find_one(
        CONFIG.MONGO.KATOTTG_COLLECTION,
        {"level1": region["level1"], "level4": {"$exists": True}, "level5": None, "name": tender_city_name},
    )

    return (
        tender_address,
        (region["name"], region["level1"]),
        (city["name"], city["level4"])
    )


def get_coordinates(address: str):
    query = f"https://maps.googleapis.com/maps/api/geocode/json?address={address}&key=AIzaSyCkV30fHW24EXjHvzYewtzQ7HzpN1zWMec"
    request = requests.get(query)
    return json.loads(request.content)["results"][0]["geometry"]["location"]


if __name__ == "__main__":
    mongo = MongoService(CONFIG.MONGO.URI, CONFIG.MONGO.DB_NAME)
    edrpou = "44858321"
    tender_id = "UA-2024-04-14-000214-a"
    region, city = build_entity_kattotg_hierarchy(mongo, edrpou)
    print(f"Entity region: {region} | City: {city}")
    region, city = build_tender_kattotg_hierarchy(mongo, "UA-2024-04-10-009873-a")
    print(f"Tender region: {region} | City: {city}")
