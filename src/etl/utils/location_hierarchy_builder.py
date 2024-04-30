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
    last_level_katottg = entity["address"][0]["creator"]["link"]
    if not entity or not last_level_katottg:
        return None, None, None
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
            {"level1": city["level1"], "level2": None, "level3": None, "level4": None, "level5": None},
        )
        print(edrpou, "  IN CITY_REGION: city", city, " city-region: ", city_region)
    else:
        city = mongo.find_one(CONFIG.MONGO.KATOTTG_COLLECTION,
                              {"level1": {"$ne": None}, "level2": {"$ne": None}, "level3": {"$ne": None},
                               "level4": last_level_katottg, "level5": None})
        region = mongo.find_one(
            CONFIG.MONGO.KATOTTG_COLLECTION,
            {"level1": city["level1"], "level2": None, "level3": None, "level4": None, "level5": None},
        )
        print(edrpou, "  IN CITY")

    return (
        entity["address"][0]["creator"]["boldText"],
        (city["name"], city["level4"]),
        (region["name"], region["level1"]),
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
        {"level1": region["level1"], "level2": {"$ne": None}, "level3": {"$ne": None}, "level4": {"$ne": None},
         "level5": None, "name": tender_city_name},
    )

    return (
        tender_address,
        (city["name"], city["level4"]),
        (region["name"], region["level1"])
    )


def get_coordinates(address: str):
    query = f"https://maps.googleapis.com/maps/api/geocode/json?address={address}&key=AIzaSyCkV30fHW24EXjHvzYewtzQ7HzpN1zWMec"
    request = requests.get(query)
    return json.loads(request.content)["results"][0]["geometry"]["location"]


if __name__ == "__main__":
    mongo = MongoService(CONFIG.MONGO.URI, CONFIG.MONGO.DB_NAME)
    tender_id = "UA-2024-04-26-010984-a"

    tender = mongo.find_one(CONFIG.MONGO.TENDERS_COLLECTION, {"tenderID": tender_id})
    edrpou = "44858321"
    address, city, region = build_entity_kattotg_hierarchy('')
    print(f"Tender region: {region} | City: {city}")
