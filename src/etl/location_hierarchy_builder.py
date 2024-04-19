from typing import Tuple, Any

from src.config import CONFIG
from src.services import MongoService

mongo = MongoService(CONFIG.MONGO.URI, CONFIG.MONGO.DB_NAME)

kyiv_kattotg = 'UA80000000000093317'


def build_entity_kattotg_hierarchy(mongo: MongoService, edrpou: str) -> tuple[Any, Any]:
    entity = mongo.find_one(CONFIG.MONGO.ENTITIES_COLLECTION, {'edrpou': edrpou})
    last_level_katottg = entity['address'][0]['creator']['link']
    city_region = mongo.find_one(CONFIG.MONGO.KATOTTG_COLLECTION, {"level5": last_level_katottg})
    if city_region and city_region['level1'] == kyiv_kattotg:
        return ("Київ", kyiv_kattotg), ("Київ", kyiv_kattotg)
    elif city_region:
        city = mongo.find_one(CONFIG.MONGO.KATOTTG_COLLECTION,
                              {"level1": city_region["level1"],
                               "level2": city_region["level2"],
                               "level3": city_region["level3"],
                               "level4": city_region["level4"],
                               "level5": None})
        region = mongo.find_one(CONFIG.MONGO.KATOTTG_COLLECTION,
                                {"level1": city_region["level1"],
                                 "level2": None,
                                 "level3": None,
                                 "level4": None,
                                 "level5": None})
    else:
        city = mongo.find_one(CONFIG.MONGO.KATOTTG_COLLECTION,
                              {"level4": last_level_katottg,
                               "level5": None})
        region = mongo.find_one(CONFIG.MONGO.KATOTTG_COLLECTION,
                                {"level1": city["level1"],
                                 "level2": None,
                                 "level3": None,
                                 "level4": None,
                                 "level5": None})

    return (region['name'], region['level1']), (city['name'], city['level4'])


def build_tender_kattotg_hierarchy(mongo: MongoService, tender_id: str) -> tuple[Any, Any]:
    tender = mongo.find_one(CONFIG.MONGO.TENDERS_COLLECTION, {'tenderID': tender_id})
    tender_city_name = tender["items"][0]["deliveryAddress"]['locality'].replace(".", " ").split()[-1]
    tender_region_name = tender["items"][0]["deliveryAddress"]['region'].replace(".", " ").split()[0]

    if tender_city_name == "Київ" and tender_region_name == "Київська":
        return ("Київ", kyiv_kattotg), ("Київ", kyiv_kattotg)

    region = mongo.find_one(CONFIG.MONGO.KATOTTG_COLLECTION,
                            {"name": tender_region_name,
                             "level2": None,
                             "level3": None,
                             "level4": None,
                             "level5": None})
    city = mongo.find_one(CONFIG.MONGO.KATOTTG_COLLECTION,
                          {"level1": region["level1"],
                           "category": {"$exists": True},
                           "name": tender_city_name})

    return (region['name'], region['level1']), (city['name'], city['level4'])


if __name__ == "__main__":
    edrpou = "44858321"
    tender_id = "UA-2024-04-14-000214-a"
    region, city = build_entity_kattotg_hierarchy(mongo, edrpou)
    print(f"Entity region: {region} | City: {city}")
    region, city = build_tender_kattotg_hierarchy(mongo, 'UA-2024-04-10-009873-a')
    print(f"Tender region: {region} | City: {city}")
