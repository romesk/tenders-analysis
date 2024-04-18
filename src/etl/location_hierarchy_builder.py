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


if __name__ == "__main__":
    edrpou = "44858321"
    region, city = build_entity_kattotg_hierarchy(mongo, edrpou)
    print(f"Region: {region} | City: {city}")
