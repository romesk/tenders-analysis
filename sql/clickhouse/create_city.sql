CREATE TABLE default.City
(
    `city_katottg` String,
    `city_name` String DEFAULT 'n/s',
    `region_katottg` String,
    `region_name` String DEFAULT 'n/s'
)
ENGINE = ReplacingMergeTree
ORDER BY (city_katottg, region_katottg)
SETTINGS index_granularity = 8192