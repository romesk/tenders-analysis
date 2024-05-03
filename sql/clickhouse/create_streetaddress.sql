CREATE TABLE default.StreetAddress
(
    `id` String,
    `address` String,
    `longitude` Float32 DEFAULT '0.000',
    `latitude` Float32 DEFAULT '0.000',
    `city_katottg` String,
    `city_name` String,
    `region_katottg` String,
    `region_name` String
)
ENGINE = ReplacingMergeTree
ORDER BY (id)
SETTINGS index_granularity = 8192