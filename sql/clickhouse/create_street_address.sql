CREATE TABLE default.StreetAddress
(
    `address` String,
    `longitude` Float32 DEFAULT '0.000',
    `latitude` Float32 DEFAULT '0.000',
    `city_katottg` String,
    `region_katottg` String,
)
ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY ()
SETTINGS index_granularity = 8192;
