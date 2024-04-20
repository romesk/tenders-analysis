CREATE TABLE default.City
(
    `city_katottg` String,
    `city_name` String,
    `region_katottg` String,

)
ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY ()
SETTINGS index_granularity = 8192;
