CREATE TABLE default.Region
(
    `region_katottg` String,
    `region_name` String,
)
ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY ()
SETTINGS index_granularity = 8192;