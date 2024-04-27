CREATE TABLE default.Region
(
    `region_katottg` String,
    `region_name` String DEFAULT 'n/s'
)
ENGINE = ReplacingMergeTree
ORDER BY (region_katottg)
SETTINGS index_granularity = 8192