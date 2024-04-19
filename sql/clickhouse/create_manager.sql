CREATE TABLE default.Manager
(
    `manager_id` String,
    `name` String DEFAULT 'default_name'
)
ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY manager_id
SETTINGS index_granularity = 8192