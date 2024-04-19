CREATE TABLE default.Stage
(
    `stage_id` Int32,
    `stage_name` String DEFAULT 'default_name',
    `stage_success` Int32 DEFAULT '0',
    `became_opportunity` Bool DEFAULT '0'
)
ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY (stage_id, stage_name)
SETTINGS index_granularity = 8192