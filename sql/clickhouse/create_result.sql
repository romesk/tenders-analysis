CREATE TABLE default.Result
(
    `result_id` Int32,
    `stage` String
)
ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY (result_id, stage)
SETTINGS index_granularity = 8192