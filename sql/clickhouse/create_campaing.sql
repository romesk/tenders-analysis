CREATE TABLE default.Campaing
(
    `campaing_id` Int32,
    `type` String DEFAULT 'default_type',
    `start_date` Nullable(String),
    `end_date` Nullable(String),
    `duration_hours` Int32 DEFAULT '0'
)
ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY (start_date, end_date, duration_hours, campaing_id)
SETTINGS index_granularity = 8192