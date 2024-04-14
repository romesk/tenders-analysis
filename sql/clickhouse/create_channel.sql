CREATE TABLE default.Channel
(
    `channel_id` Int32,
    `channel_name` String DEFAULT 'default_name'
)
ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY (channel_id, channel_name)
SETTINGS index_granularity = 8192