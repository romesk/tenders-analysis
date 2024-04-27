CREATE TABLE default.Channel
(
    `channel_id` String,
    `channel_name` String DEFAULT 'n/s'
)
ENGINE = ReplacingMergeTree
ORDER BY (channel_id)
SETTINGS index_granularity = 8192