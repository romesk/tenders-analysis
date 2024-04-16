CREATE TABLE default.TenderClosed
(
    `amount` Int32 DEFAULT '0',
    `duration` Int32 DEFAULT '0',
    `participant_count` Int32 DEFAULT '0',
    `open_time_id` Nullable(String),
    `close_time_id` Nullable(String),
    `tender_id` Nullable(String),
    `procurement_id` Nullable(String),
    `performer_id` Nullable(String)
)
ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY (duration, participant_count, amount)
SETTINGS index_granularity = 8192