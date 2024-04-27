CREATE TABLE default.TenderClosed
(
    `amount` Float32 DEFAULT '0',
    `duration` Int32 DEFAULT '0',
    `participant_count` Int32 DEFAULT '0',
    `open_time_id` Nullable(String),
    `close_time_id` Nullable(String),
    `tender_id` String,
    `procurement_id` Nullable(String),
    `performer_id` Nullable(String)
)
ENGINE = ReplacingMergeTree
ORDER BY (tender_id)
SETTINGS index_granularity = 8192