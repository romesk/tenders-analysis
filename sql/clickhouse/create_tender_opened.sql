CREATE TABLE default.TenderOpened
(
    `amount` Float32 DEFAULT '0',
    `time_to_end` Int32 DEFAULT '0',
    `open_time_id` Nullable(String),
    `close_time_id` Nullable(String),
    `tender_id` String DEFAULT '0',
    `procurement_id` Nullable(String) DEFAULT '0'
)
ENGINE = ReplacingMergeTree
ORDER BY (tender_id)
SETTINGS index_granularity = 8192