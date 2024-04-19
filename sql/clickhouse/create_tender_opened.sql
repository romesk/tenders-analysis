CREATE TABLE default.TenderOpened
(
    `amount` Int32 DEFAULT '0',
    `time_to_end` Int32 DEFAULT '0',
    `open_time_id` Nullable(String) DEFAULT '0',
    `close_time_id` Nullable(String) DEFAULT '0',
    `tender_id` Nullable(String) DEFAULT '0',
    `procurement_id` Nullable(String) DEFAULT '0'
)
ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY (amount, time_to_end)
SETTINGS index_granularity = 8192