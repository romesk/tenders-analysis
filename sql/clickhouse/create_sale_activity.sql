CREATE TABLE default.SaleActivity
(
    `price` Int32 DEFAULT '0',
    `time_spent` Int32 DEFAULT '0',
    `estimated_profit` Float32 DEFAULT '0',
    `manager_seniority` String DEFAULT 'def_seniority',
    `result_id` Nullable(Int32),
    `tender_id` String,
    `manager_id` String,
    `performer_id` String,
    `start_all_time_id` Nullable(String),
    `end_all_time_id` Nullable(String)
)
ENGINE = ReplacingMergeTree
ORDER BY (tender_id, manager_id, performer_id)
SETTINGS index_granularity = 8192