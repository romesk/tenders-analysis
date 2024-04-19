CREATE TABLE default.SaleActivity
(
    `price` Int32 DEFAULT '0',
    `time_spent` Int32 DEFAULT '0',
    `estimated_profit` Float32 DEFAULT '0',
    `manager_seniority` String DEFAULT 'def_seniority',
    `result_id` Nullable(Int32),
    `tender_id` Nullable(String),
    `manager_id` Nullable(String),
    `performer_id` Nullable(String),
    `start_all_time_id` Nullable(String),
    `end_all_time_id` Nullable(String)
)
ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY (estimated_profit, time_spent, price)
SETTINGS index_granularity = 8192