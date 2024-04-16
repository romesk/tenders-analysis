CREATE TABLE default.TenderInfo
(
    `tender_id` String,
    `title` String DEFAULT 'Table Title',
    `delivery_address` Nullable(String),
    `division` Nullable(String),
    `group` Nullable(String),
    `class` Nullable(String),
    `category` Nullable(String),
    `clarification` Nullable(String)
)
ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY ()
SETTINGS index_granularity = 8192