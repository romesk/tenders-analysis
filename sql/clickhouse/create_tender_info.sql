CREATE TABLE default.TenderInfo
(
    `tender_id` String,
    `title` String DEFAULT 'n/s',
    `location` Nullable(String),
    `division` Nullable(String),
    `group` Nullable(String),
    `class_name` Nullable(String),
    `category` Nullable(String),
    `clarification` Nullable(String)
)
ENGINE = ReplacingMergeTree
ORDER BY (tender_id)
SETTINGS index_granularity = 8192