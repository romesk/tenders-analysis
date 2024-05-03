CREATE TABLE default.Manager
(
    `manager_id` String,
    `name` String DEFAULT 'n/s'
)
ENGINE = ReplacingMergeTree
ORDER BY (manager_id)
SETTINGS index_granularity = 8192