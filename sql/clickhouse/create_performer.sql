CREATE TABLE default.Performer
(
    `performer_id` String,
    `organization_type` String DEFAULT 'n/s',
    `location` Nullable(String),
    `class_name` String DEFAULT 'n/s',
    `section_name` String DEFAULT 'n/s',
    `partition_name` String DEFAULT 'n/s',
    `group_name` String DEFAULT 'n/s',
    `section_code` Nullable(String),
    `partition_code` Nullable(String),
    `group_code` Nullable(String),
    `class_code` Nullable(String)
)
ENGINE = ReplacingMergeTree
ORDER BY (performer_id)
SETTINGS index_granularity = 8192