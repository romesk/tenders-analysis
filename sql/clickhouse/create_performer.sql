CREATE TABLE default .Performer (
    `performer_id` String,
    `organization_type` String DEFAULT 'n/a',
    `location` Nullable(String),
    `section_code` Nullable(String),
    `name` String DEFAULT 'n/a',
    `partition_code` Nullable(String),
    `group_code` Nullable(String),
    `class_code` Nullable(String)
) ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY
    (performer_id, organization_type) SETTINGS index_granularity = 8192