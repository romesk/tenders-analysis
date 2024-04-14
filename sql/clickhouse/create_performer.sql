CREATE TABLE default.Performer
(
    `performer_id` String,
    `organization_type` String DEFAULT 'type_of_organization',
    `location` Nullable(Int32),
    `section_code` Nullable(Int32),
    `name` String DEFAULT 'name',
    `partition_code` Nullable(String),
    `group_code` Nullable(String),
    `class_code` Nullable(String)
)
ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY (performer_id, organization_type)
SETTINGS index_granularity = 8192