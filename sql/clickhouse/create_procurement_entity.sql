CREATE TABLE default.ProcurementEntity
(
    `entity_id` String,
    `legal_name` String,
    `contact_name` String DEFAULT 'name',
    `contact_phone` String DEFAULT 'phone'
)
ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY entity_id
SETTINGS index_granularity = 8192