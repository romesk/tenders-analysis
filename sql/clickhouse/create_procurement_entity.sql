CREATE TABLE default.ProcurementEntity
(
    `entity_id` String,
    `legal_name` String DEFAULT 'legal_name',
    `contact_name` String DEFAULT 'contact_name'
)
ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY entity_id
SETTINGS index_granularity = 8192