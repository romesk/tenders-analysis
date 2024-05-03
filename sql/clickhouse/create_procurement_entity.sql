CREATE TABLE default.ProcurementEntity
(
    `entity_id` String,
    `legal_name` String DEFAULT 'n/s',
    `contact_name` String DEFAULT 'n/s',
    `contact_phone` String DEFAULT 'n/s'
)
ENGINE = ReplacingMergeTree
ORDER BY (entity_id)
SETTINGS index_granularity = 8192