CREATE TABLE default.Campaign
(
    `campaign_id` String,
    `campaign_type` String DEFAULT 'n/s',
    `start_date` String,
    `end_date` Nullable(String),
    `duration_hours` Int32 DEFAULT '0'
)
ENGINE = ReplacingMergeTree
ORDER BY (campaign_id)
SETTINGS index_granularity = 8192