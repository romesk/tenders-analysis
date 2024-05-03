CREATE TABLE default.Stage
(
    `stage_id` String,
    `stage_name` String DEFAULT 'n/s',
    `stage_success` Int32 DEFAULT '0',
    `became_opportunity` Bool DEFAULT '0'
)
ENGINE = ReplacingMergeTree
ORDER BY (stage_id, stage_name)
SETTINGS index_granularity = 8192