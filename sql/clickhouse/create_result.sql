CREATE TABLE default.Result
(
    `result_id` Int32,
    `stage` String
)
ENGINE = ReplacingMergeTree
ORDER BY (result_id)
SETTINGS index_granularity = 8192