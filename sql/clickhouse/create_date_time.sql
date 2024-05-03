CREATE TABLE default.DateDim
(
    `day` String,
    `day_of_week` Int32,
    `day_of_month` Int32,
    `month` String,
    `quarter` Int32,
    `year` Int32
)
ENGINE = ReplacingMergeTree
ORDER BY (day)
SETTINGS index_granularity = 8192