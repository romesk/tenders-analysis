CREATE TABLE default.DateTime
(
    `day` String,
    `day_of_week` Int32,
    `day_of_month` Int32,
    `month` String,
    `quarter` String,
    `year` Int32
)
ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY (day, day_of_week, day_of_month, month, quarter, year)
SETTINGS index_granularity = 8192