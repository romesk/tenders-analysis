CREATE TABLE default.LeadActivity
(
    `sucess_rate` Int32 DEFAULT '0',
    `time_from_prev_stage` Int32 DEFAULT '0',
    `activities_from_last_stage` Int32 DEFAULT '0',
    `feedback_from_last_stage` Int32 DEFAULT '0',
    `manager_id` Nullable(Int32),
    `campaing_id` Nullable(Int32),
    `channel_id` Nullable(Int32),
    `prev_stage_id` Nullable(Int32),
    `curr_stage_id` Int32,
    `time_id` Nullable(String)
)
ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY (sucess_rate, time_from_prev_stage, activities_from_last_stage, feedback_from_last_stage)
SETTINGS index_granularity = 8192