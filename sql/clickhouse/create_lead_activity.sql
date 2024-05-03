CREATE TABLE default.LeadActivity
(
    `sucess_rate` Int32 DEFAULT '0',
    `time_from_prev_stage` Int32 DEFAULT '0',
    `activities_from_last_stage` Int32 DEFAULT '0',
    `feedback_from_last_stage` Int32 DEFAULT '0',
    `manager_id` Int32,
    `performer_id` Int32,
    `campaing_id` Int32,
    `channel_id` Int32,
    `prev_stage_id` Nullable(Int32),
    `curr_stage_id` Int32,
    `time_id` Nullable(Int32)
)
ENGINE = ReplacingMergeTree
ORDER BY (manager_id, performer_id, campaing_id, channel_id, curr_stage_id)
SETTINGS index_granularity = 8192