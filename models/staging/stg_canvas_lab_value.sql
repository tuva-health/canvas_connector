select
    id,
    created,
    modified,
    report_id,
    value,
    units,
    abnormal_flag,
    reference_range,
    low_threshold,
    high_threshold,
    comment,
    test_id,
    performer_id,
    observation_status
from {{ source('canvas', 'api_labvalue') }}
