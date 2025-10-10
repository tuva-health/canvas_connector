select
      id
    -- , dbid
    , created
    , modified
    , report_id
    , value
    , units
    , abnormal_flag
    , reference_range
    , low_threshold
    , high_threshold
    , comment
    , observation_status
from {{ source('canvas', 'api_labvalue') }}
