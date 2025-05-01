select
      id
    , dbid
    , deleted
    , entered_in_error
    , committer
    , patient
    , onset_date
    , resolution_date
    , clinical_status
from {{ source('canvas', 'api_condition') }}
where not coalesce(deleted, false)
and not coalesce(entered_in_error, false)
