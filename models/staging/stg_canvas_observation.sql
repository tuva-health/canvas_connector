select
    id
    -- , dbid
    , created
    , modified
    , originator_id
    , committer_id
    , entered_in_error_id
    , deleted
    , patient_id
    , is_member_of_id
    , category
    , units
    , value
    , note_id
    , name
    , effective_datetime
from {{ source('canvas', 'api_observation') }}
where not coalesce(deleted, false)
and not coalesce(entered_in_error_id is not null, false)