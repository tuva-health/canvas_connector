select
    id
    , dbid
    , created
    , modified
    , originator
    , committer
    , entered_in_error
    , deleted
    , patient
    , is_member_of
    , category
    , units
    , value
    , note_id
    , name
    , effective_datetime
from {{ source('canvas', 'api_observation') }}
where not coalesce(deleted, false)
and not coalesce(entered_in_error, false)
