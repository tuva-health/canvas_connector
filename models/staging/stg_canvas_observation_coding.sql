select
    -- dbid
    id
    , system
    , version
    , code
    , display
    , user_selected
    , observation_id
from {{ source('canvas', 'api_observationcoding') }}
