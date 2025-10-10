select
    -- dbid
    id
    , system
    , version
    , code
    , display
    , user_selected
    , condition_id
from {{ source('canvas', 'api_conditioncoding') }}