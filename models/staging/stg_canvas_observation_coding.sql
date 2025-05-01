select
    dbid
    , system
    , version
    , code
    , display
    , user_selected
    , observation
from {{ source('canvas', 'api_observationcoding') }}
