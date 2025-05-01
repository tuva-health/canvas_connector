select
      dbid
    , created
    , modified
    , value
    , code
    , name
    , system
from {{ source('canvas', 'api_labvaluecoding') }}
