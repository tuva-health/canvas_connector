select
    dbid
    system
    version
    code
    display
    user_selected
    condition
from {{ source('canvas', 'api_conditioncoding') }}