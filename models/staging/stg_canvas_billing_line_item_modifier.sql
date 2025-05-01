select    
    dbid
    , system
    , version
    , code
    , display
    , user_selected
    , line_item
from {{ source('canvas', 'api_billinglineitemmodifier') }}
