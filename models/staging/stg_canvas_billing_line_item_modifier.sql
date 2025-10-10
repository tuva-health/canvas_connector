select    
    -- dbid
    id
    , system
    , version
    , code
    , display
    , user_selected
    , line_item_id
from {{ source('canvas', 'api_billinglineitemmodifier') }}
