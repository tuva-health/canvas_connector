select
    id
    -- , dbid
    , created
    , modified
    , note_id
    , patient_id
    , cpt
    , charge
    , description
    , units
    , command_type
    , command_id
    , status
from {{ source('canvas', 'api_billinglineitem') }}
