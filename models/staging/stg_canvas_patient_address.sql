select
    id
    -- , dbid
    , line1
    , line2
    , city
    , district
    , state_code
    , postal_code
    , use
    , type
    , longitude
    , latitude
    , "START"
    , "END"
    , country
    , state
    , patient_id
from {{ source('canvas', 'api_patientaddress') }}
