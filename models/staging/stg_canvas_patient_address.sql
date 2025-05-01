select
    id
    , dbid
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
    , start
    , end
    , country
    , state
    , patient
from {{ source('canvas', 'api_patientaddress') }}
