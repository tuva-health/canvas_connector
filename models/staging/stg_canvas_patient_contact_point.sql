select
      id
    -- , dbid
    , system
    , value
    , use
    , use_notes
    , rank
    , state
    , patient_id
    , has_consent
    , last_verified
    , verification_token
    , opted_out
from {{ source('canvas', 'api_patientcontactpoint') }}
