select
    id
    -- , dbid
    , first_name
    , last_name
    , birth_date
    , sex_at_birth
    , created
    , modified
    , prefix
    , suffix
    , middle_name
    , maiden_name
    , nickname
    , sexual_orientation_term
    , sexual_orientation_code
    , gender_identity_term
    , gender_identity_code
    , preferred_pronouns
    , biological_race_codes
    , last_known_timezone
    , mrn
    , active
    , deceased
    , deceased_datetime
    , deceased_cause
    , deceased_comment
    , other_gender_description
    , social_security_number
    , administrative_note
    , clinical_note
    , mothers_maiden_name
    , multiple_birth_indicator
    , birth_order
    , default_location_id
    , default_provider_id
    , user_id
from {{ source('canvas', 'api_patient') }}
