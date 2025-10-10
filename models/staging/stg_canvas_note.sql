select
    id
    -- dbid
    , created
    , modified
    , patient_id
    , provider_id
    , note_type
    , note_type_version_id
    , title
    , _body
    , originator_id
    , last_modified_by_staff_id
    , checksum
    , billing_note
    , related_data
    , location_id
    , datetime_of_service
    , place_of_service
from {{ source('canvas', 'api_note') }}
