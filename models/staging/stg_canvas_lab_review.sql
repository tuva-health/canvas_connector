select
    id
    -- , dbid
    , created
    , modified
	, originator_id
	, editors
	, deleted
	, committer_id
	, entered_in_error_id
	, note_id
	, internal_comment
	, message_to_patient
	, is_released_to_patient
	, status
	, patient_id
	, patient_communication_method
	, externally_exposable_id
from {{ source('canvas', 'api_labreview') }}
WHERE not coalesce(entered_in_error_id is not null, false)