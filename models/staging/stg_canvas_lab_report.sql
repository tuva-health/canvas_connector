select
      id
    -- , dbid
    , created
    , modified
    , review_mode
    , junked
    , requires_signature
    , assigned_date
    , patient_id
    , transmission_type
    , for_test_only
    , external_id
    , version
    , requisition_number
    , review_id
    , original_date
    , date_performed
    , custom_document_name
    , originator_id
    , committer_id
    , entered_in_error_id
    , deleted
from {{ source('canvas', 'api_labreport') }}
where not coalesce(deleted, false)
and not coalesce(entered_in_error_id is not null, false)