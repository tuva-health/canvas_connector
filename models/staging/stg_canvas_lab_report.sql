select
      id
    , dbid
    , created
    , modified
    , review_mode
    , junked
    , requires_signature
    , assigned_date
    , patient
    , transmission_type
    , for_test_only
    , external_id
    , version
    , requisition_number
    , review
    , original_date
    , date_performed
    , custom_document_name
    , originator
    , committer
    , entered_in_error
    , deleted
from {{ source('canvas', 'api_labreport') }}
where not coalesce(deleted, false)
and not coalesce(entered_in_error, false)
