select
    id
    -- , dbid
    , created
    , modified
    , originator_id
    , deleted
    , committer_id
    , entered_in_error_id
    , patient_id
    , ontology_lab_partner
    , comment
    , requisition_number
    , is_patient_bill
    , date_ordered
    , fasting_status
    , specimen_collection_type
    , transmission_type
    , courtesy_copy_type
    , courtesy_copy_number
    , courtesy_copy_text
    , ordering_provider_id
    , parent_order_id
    , healthgorilla_id
    , manual_processing_status
    , manual_processing_comment
    , labcorp_abn_url
from {{ source('canvas', 'api_laborder') }}
