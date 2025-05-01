select
      id
    , dbid
    , patient
    , deleted
    , entered_in_error
    , committer
    , status
    , start_date
    , end_date
    , quantity_qualifier_description
    , clinical_quantity_description
    , potency_unit_code
    , national_drug_code
    , erx_quantity
from {{ source('canvas', 'api_medication') }}
where not coalesce(deleted, false)
and not coalesce(entered_in_error, false)