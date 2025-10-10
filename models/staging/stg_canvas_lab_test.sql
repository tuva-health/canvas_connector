select 
      id
    -- , dbid
    , ontology_test_name
    , ontology_test_code
    , status
    , report_id
    , aoe_code
    , procedure_class
    , specimen_type
    , specimen_source_code
    , specimen_source_description
    , specimen_source_coding_system
    , order_id
from {{ source('canvas', 'api_labtest') }}
