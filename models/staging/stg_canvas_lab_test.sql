select 
      id
    , dbid
    , ontology_test_name
    , ontology_test_code
    , status
    , report
    , aoe_code
    , procedure_class
    , specimen_type
    , specimen_source_code
    , specimen_source_description
    , specimen_source_coding_system
    , order
from {{ source('canvas', 'api_labtest') }}
