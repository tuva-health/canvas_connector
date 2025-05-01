select
    id
    , dbid
    , created
    , modified
    , organization
    , place_of_service_code
    , full_name
    , short_name
    , background_image_url
    , background_gradient
    , active
    , npi_number
    , bill_through_organization
    , tax_id
    , tax_id_type
    , billing_location_name
    , group_npi_number
    , taxonomy_number
    , include_zz_qualifier
from {{ source('canvas', 'api_practicelocation') }}
