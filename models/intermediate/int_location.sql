with location_base as (
    select
        id as location_id
        , cast(npi_number as {{ dbt.type_string() }}) as npi
        , full_name as name
        , organization_id as parent_organization
        , taxonomy_number as facility_type
        , 'Canvas' as data_source
    from {{ ref('stg_canvas_practice_location') }} as pl
),

mapped_data as (
    select
        location_id
        , location_base.npi
        , name
        , parent_organization
        , facility_type
        , p.practice_address_line_1 as address
        , p.practice_city as city
        , p.practice_state as state
        , p.practice_zip_code as zip_code
        , null as latitude
        , null as longitude
        , data_source
    from location_base
    left join {{ ref('terminology__provider') }} as p
    on location_base.npi = p.npi
)


select
      cast(location_id as {{ dbt.type_string() }} ) as location_id
    , cast(npi as {{ dbt.type_string() }} ) as npi
    , cast(name as {{ dbt.type_string() }} ) as name
    , cast(facility_type as {{ dbt.type_string() }} ) as facility_type
    , cast(parent_organization as {{ dbt.type_string() }} ) as parent_organization
    , cast(address as {{ dbt.type_string() }} ) as address
    , cast(city as {{ dbt.type_string() }} ) as city
    , cast(state as {{ dbt.type_string() }} ) as state
    , cast(zip_code as {{ dbt.type_string() }} ) as zip_code
    , cast(latitude as {{ dbt.type_float() }} ) as latitude
    , cast(longitude as {{ dbt.type_float() }} ) as longitude
    , cast(data_source as {{ dbt.type_string() }} ) as data_source
    , cast(null as {{ dbt.type_string() }} ) as file_name
    , cast(null as {{ dbt.type_timestamp() }} ) as ingest_datetime
from mapped_data
