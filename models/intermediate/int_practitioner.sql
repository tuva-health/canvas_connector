with practitioner_base as (
    select
        id as practitioner_id
        , npi_number as npi
        , first_name
        , last_name
        , practice_location as practice_location_id
    from {{ ref('stg_canvas_staff') }}
),

practice_location as (
    select
        id as practice_location_id
        , full_name
    from {{ ref('stg_canvas_practice_location') }}
),

mapped_data as (
    select
        practitioner_id
        , npi
        , first_name
        , last_name
        , full_name as practice_affiliation
        , 'Canvas' as data_source
    from practitioner_base as pb
    left join practice_location as pl
        on pb.practice_location_id = pl.practice_location_id
)

select
      cast(practitioner_id as {{ dbt.type_string() }} ) as practitioner_id
    , cast(npi as {{ dbt.type_string() }} ) as npi
    , cast(first_name as {{ dbt.type_string() }} ) as first_name
    , cast(last_name as {{ dbt.type_string() }} ) as last_name
    , cast(practice_affiliation as {{ dbt.type_string() }} ) as practice_affiliation
    , cast(null as {{ dbt.type_string() }} ) as specialty
    , cast(null as {{ dbt.type_string() }} ) as sub_specialty
    , cast(data_source as {{ dbt.type_string() }} ) as data_source
    , cast(null as {{ dbt.type_string() }} ) as file_name
    , cast(null as {{ dbt.type_timestamp() }} ) as ingest_datetime
from mapped_data
