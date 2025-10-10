with mapped_data as (
    select
        n.id as encounter_id --TODO: check if we need to bring in api_note from canvas
        , p.mrn as person_id
        , p.id as patient_id
        , note_type as encounter_type
        , datetime_of_service as encounter_start_date
        , datetime_of_service as encounter_end_date
        , pl.id as facility_id
        , pl.full_name as facility_name
        , n.provider_id as attending_provider_id
        , concat(coalesce(s.last_name, ''), 
            ', ', coalesce(s.first_name, '')) 
            as attending_provider_name
        , 'Canvas' as data_source
    from {{ ref('stg_canvas_note' )}} as n
    left join {{ ref('stg_canvas_patient') }} as p
        on n.patient_id = p.id
    left join {{ ref('stg_canvas_practice_location') }} as pl
        on n.location_id = pl.id
    left join {{ ref('stg_canvas_staff') }} as s
        on n.provider_id = s.id
)

    select 
      cast(encounter_id as {{ dbt.type_string() }} ) as encounter_id
    , cast(person_id as {{ dbt.type_string() }} ) as person_id
    , cast(patient_id as {{ dbt.type_string() }} ) as patient_id
    , cast(encounter_type as {{ dbt.type_string() }} ) as encounter_type
    , cast(encounter_start_date as date) as encounter_start_date
    , cast(encounter_end_date as date) as encounter_end_date
    , cast(null as {{ dbt.type_int() }} ) as length_of_stay
    , cast(null as {{ dbt.type_string() }} ) as admit_source_code
    , cast(null as {{ dbt.type_string() }} ) as admit_source_description
    , cast(null as {{ dbt.type_string() }} ) as admit_type_code
    , cast(null as {{ dbt.type_string() }} ) as admit_type_description
    , cast(null as {{ dbt.type_string() }} ) as discharge_disposition_code
    , cast(null as {{ dbt.type_string() }} ) as discharge_disposition_description
    , cast(attending_provider_id as {{ dbt.type_string() }} ) as attending_provider_id
    , cast(attending_provider_name as {{ dbt.type_string() }} ) as attending_provider_name
    , cast(facility_id as {{ dbt.type_string() }} ) as facility_id
    , cast(facility_id as {{ dbt.type_string() }} ) as facility_name
    , cast(null as {{ dbt.type_string() }} ) as primary_diagnosis_code_type
    , cast(null as {{ dbt.type_string() }} ) as primary_diagnosis_code
    , cast(null as {{ dbt.type_string() }} ) as primary_diagnosis_description
    , cast(null as {{ dbt.type_string() }} ) as drg_code_type
    , cast(null as {{ dbt.type_string() }} ) as drg_code
    , cast(null as {{ dbt.type_string() }} ) as drg_description
    , cast(null as {{ dbt.type_numeric() }} ) as paid_amount
    , cast(null as {{ dbt.type_numeric() }} ) as allowed_amount
    , cast(null as {{ dbt.type_numeric() }} ) as charge_amount
    , cast(data_source as {{ dbt.type_string() }} ) as data_source
    , cast(null as {{ dbt.type_string() }} ) as file_name
    , cast(null as {{ dbt.type_timestamp() }} ) as ingest_datetime
from mapped_data
