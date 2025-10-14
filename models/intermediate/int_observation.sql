with mapped_data as (
    select
        {{ sha_hash_512('concat(o.id, coalesce(c.id, o.id))') }} as observation_id
        , p.mrn as person_id
        , p.id as patient_id
        , o.note_id as encounter_id
        , null as panel_id
        , o.effective_datetime as observation_date
        , o.category as observation_type
        , c.system as source_code_type
        , c.code as source_code -- multiple source codes causing duplicate obvservation_ids
        , c.display as source_description
        , null as normalized_code_type
        , null as normalized_code
        , null as normalized_description
        , o.value as result
        , o.units as source_units
        , null as normalized_units
        , null as source_reference_range_low
        , null as source_reference_range_high 
        , null as normalized_reference_range_low
        , null as normalized_reference_range_high
        , 'Canvas' as data_source
    from {{ ref('stg_canvas_observation') }} as o
    left join {{ ref('stg_canvas_patient') }} as p
        on o.patient_id = p.id
    left join {{ ref('stg_canvas_observation_coding') }} as c
        on o.id = c.observation_id
)

select
      cast(observation_id as {{ dbt.type_string() }} ) as observation_id
    , cast(person_id as {{ dbt.type_string() }} ) as person_id
    , cast(patient_id as {{ dbt.type_string() }} ) as patient_id
    , cast(encounter_id as {{ dbt.type_string() }} ) as encounter_id
    , cast(panel_id as {{ dbt.type_string() }} ) as panel_id
    , cast(observation_date as date) as observation_date
    , cast(observation_type as {{ dbt.type_string() }} ) as observation_type
    , cast(source_code_type as {{ dbt.type_string() }} ) as source_code_type
    , cast(source_code as {{ dbt.type_string() }} ) as source_code
    , cast(source_description as {{ dbt.type_string() }} ) as source_description
    , cast(normalized_code_type as {{ dbt.type_string() }} ) as normalized_code_type
    , cast(normalized_code as {{ dbt.type_string() }} ) as normalized_code
    , cast(normalized_description as {{ dbt.type_string() }} ) as normalized_description
    , cast(result as {{ dbt.type_string() }} ) as result
    , cast(source_units as {{ dbt.type_string() }} ) as source_units
    , cast(normalized_units as {{ dbt.type_string() }} ) as normalized_units
    , cast(source_reference_range_low as {{ dbt.type_string() }} ) as source_reference_range_low
    , cast(source_reference_range_high as {{ dbt.type_string() }} ) as source_reference_range_high
    , cast(normalized_reference_range_low as {{ dbt.type_string() }} ) as normalized_reference_range_low
    , cast(normalized_reference_range_high as {{ dbt.type_string() }} ) as normalized_reference_range_high
    , cast(data_source as {{ dbt.type_string() }} ) as data_source
    , cast(null as {{ dbt.type_string() }} ) as file_name
    , cast(null as {{ dbt.type_timestamp() }} ) as ingest_datetime
from mapped_data
