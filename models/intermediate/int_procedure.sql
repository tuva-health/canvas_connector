with patient as (
    select
        id,
        mrn
    from {{ ref('stg_canvas_patient') }}
),

note as (
    select
        id,
        provider_id,
        datetime_of_service
    from {{ ref('stg_canvas_note') }}
),

 mapped_data as (
   select
          b.id as procedure_id
        , p.id as patient_id
        , p.mrn as person_id
        , n.id as encounter_id
        , null as claim_id
        , n.datetime_of_service as procedure_date
        , 'hcpcs' as source_code_type
        , b.cpt as source_code
        , null as source_description
        , case 
            when hcpcs.hcpcs is not null then 'hcpcs' 
            end as normalized_code_type
        , hcpcs.hcpcs as normalized_code
        , hcpcs.long_description as normalized_description
        , null as modifier_1
        , null as modifier_2
        , null as modifier_3
        , null as modifier_4
        , null as modifier_5
        , n.provider_id as practitioner_id
        , 'Canvas' as data_source
    from {{ ref('stg_canvas_billing_line_item') }} as b
    left join patient as p
        on b.patient_id = p.id
    left join note as n
        on b.note_id = n.id
    left join {{ ref('terminology__hcpcs_level_2') }} as hcpcs
        on b.cpt = hcpcs.hcpcs
)



select
      cast(procedure_id as {{ dbt.type_string() }} ) as procedure_id
    , cast(person_id as {{ dbt.type_string() }} ) as person_id
    , cast(patient_id as {{ dbt.type_string() }} ) as patient_id
    , cast(encounter_id as {{ dbt.type_string() }} ) as encounter_id
    , cast(claim_id as {{ dbt.type_string() }} ) as claim_id
    , cast(procedure_date as date) as procedure_date
    , cast(source_code_type as {{ dbt.type_string() }} ) as source_code_type
    , cast(source_code as {{ dbt.type_string() }} ) as source_code
    , cast(source_description as {{ dbt.type_string() }} ) as source_description
    , cast(normalized_code_type as {{ dbt.type_string() }} ) as normalized_code_type
    , cast(normalized_code as {{ dbt.type_string() }} ) as normalized_code
    , cast(normalized_description as {{ dbt.type_string() }} ) as normalized_description
    , cast(modifier_1 as {{ dbt.type_string() }} ) as modifier_1
    , cast(modifier_2 as {{ dbt.type_string() }} ) as modifier_2
    , cast(modifier_3 as {{ dbt.type_string() }} ) as modifier_3
    , cast(modifier_4 as {{ dbt.type_string() }} ) as modifier_4
    , cast(modifier_5 as {{ dbt.type_string() }} ) as modifier_5
    , cast(practitioner_id as {{ dbt.type_string() }} ) as practitioner_id
    , cast(data_source as {{ dbt.type_string() }} ) as data_source
    , cast(null as {{ dbt.type_string() }} ) as file_name
    , cast(null as {{ dbt.type_timestamp() }} ) as ingest_datetime
from mapped_data
