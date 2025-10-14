with condition_base as (
  select
    {{ sha_hash_512('concat(c.id, coalesce(cc.id, c.id))') }} as condition_id
    , p.mrn as person_id
    , p.id as patient_id
    , null as encounter_id
    , null as claim_id
    , null as recorded_date
    , onset_date
    , c.resolution_date as resolved_date
    , c.clinical_status as status
    , null as condition_type
    , case 
        when cc.system = 'ICD-10' then 'icd-10-cm'
        else cc.system
        end as source_code_type
    , cc.code as source_code -- multiple code and code systems causing duplicate condition_ids
    , cc.display as source_description
    , 'Canvas' as data_source
  from {{ ref('stg_canvas_condition') }} as c
  left join {{ ref('stg_canvas_patient') }} as p
    on c.patient_id = p.id
  left join {{ ref('stg_canvas_condition_coding') }} as cc
    on c.id = cc.condition_id
)

select
      cast(condition_id as {{ dbt.type_string() }} ) as condition_id
    , cast(person_id as {{ dbt.type_string() }} ) as person_id
    , cast(patient_id as {{ dbt.type_string() }} ) as patient_id
    , cast(encounter_id as {{ dbt.type_string() }} ) as encounter_id
    , cast(claim_id as {{ dbt.type_string() }} ) as claim_id
    , cast(recorded_date as date) as recorded_date
    , cast(onset_date as date) as onset_date
    , cast(resolved_date as date) as resolved_date
    , cast(status as {{ dbt.type_string() }} ) as status
    , cast(condition_type as {{ dbt.type_string() }} ) as condition_type
    , cast(source_code_type as {{ dbt.type_string() }} ) as source_code_type
    , cast(source_code as {{ dbt.type_string() }} ) as source_code
    , cast(source_description as {{ dbt.type_string() }} ) as source_description
    , cast(null as {{ dbt.type_string() }} ) as normalized_code_type
    , cast(null as {{ dbt.type_string() }} ) as normalized_code
    , cast(null as {{ dbt.type_string() }} ) as normalized_description
    , cast(null as {{ dbt.type_int() }} ) as condition_rank
    , cast(null as {{ dbt.type_string() }} ) as present_on_admit_code
    , cast(null as {{ dbt.type_string() }} ) as present_on_admit_description
    , cast(data_source as {{ dbt.type_string() }} ) as data_source
    , cast(null as {{ dbt.type_string() }} ) as file_name
    , cast(null as {{ dbt.type_timestamp() }} ) as ingest_datetime
from condition_base
