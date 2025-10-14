with mapped_data as (
    select
        {{ sha_hash_512('concat(m.id, coalesce(pr.id, m.id))') }} as medication_id 
        , p.mrn as person_id
        , p.id as patient_id
        , null as encounter_id
        , pr.created as dispensing_date --multiple dispenses causing duplicates (duplicate medication_id)
        , pr.written_date as prescribing_date
        , 'ndc' as source_code_type
        , m.national_drug_code as source_code
        , null as source_description
        , m.national_drug_code as ndc_code
        , null as ndc_description
        , m.potency_unit_code as strength
        , pr.dispense_quantity as quantity
        , null as quantity_unit
        , pr.days_supply
        , pr.prescriber_id as practitioner_id
        , 'Canvas' as data_source
    from {{ ref('stg_canvas_medication') }} as m
    left join {{ ref('stg_canvas_patient')}} as p
        on m.patient_id = p.id
    left join {{ ref('stg_canvas_prescription') }} as pr
        on m.id = pr.medication_id
    left join {{ ref('terminology__ndc') }} as ndc
        on m.national_drug_code = ndc.ndc
)

select 
      cast(medication_id as {{ dbt.type_string() }} ) as medication_id
    , cast(person_id as {{ dbt.type_string() }} ) as person_id
    , cast(patient_id as {{ dbt.type_string() }} ) as patient_id
    , cast(encounter_id as {{ dbt.type_string() }} ) as encounter_id
    , cast(dispensing_date as date) as dispensing_date
    , cast(prescribing_date as date) as prescribing_date
    , cast(source_code_type as {{ dbt.type_string() }} ) as source_code_type
    , cast(source_code as {{ dbt.type_string() }} ) as source_code
    , cast(source_description as {{ dbt.type_string() }} ) as source_description
    , cast(ndc_code as {{ dbt.type_string() }} ) as ndc_code
    , cast(ndc_description as {{ dbt.type_string() }} ) as ndc_description
    -- , cast(rxnorm_code as {{ dbt.type_string() }} ) as rxnorm_code
    -- , cast(rxnorm_description as {{ dbt.type_string() }} ) as rxnorm_description
    -- , cast(atc_code as {{ dbt.type_string() }} ) as atc_code
    -- , cast(atc_description as {{ dbt.type_string() }} ) as atc_description
    -- , cast(route as {{ dbt.type_string() }} ) as route
    , cast(strength as {{ dbt.type_string() }} ) as strength
    , cast(quantity as {{ dbt.type_int() }} ) as quantity
    , cast(quantity_unit as {{ dbt.type_string() }} ) as quantity_unit
    , cast(days_supply as {{ dbt.type_int() }} ) as days_supply
    , cast(practitioner_id as {{ dbt.type_string() }} ) as practitioner_id
    , cast(data_source as {{ dbt.type_string() }} ) as data_source
    , cast(null as {{ dbt.type_string() }} ) as file_name
    , cast(null as {{ dbt.type_timestamp() }} ) as ingest_datetime
from mapped_data
