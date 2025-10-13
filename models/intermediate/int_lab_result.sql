with lab_report as (
    select
        coalesce({{ sha_hash_512('lr.id') }}, {{ sha_hash_512('lrv.id') }}) as lab_result_id
      , lr.id as source_id
      , p.mrn as person_id
      , p.id as patient_id
      , null as encounter_id
      , null as accession_number
    --   , lvc.system as source_code_type
    --   , lvc.code as source_code
    --   , lvc.name as source_description
      , null as normalized_code_type
      , null as normalized_code
      , null as normalized_description
      , observation_status as status
      , lrv.internal_comment as result
      , lrv.created as result_date
      , lr.original_date as collection_date
      , lv.units as source_units
      , null as normalized_units
      , low_threshold as source_reference_range_low
      , high_threshold as source_reference_range_high
      , null as normalized_reference_range_low
      , null as normalized_reference_range_high
      , lv.abnormal_flag as source_abnormal_flag
      , case 
            when {{ dbt.safe_cast('lv.abnormal_flag', api.Column.translate_type("boolean") ) }} then '1'
            when not {{ dbt.safe_cast('lv.abnormal_flag', api.Column.translate_type("boolean") ) }} then '0'
        end as normalized_abnormal_flag
      , lt.specimen_type as specimen
      , lo.ordering_provider_id as ordering_practitioner_id
      , 'Canvas' as data_source
from {{ ref('stg_canvas_lab_report') }} as lr
left join {{ ref('stg_canvas_lab_value')}} as lv
    on lr.id = lv.report_id
-- left join stg_canvas_lab_value_coding as lvc --TODO: implement - missing from canvas for now
    -- on lv.id = lvc.value
left join {{ ref('stg_canvas_lab_review') }} as lrv
    on lr.review_id = lrv.id
left join {{ ref('stg_canvas_patient') }} as p
    on lr.patient_id = p.id
left join {{ ref('stg_canvas_lab_test') }} as lt
    on lt.report_id = lr.id
left join {{ ref('stg_canvas_lab_order') }} as lo
    on lt.order_id = lo.id
)

select
      cast(lab_result_id as {{ dbt.type_string() }} ) as lab_result_id
    , cast(person_id as {{ dbt.type_string() }} ) as person_id
    , cast(patient_id as {{ dbt.type_string() }} ) as patient_id
    , cast(encounter_id as {{ dbt.type_string() }} ) as encounter_id
    , cast(accession_number as {{ dbt.type_string() }} ) as accession_number
    , cast(null as {{ dbt.type_string() }} ) as source_code_type
    , cast(null as {{ dbt.type_string() }} ) as source_code
    , cast(null as {{ dbt.type_string() }} ) as source_description
    , cast(null as {{ dbt.type_string() }} ) as source_component
    , cast(normalized_code_type as {{ dbt.type_string() }} ) as normalized_code_type
    , cast(normalized_code as {{ dbt.type_string() }} ) as normalized_code
    , cast(normalized_description as {{ dbt.type_string() }} ) as normalized_description
    , cast(null as {{ dbt.type_string() }} ) as normalized_component
    , cast(status as {{ dbt.type_string() }} ) as status
    , cast(result as {{ dbt.type_string() }} ) as result
    , cast(result_date as date) as result_date
    , cast(collection_date as date) as collection_date
    , cast(source_units as {{ dbt.type_string() }} ) as source_units
    , cast(normalized_units as {{ dbt.type_string() }} ) as normalized_units
    , cast(source_reference_range_low as {{ dbt.type_string() }} ) as source_reference_range_low
    , cast(source_reference_range_high as {{ dbt.type_string() }} ) as source_reference_range_high
    , cast(normalized_reference_range_low as {{ dbt.type_string() }} ) as normalized_reference_range_low
    , cast(normalized_reference_range_high as {{ dbt.type_string() }} ) as normalized_reference_range_high
    , cast(source_abnormal_flag as {{ dbt.type_int() }} ) as source_abnormal_flag
    , cast(normalized_abnormal_flag as {{ dbt.type_int() }} ) as normalized_abnormal_flag
    , cast(specimen as {{ dbt.type_string() }} ) as specimen
    , cast(ordering_practitioner_id as {{ dbt.type_string() }} ) as ordering_practitioner_id
    , cast(data_source as {{ dbt.type_string() }} ) as data_source
    , cast(null as {{ dbt.type_string() }} ) as file_name
    , cast(null as {{ dbt.type_timestamp() }} ) as ingest_datetime
from lab_report
