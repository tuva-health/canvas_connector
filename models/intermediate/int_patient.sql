with patient_base as (
    select
        mrn as person_id
        , id as patient_id
        , first_name
        , last_name
        , case
            when sex_at_birth in ('F', 'female') then 'female'
            when sex_at_birth in ('M', 'male') then 'male'
            else 'unknown'
          end as sex
        , biological_race_codes as race
        , birth_date
        , deceased_datetime as death_date
        , deceased as death_flag
        , social_security_number
    from {{ ref('stg_canvas_patient') }}
),

patient_address as (
    select
        patient as patient_id
        , line1 as address
        , city
        , state
        , postal_code as zip_code
        , latitude
        , longitude
    from {{ ref('stg_canvas_patient_address') }}
    where end is null
    qualify row_number() over (
        partition by patient
        order by start desc
    ) = 1
),

patient_contact as (
    select
        patient as patient_id
        , value as phone
    from {{ ref('stg_canvas_patient_contact_point') }}
    where system = 'phone'
    qualify row_number()
    over (
        partition by patient_id 
        order by rank desc
    ) = 1
),

mapped_data as (
    select
        pb.person_id
        , pb.patient_id
        , pb.first_name
        , pb.last_name
        , pb.sex
        , pb.race
        , pb.birth_date
        , pb.death_date
        , pb.death_flag
        , pb.social_security_number
        , pa.address
        , pa.city
        , pa.state
        , pa.zip_code
        , pc.phone
        , pa.latitude
        , pa.longitude
    from patient_base as pb
    left join patient_address as pa
        on pb.patient_id = pa.patient_id
    left join patient_contact as pc
        on pb.patient_id = pc.patient_id
)

select
      cast(person_id as {{ dbt.type_string() }} ) as person_id
    , cast(patient_id as {{ dbt.type_string() }} ) as patient_id
    , cast(first_name as {{ dbt.type_string() }} ) as first_name
    , cast(last_name as {{ dbt.type_string() }} ) as last_name
    , cast(sex as {{ dbt.type_string() }} ) as sex
    , cast(race as {{ dbt.type_string() }} ) as race
    , cast(birth_date as date) as birth_date
    , cast(death_date as date) as death_date
    , cast(death_flag as {{ dbt.type_int() }} ) as death_flag
    , cast(social_security_number as {{ dbt.type_string() }} ) 
        as social_security_number
    , cast(address as {{ dbt.type_string() }} ) as address
    , cast(city as {{ dbt.type_string() }} ) as city
    , cast(state as {{ dbt.type_string() }} ) as state
    , cast(zip_code as {{ dbt.type_string() }} ) as zip_code
    , cast(null as {{ dbt.type_string() }} ) as county
    , cast(latitude as {{ dbt.type_float() }} ) as latitude
    , cast(longitude as {{ dbt.type_float() }} ) as longitude
    , cast(phone as {{ dbt.type_string() }} ) as phone
    , cast('Canvas' as {{ dbt.type_string() }} ) as data_source
    , cast(null as {{ dbt.type_string() }} ) as file_name
    , cast(null as {{ dbt.type_timestamp() }} ) as ingest_datetime
from mapped_data
