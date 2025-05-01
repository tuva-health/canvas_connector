{% docs accession_number %}
The lab order number from the source system.
{% enddocs %}

{% docs address %}
The street address of the record (e.g., facility location, patient, etc).
{% enddocs %}

{% docs admit_source_code %}
Indicates where the patient was before the healthcare encounter (inpatient claims only).
{% enddocs %}

{% docs admit_source_description %}
Description of the admit_source_code for the encounter.
{% enddocs %}

{% docs admit_type_code %}
Indicates the type of admission (inpatient claims only).
{% enddocs %}

{% docs admit_type_description %}
Description of the admit_type_code for the encounter.
{% enddocs %}

{% docs allowed_amount %}
The total amount allowed (includes amount paid by the insurer and patient).
{% enddocs %}

{% docs atc_code %}
ATC code for the medication.
{% enddocs %}

{% docs atc_description %}
Description for the ATC code.
{% enddocs %}

{% docs attending_provider_id %}
ID for the attending provider on the encounter.
{% enddocs %}

{% docs attending_provider_name %}
Name of the attending provider on the encounter.
{% enddocs %}

{% docs birth_date %}
The birth date of the patient.
{% enddocs %}

{% docs charge_amount %}
The total amount charged for the services provided, before any adjustments or payments. This is typically in US dollars.
{% enddocs %}

{% docs city %}
The city of the record (e.g., facility location, patient, etc).
{% enddocs %}

{% docs claim_id %}
Unique identifier for a claim. Each claim represents a distinct healthcare service or set of services provided to a patient.
{% enddocs %}

{% docs collection_date %}
Date the test was collected.
{% enddocs %}

{% docs condition_id %}
Unique identifier for each condition in the table.
{% enddocs %}

{% docs condition_rank %}
The numerical ranking of a diagnosis code in a claim.  Principle  diagnosis code is given a ranking of 1. Claims data typically has a  strict ranking of conditions whereas medical records will often not  have this information or the information won't be accurate.
{% enddocs %}

{% docs condition_type %}
The type of condition i.e. problem, admitting, or billing.
{% enddocs %}

{% docs county %}
The county for the patient.
{% enddocs %}

{% docs data_source %}
User-configured field that indicates the data source.
{% enddocs %}

{% docs days_supply %}
The number of days supply included.
{% enddocs %}

{% docs death_date %}
Date the patient died if there is one.
{% enddocs %}

{% docs death_flag %}
A flag indicating if the patient has died.
{% enddocs %}

{% docs discharge_date %}
Discharge date for the claim (inpatient claims only).
{% enddocs %}

{% docs discharge_disposition_code %}
Indicates the type of setting the patient was discharged to (institutional inpatient claims only).
{% enddocs %}

{% docs discharge_disposition_description %}
Description of the discharge_disposition_code for the encounter.
{% enddocs %}

{% docs dispensing_date %}
Date the medication was dispensed.
{% enddocs %}

{% docs dme_flag %}
Indicates whether durable medical equipment (DME) was used during the encounter (1 for yes, 0 for no).
{% enddocs %}

{% docs drg_code_type %}
The DRG system used for the claim.
{% enddocs %}

{% docs drg_code %}
The DRG code on the claim.
{% enddocs %}

{% docs drg_description %}
The description for the DRG code used on the claim.
{% enddocs %}

{% docs encounter_end_date %}
Date when the encounter ended.
{% enddocs %}

{% docs encounter_id %}
Unique identifier for each encounter in the dataset.
{% enddocs %}

{% docs encounter_start_date %}
Date when the encounter started.
{% enddocs %}

{% docs encounter_type %}
Indicates the type of encounter e.g. acute inpatient, emergency department, etc.
{% enddocs %}

{% docs facility_id %}
Facility ID for the claim (typically represents the facility where services were performed).
{% enddocs %}

{% docs facility_name %}
Facility name.
{% enddocs %}

{% docs facility_npi %}
Facility NPI for the claim (typically represents the facility where services were performed).
{% enddocs %}

{% docs file_name %}
The file name of the source file.
{% enddocs %}

{% docs facility_type %}
The type of facility e.g. acute care hospital.
{% enddocs %}

{% docs first_name %}
The first name of the patient.
{% enddocs %}

{% docs gender %}
The gender of the patient.
{% enddocs %}

{% docs hcpcs_code %}
The CPT or HCPCS code representing the procedure or service provided. These codes are used to describe medical, surgical, and diagnostic services.
{% enddocs %}

{% docs hcpcs_modifier_1 %}
1st modifier for HCPCS code.
{% enddocs %}

{% docs hcpcs_modifier_2 %}
2nd modifier for HCPCS code.
{% enddocs %}

{% docs hcpcs_modifier_3 %}
3rd modifier for HCPCS code.
{% enddocs %}

{% docs hcpcs_modifier_4 %}
4th modifier for HCPCS code.
{% enddocs %}

{% docs hcpcs_modifier_5 %}
5th modifier for HCPCS code.
{% enddocs %}

{% docs ingest_datetime %}
The date and time the source file was ingested into the data warehouse or landed in cloud storage.
{% enddocs %}

{% docs lab_result_id %}
Unique identifier for each lab result.
{% enddocs %}

{% docs last_name %}
The last name of the patient.
{% enddocs %}

{% docs latitude %}
The latitude of the record (e.g., facility location, patient, etc).
{% enddocs %}

{% docs length_of_stay %}
Length of the encounter calculated as encounter_end_date - encounter_start_date.
{% enddocs %}

{% docs location_id %}
Unique identifier for each location.
{% enddocs %}

{% docs longitude %}
The longitude of the record (e.g., facility location, patient, etc).
{% enddocs %}

{% docs medication_id %}
Unique identifier for each medication in the table.
{% enddocs %}

{% docs modifier_1 %}
First modifier for the procedure code.
{% enddocs %}

{% docs modifier_2 %}
Second modifier for the procedure code.
{% enddocs %}

{% docs modifier_3 %}
Third modifier for the procedure code.
{% enddocs %}

{% docs modifier_4 %}
Fourth modifier for the procedure code.
{% enddocs %}

{% docs modifier_5 %}
Fifth modifier for the procedure code.
{% enddocs %}

{% docs name %}
The name of the location.
{% enddocs %}

{% docs ndc_code %}
National drug code associated with the medication.
{% enddocs %}

{% docs ndc_description %}
Description for the NDC.
{% enddocs %}

{% docs normalized_abnormal_flag %}
Normalized abnormal flag.
{% enddocs %}

{% docs normalized_code %}
The normalized code.
{% enddocs %}

{% docs normalized_code_type %}
The normalized type of code.
{% enddocs %}

{% docs normalized_component %}
The normalized component.
{% enddocs %}

{% docs normalized_description %}
Normalized description of the code.
{% enddocs %}

{% docs normalized_reference_range_high %}
The normalized high end of the reference range.
{% enddocs %}

{% docs normalized_reference_range_low %}
The normalized low end of the reference range.
{% enddocs %}

{% docs normalized_units %}
Normalized units of the lab test.
{% enddocs %}

{% docs npi %}
The national provider identifier associated with the record e.g. facility_npi, provider_npi
{% enddocs %}

{% docs observation_date %}
Date the observation was recorded.
{% enddocs %}

{% docs observation_id %}
Unique identifier for each observation in the dataset.
{% enddocs %}

{% docs observation_type %}
Type of observation.
{% enddocs %}

{% docs onset_date %}
Date when the condition first occurred.
{% enddocs %}

{% docs ordering_practitioner_id %}
Unique identifier for the practitioner who ordered the lab test.
{% enddocs %}

{% docs paid_amount %}
The total amount paid by the insurer.
{% enddocs %}

{% docs panel_id %}
Unique identifier for the panel.
{% enddocs %}

{% docs parent_organization %}
The parent organization associated with the facility.
{% enddocs %}

{% docs patient_id %}
Identifier that links a patient to a particular clinical source system.
{% enddocs %}

{% docs payer %}
Name of the payer (i.e. health insurer) providing coverage.
{% enddocs %}

{% docs person_id %}
Unique identifier for each person in the dataset.
{% enddocs %}

{% docs phone %}
The phone number for the patient.
{% enddocs %}

{% docs place_of_service_code %}
Place of service for the claim (professional claims only).
{% enddocs %}

{% docs place_of_service_description %}
Place of service description.
{% enddocs %}

{% docs plan %}
Name of the plan (i.e. sub contract) providing coverage.
{% enddocs %}

{% docs practice_affiliation %}
Practice affiliation of the provider.
{% enddocs %}

{% docs practitioner_id %}
Unique identifier for the practitioner on record (e.g., ordered medication, performed the procedure, etc).
{% enddocs %}

{% docs prescribing_date %}
Date the medication was prescribed.
{% enddocs %}

{% docs prescribing_provider_id %}
ID for the provider that wrote the prescription (e.g. priamry care physician).
{% enddocs %}

{% docs prescribing_provider_name %}
Prescribing provider name.
{% enddocs %}

{% docs present_on_admit_code %}
The present_on_admit_code related to the condition.
{% enddocs %}

{% docs present_on_admit_description %}
The description of the present_on_admit_code for the condition.
{% enddocs %}

{% docs primary_diagnosis_code %}
Primary diagnosis code for the encounter. If from claims the primary diagnosis code comes from the institutional claim.
{% enddocs %}

{% docs primary_diagnosis_code_type %}
The type of condition code reported in the source system e.g. ICD-10-CM.
{% enddocs %}

{% docs primary_diagnosis_description %}
Description of the primary diagnosis code.
{% enddocs %}

{% docs primary_specialty_description %}
Description of the primary medical specialty of the provider.
{% enddocs %}

{% docs primary_taxonomy_code %}
The primary taxonomy code identifying the provider's specialty, classification, or area of practice.
{% enddocs %}

{% docs procedure_date %}
Date when the procedure was performed.
{% enddocs %}

{% docs procedure_id %}
The unique identifier for the performed procedure.
{% enddocs %}

{% docs provider_first_name %}
The first name of the healthcare provider.
{% enddocs %}

{% docs provider_last_name %}
The last name of the healthcare provider.
{% enddocs %}

{% docs provider_name %}
The name of the healthcare provider.
{% enddocs %}

{% docs provider_specialty %}
The medical specialty of the provider.
{% enddocs %}

{% docs quantity %}
The quantity of the medication.
{% enddocs %}

{% docs quantity_unit %}
The units for the quantity.
{% enddocs %}

{% docs race %}
The patient's race.
{% enddocs %}

{% docs recorded_date %}
Date when the condition was recorded.
{% enddocs %}

{% docs refills %}
Number of refills for the prescription.
{% enddocs %}

{% docs resolved_date %}
Date when the condition was resolved.
{% enddocs %}

{% docs result %}
The result of the record (e.g., lab test, observation, etc).
{% enddocs %}

{% docs result_date %}
Date of the test result.
{% enddocs %}

{% docs route %}
The route used to administer the medication.
{% enddocs %}

{% docs rxnorm_code %}
RxNorm code associated with the medication.
{% enddocs %}

{% docs rxnorm_description %}
Description for the RxNorm code.
{% enddocs %}

{% docs rxnorm_mapping_method %}
mapping method used to populate the normalized codes and descriptions.  Can be manual (fields were populated in input layer), automatic (dictionary codes matching the source code were found and was automatically populated) or custom (populated by normalization engine)
{% enddocs %}

{% docs sex %}
The gender of the patient.
{% enddocs %}

{% docs social_security_number %}
The social security number of the patient.
{% enddocs %}

{% docs source_abnormal_flag %}
Indicates whether the result is abnormal or normal.
{% enddocs %}

{% docs source_code %}
The code in the source system (e.g., the ICD-10 code, NDC, lab, etc)
{% enddocs %}

{% docs source_code_type %}
The type of code reported in the source system (e.g., ICD-10 code, NDC, lab, etc)
{% enddocs %}

{% docs source_component %}
The description for the component of the test from the source system.
{% enddocs %}

{% docs source_description %}
Description of the source code in the source system.
{% enddocs %}

{% docs source_reference_range_high %}
The high end of the reference range from the source system.
{% enddocs %}

{% docs source_reference_range_low %}
The low end of the reference range from the source system.
{% enddocs %}

{% docs source_units %}
Source units of the lab test.
{% enddocs %}

{% docs specialty %}
Specialty of the provider.
{% enddocs %}

{% docs specimen %}
The type of specimen e.g. blood, plasma, urine.
{% enddocs %}

{% docs state %}
The state of the record (e.g., facility location, patient, etc).
{% enddocs %}

{% docs status %}
Status of the record (e.g., condition, test, etc).
{% enddocs %}

{% docs strength %}
The strength of the medication.
{% enddocs %}

{% docs sub_specialty %}
Sub specialty of the provider.
{% enddocs %}

{% docs tuva_last_run %}
The last time the data was refreshed. Generated by `dbt_utils.pretty_time` as the local time of the `dbt run` environment. Timezone is configurable via the `tuva_last_run` var.
{% enddocs %}

{% docs year_month %}
Unique year-month of in the dataset computed from eligibility.
{% enddocs %}

{% docs zip_code %}
The zip code of the record (e.g., facility location, patient, etc).
{% enddocs %}