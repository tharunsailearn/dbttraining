{{ config(materialized='view') }}
select trim(encounter_id ) as encounter_id,
trim(patient_id) as patient_id,
trim(primary_provider_id) as primary_provider_id,
trim(hospital_id) as hospital_id,
encounter_date as encounter_date,
encounter_type,
length_of_stay,
patient_satisfaction
from {{source('raw','encounters')}}