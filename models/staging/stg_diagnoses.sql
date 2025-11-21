{{config(materialized = 'view')}}
select
trim(diagnosis_id) as diagnosis_id,
trim(encounter_id) as encounter_id,
trim(patient_id) as patient_id,
trim(diagnosis_code) as diagnosis_code,
description,
diagnosis_date as diagnosis_date
from {{source('raw','diagnoses')}}