{{config(materialized = 'view')}}
select
trim(prescription_id) as prescription_id,
trim(encounter_id) as encounter_id,
trim(patient_id) as patient_id,
medication,
dose,
start_date,
end_date,
prescribed_by
from {{source('raw','prescriptions')}}