{{ config(materialized='table') }}
 
with patients as (
  select * from {{ ref('stg_patients') }}
),
 
enc_agg as (
  select patient_id,
         count(distinct encounter_id) as total_encounters,
         min(encounter_date) as first_encounter_date,
         max(encounter_date) as last_encounter_date
  from {{ ref('stg_encounters') }}
  group by patient_id
),
 
latest_diag as (
  select patient_id,
         diagnosis_code as latest_diagnosis_code,
         description as latest_diagnosis_description,
         diagnosis_date as latest_diagnosis_date
  from {{ ref('stg_diagnoses') }}
  qualify row_number() over (partition by patient_id order by diagnosis_date desc, diagnosis_id desc) = 1
),
 
latest_rx as (
  select patient_id,
         medication as latest_medication,
         start_date as latest_medication_start,
         end_date as latest_medication_end,
         prescribed_by as latest_prescribed_by
  from {{ ref('stg_prescriptions') }}
  qualify row_number() over (partition by patient_id order by start_date desc, prescription_id desc) = 1
)
 
select
  p.patient_id,
  p.first_name,
  p.last_name,
  p.date_of_birth,
  p.gender,
  p.insurance,
 
  coalesce(ea.total_encounters,0) as total_encounters,
  ea.first_encounter_date,
  ea.last_encounter_date,
 
  ld.latest_diagnosis_code,
  ld.latest_diagnosis_description,
  ld.latest_diagnosis_date,
 
  lr.latest_medication,
  lr.latest_medication_start,
  lr.latest_medication_end,
  lr.latest_prescribed_by
 
from patients p
left join enc_agg ea on p.patient_id = ea.patient_id
left join latest_diag ld on p.patient_id = ld.patient_id
left join latest_rx lr on p.patient_id = lr.patient_id
 