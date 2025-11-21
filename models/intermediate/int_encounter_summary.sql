{{ config(materialized='table') }}
 
with e as (
  select * from {{ ref('stg_encounters') }}
),
 
p as (select patient_id, first_name, last_name, date_of_birth, gender, insurance from {{ ref('stg_patients') }}),
 
h as (select hospital_id, hospital_name, city as hospital_city, specialty as hospital_specialty from {{ ref('stg_hospitals') }}),
 
diag_agg as (
  select encounter_id,
         count(distinct diagnosis_id) as number_of_diagnoses,
         max(diagnosis_date) as latest_diagnosis_date
  from {{ ref('stg_diagnoses') }}
  group by encounter_id
),
 
rx_agg as (
  select encounter_id,
         count(distinct prescription_id) as number_of_prescriptions,
         max(start_date) as latest_prescription_date
  from {{ ref('stg_prescriptions') }}
  group by encounter_id
)
 
select
  e.encounter_id,
  e.encounter_date,
  e.encounter_type,
  e.length_of_stay,
  e.patient_satisfaction,
  e.primary_provider_id,
 
  p.patient_id,
  p.first_name as patient_first_name,
  p.last_name as patient_last_name,
  p.date_of_birth as patient_dob,
  p.gender as patient_gender,
  p.insurance as patient_insurance,
 
  h.hospital_id,
  h.hospital_name,
  h.hospital_city,
  h.hospital_specialty,
 
  coalesce(d.number_of_diagnoses,0) as number_of_diagnoses,
  d.latest_diagnosis_date,
  coalesce(r.number_of_prescriptions,0) as number_of_prescriptions,
  r.latest_prescription_date,
 
  case when e.encounter_type='Inpatient' and e.length_of_stay is not null then e.length_of_stay else null end as inpatient_days
 
from e
left join p on e.patient_id = p.patient_id
left join h on e.hospital_id = h.hospital_id
left join diag_agg d on e.encounter_id = d.encounter_id
left join rx_agg r on e.encounter_id = r.encounter_id