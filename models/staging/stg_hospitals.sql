{{ config(materialized='view') }}
select trim(hospital_Id) as hospital_Id,
hospital_name,
city,
SPECIALTY
from {{source('raw','hospitals')}}