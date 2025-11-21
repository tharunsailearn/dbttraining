{{config(materialized = 'view')}}
select trim(provider_id) as provider_id,
provider_name,
hospital_id
from {{source('raw','providers')}}