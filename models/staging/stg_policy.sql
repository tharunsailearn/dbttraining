{{config(materialized = 'view')}}

with src  as (select * from {{ref ('stg_raw_policy')}})
 
select policy_id, policy_type, policy_holder,start_date,premium_amount,region,
case when policy_type = 'motor -3rd party' then 'motor'
when policy_type like 'life%' then 'life'
when policy_type like 'health%' then 'health'
else 'other'
end as policy_category
from src