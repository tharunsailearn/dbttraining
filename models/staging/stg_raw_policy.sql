{{config(materialized = 'view')}}

select cast(policy_id as int) as policy_id,
lower(policy_type) as policy_type,
initcap(policy_holder) as policy_holder,
cast(start_date as date) as start_date,
cast(premium_amount as numeric) as premium_amount,
region
from {{ref('raw_policy_data')}}