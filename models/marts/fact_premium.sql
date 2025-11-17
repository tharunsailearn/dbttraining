{{config(materialized = 'table')}}

with policy as
(
    select * from {{ref('stg_policy')}}
),
cal as
(select policy_id, policy_type, policy_holder, start_date, premium_amount,
---Rule:Adjust premium for policies before 2023
case when start_date <'2023-01-01'
then {{ add_gst("premium_amount") }}
else premium_amount end as adjusted_premium,
--Fixed surcharge for motor policies
case when policy_type = 'motor-3rd party' then 500 else 0 end as surcharge,
--Final premium +GST 18%
((premium_amount+surcharge)*1.18) as premium_with_gst from policy
 
 
) select * from cal