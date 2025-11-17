{% snapshot policy_snapshot %}
{{
 
    config (unique_key = 'policy_id',
    strategy = 'check',
    check_cols= ['premium_amount','policy_type']
   
    )
}}
 
select * from {{ ref('stg_policy')}}
{% endsnapshot%}