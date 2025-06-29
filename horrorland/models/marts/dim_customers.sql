{{ config(
    tags=['marts']
) }}

with customers as (
    select
        customer_id,
        age,
        gender,
        email,
        registration_date,
        is_vip_member,
        preferred_scare_level
    from {{ ref('stg_people__customers') }}
),

creating_categories as (   
    select
        customer_id,
        case
            when age < 18 then 'Under 18' 
            when age between 18 and 24 then '18-24' 
            when age between 25 and 34 then '25-34' 
            when age between 35 and 49 then '35-49'
            else '50+' 
        end as age_group,
        gender,
        email,
        registration_date,
        case 
            when is_vip_member then 'VIP Member' 
            else 'Regular Visitor'
        end as customer_category,
        preferred_scare_level
    from customers
)

select 
    *
from creating_categories