with source as (
        select
            ticket_id,
            customer_id,
            haunted_house_id,
            purchase_date,
            visit_date,
            visit_hour,
            ticket_type,
            ticket_price,
            purchase_channel,
            payment_method,
            days_before_halloween,
            group_size,
            created_at,
            updated_at
        from {{ source('ATTRACTIONS', 'HAUNTED_HOUSE_TICKETS') }}
)

select *
from source