with source as (
    select
        feedback_id,
        ticket_id,
        rating,
        feedback_date,
        to_boolean(lower(would_recommend)) as would_recommend,
        to_boolean(lower(felt_scared)) as felt_scared,
        to_boolean(lower(worth_the_price)) as worth_the_price,
        created_at,
        updated_at
    from {{ source('PEOPLE', 'CUSTOMER_FEEDBACKS') }}
)

select *
from source