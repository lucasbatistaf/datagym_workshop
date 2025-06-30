{{ config(
    tags=['marts']
) }}

with feedbacks as (
    select
        ticket_id,
        rating,
        feedback_date,
        would_recommend,
        felt_scared,
        worth_the_price,
        updated_at,
    from {{ ref('stg_people__customer_feedbacks') }}       
    {% if is_incremental() %}
        -- new feedbacks
        where updated_at > (select max(feedback_updated_at) from {{ this }})
    {% endif %}
 
),

tickets as (
    select
        ticket_id,
        customer_id,
        haunted_house_id,
        visit_date,
        visit_hour,
        ticket_type,
        ticket_price,
        group_size,
        updated_at,
    from {{ ref('stg_attractions__haunted_house_tickets') }}
    {% if is_incremental() %}
        where
            -- new tickets
            updated_at > (select max(ticket_updated_at) from {{ this }})
            -- or old tickets with new feedbacks
            or ticket_id in (select ticket_id from feedbacks)
    {% endif %}
)

select
    tickets.ticket_id,
    tickets.customer_id,
    tickets.haunted_house_id,
    tickets.ticket_type,
    tickets.ticket_price,
    tickets.visit_date,
    case
        when extract(month from tickets.visit_date) = 10 then
            extract(day from tickets.visit_date) - 31
        else null
    end as days_to_halloween,
    case
        when tickets.visit_hour between 9 and 12 then 'Morning'
        when tickets.visit_hour between 13 and 17 then 'Afternoon'
        when tickets.visit_hour between 18 and 21 then 'Evening'
        else 'Night'
    end as visit_time_of_day,
    case
        when tickets.group_size > 4 then 'Large Group'
        when tickets.group_size > 2 then 'Medium Group'
        else 'Small Group'
    end as group_category,
    round(tickets.ticket_price / tickets.group_size) as price_per_person,
    feedbacks.rating,
    feedbacks.would_recommend,
    feedbacks.felt_scared,
    feedbacks.worth_the_price,
    tickets.updated_at as ticket_updated_at,
    feedbacks.updated_at as feedback_updated_at
from tickets
left join feedbacks on tickets.ticket_id = feedbacks.ticket_id