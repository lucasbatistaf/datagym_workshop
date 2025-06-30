SELECT
    h.fear_category,
    h.duration_category,
    f.visit_time_of_day,
    COUNT(f.ticket_id) as total_tickets,
    AVG(f.ticket_price) as avg_ticket_price,
    AVG(f.price_per_person) as avg_price_per_person,
    AVG(f.rating) as avg_rating,
    AVG(CASE WHEN f.felt_scared THEN 1.0 ELSE 0.0 END) as scare_success_rate,
    SUM(f.ticket_price) as total_revenue
FROM {{ ref('fact_tickets') }} f
JOIN {{ ref('dim_haunted_houses') }} h ON f.haunted_house_id = h.haunted_house_id
WHERE f.rating IS NOT NULL
GROUP BY h.fear_category, h.duration_category, f.visit_time_of_day
ORDER BY total_revenue DESC, avg_rating DESC