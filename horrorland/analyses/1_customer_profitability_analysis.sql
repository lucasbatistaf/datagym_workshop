SELECT
    c.age_group,
    c.gender,
    c.preferred_scare_level,
    COUNT(DISTINCT f.ticket_id) as total_visits,
    AVG(f.price_per_person) as avg_price_per_person,
    AVG(f.rating) as avg_rating,
    AVG(CASE WHEN f.would_recommend THEN 1.0 ELSE 0.0 END) as recommendation_rate,
    SUM(f.ticket_price) as total_revenue
FROM {{ ref('fact_tickets') }} f
JOIN {{ ref('dim_customers') }} c ON f.customer_id = c.customer_id
WHERE f.rating IS NOT NULL
GROUP BY c.age_group, c.gender, c.preferred_scare_level
HAVING COUNT(DISTINCT f.ticket_id) >= 10
ORDER BY avg_rating DESC