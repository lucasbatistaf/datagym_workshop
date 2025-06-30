SELECT
    f.is_vip_ticket,
    c.customer_status,
    h.fear_category,
    h.size_category,
    COUNT(f.ticket_id) as total_tickets,
    AVG(f.ticket_price) as avg_ticket_price,
    AVG(f.price_per_person) as avg_price_per_person,
    AVG(f.rating) as avg_rating,
    AVG(CASE WHEN f.would_recommend THEN 1.0 ELSE 0.0 END) as recommendation_rate,
    AVG(CASE WHEN f.worth_the_price THEN 1.0 ELSE 0.0 END) as worth_price_rate,
    AVG(CASE WHEN f.felt_scared THEN 1.0 ELSE 0.0 END) as scare_success_rate,
    SUM(f.ticket_price) as total_revenue,
    -- Análise de conversão: clientes regulares que compraram VIP
    AVG(CASE WHEN c.customer_status = 'Regular Visitor' AND f.is_vip_ticket THEN 1.0 ELSE 0.0 END) as regular_to_vip_rate
FROM {{ ref('fact_tickets') }} f
JOIN {{ ref('dim_customers') }} c ON f.customer_id = c.customer_id
JOIN {{ ref('dim_haunted_houses') }} h ON f.haunted_house_id = h.haunted_house_id
WHERE f.rating IS NOT NULL
GROUP BY f.is_vip_ticket, c.customer_status, h.fear_category, h.size_category
ORDER BY f.is_vip_ticket DESC, avg_rating DESC