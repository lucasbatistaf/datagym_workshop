SELECT
    CASE
        WHEN f.days_to_halloween BETWEEN -6 AND 0 THEN 'Semana do Halloween (25-31 Out)'
        WHEN f.days_to_halloween BETWEEN -5 AND -1 THEN 'Véspera do Halloween (26-30 Out)'
        WHEN f.days_to_halloween = 0 THEN 'Dia do Halloween (31 Out)'
        WHEN f.visit_date >= '2024-11-01' THEN 'Pós-Halloween (01-10 Nov)'
        ELSE 'Após Halloween (11-30 Nov)'
    END as halloween_period,
    f.group_category,
    f.visit_time_of_day,
    COUNT(f.ticket_id) as total_tickets,
    AVG(f.ticket_price) as avg_ticket_price,
    AVG(f.price_per_person) as avg_price_per_person,
    AVG(f.rating) as avg_rating,
    AVG(CASE WHEN f.worth_the_price THEN 1.0 ELSE 0.0 END) as worth_price_rate,
    AVG(CASE WHEN f.felt_scared THEN 1.0 ELSE 0.0 END) as scare_success_rate,
    SUM(f.ticket_price) as total_revenue,
    -- Comparar demanda por período
    ROUND(COUNT(f.ticket_id) * 100.0 / SUM(COUNT(f.ticket_id)) OVER(), 2) as pct_total_demand
FROM {{ ref('fact_tickets') }} f
WHERE f.visit_date BETWEEN '2024-10-25' AND '2024-11-10'
    AND f.rating IS NOT NULL
GROUP BY
    halloween_period,
    f.group_category,
    f.visit_time_of_day
ORDER BY
    total_revenue DESC