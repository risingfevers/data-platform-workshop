{{ config(materialized='table') }}

WITH orders_agg AS (
    SELECT
        product_id,
        SUM(quantity) AS total_orders,
        AVG(price) AS avg_price
    FROM {{ source('raw', 'orders') }}
    GROUP BY product_id
)

SELECT
    p.product_id,
    p.click_count,
    o.total_orders,
    o.avg_price,
    (p.click_count * 0.7 + o.total_orders * 0.3) AS score
FROM {{ ref('popular_products') }} p
LEFT JOIN orders_agg o ON p.product_id = o.product_id
ORDER BY score DESC
LIMIT 10
