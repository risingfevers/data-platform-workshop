{{ config(materialized='table') }}

SELECT
    product_id,
    COUNT(*) AS click_count,
    MAX(timestamp) AS last_click
FROM {{ source('raw', 'clicks') }}
WHERE timestamp >= UNIX_MILLIS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR))
GROUP BY product_id
