

{{
    config(materialized='table')
}}

WITH status_orders AS (
    SELECT
    order_status,
    COUNT(*) AS count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (),2) AS percentage
FROM {{ref ("stg_orders")}}
GROUP BY order_status
ORDER BY order_status)

SELECT * FROM status_orders
