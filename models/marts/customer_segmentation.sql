-- Customer Segmentation based on Total Spend and Order Frequency

{{
    config (materialized = 'table')
}}

WITH customers AS (
    SELECT * FROM {{ref ("stg_customers")}}
),
customer_spend AS (
    SELECT
        customer_id,
        SUM(order_total_amount) AS total_spend
    FROM {{ref ("fct_orders")}}
    GROUP BY customer_id
),
customer_orders AS (
    SELECT
        customer_id,
        COUNT(DISTINCT order_id) AS order_frequency
    FROM {{ref ("fct_orders")}}
    GROUP BY customer_id
),
customer_segmentation AS (
    SELECT
        c.customer_id,
        customer_name,
        cs.total_spend,
        co.order_frequency,
        CASE
            WHEN cs.total_spend >= 160000 AND co.order_frequency >= 1100 THEN 'High-Value Frequent'
            WHEN cs.total_spend >= 155000 AND co.order_frequency < 1100 THEN 'High-Value Occasional'
            WHEN cs.total_spend < 150000 AND co.order_frequency >= 10 THEN 'Low-Value Frequent'
            ELSE 'Low-Value Occasional'
        END AS customer_segment
    FROM customers c
    LEFT JOIN customer_spend cs ON c.customer_id = cs.customer_id
    LEFT JOIN customer_orders co ON c.customer_id = co.customer_id
)
SELECT customer_id, 
customer_segment 
FROM customer_segmentation
