
{{
    config(materialized='table')
}}

WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
),
orders AS (
    SELECT * FROM {{ ref('fct_orders') }}
),
customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),
product_purchase_count AS (
    SELECT
        product_id,
        COUNT(*) AS total_purchase_count
    FROM orders
    GROUP BY product_id
),
customer_most_preferred_product AS (
    SELECT
        o.customer_id,
        o.product_id AS most_preferred_product,
        pp.total_purchase_count,
        ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY pp.total_purchase_count DESC) AS rn
    FROM orders o
    JOIN product_purchase_count pp ON o.product_id = pp.product_id
),
churned_customers AS (
    SELECT
        lpd.customer_id,
        CASE WHEN (current_date - lpd.last_purchase_date) >= 90 THEN 'Churned' ELSE 'Active' END AS churn_status
    FROM (
        SELECT
            customer_id,
            MAX(order_date) AS last_purchase_date
        FROM orders
        GROUP BY customer_id
    ) lpd
)
SELECT
    cust.customer_id,
    cust.customer_name,
    cust.country,
    churned_customers.churn_status,
    p.product_name AS most_preferred_product,
    c.total_purchase_count,
    SUM(o.order_total_amount) AS total_spend,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(SUM(o.order_total_amount) / COUNT(DISTINCT o.order_id),2) AS average_spend,
    ROUND((CLTV.coefficient * SUM(o.order_total_amount) / COUNT(DISTINCT o.order_id)), 2) AS cltv,
    CASE
            WHEN SUM(o.order_total_amount) >= 160000 AND COUNT(DISTINCT o.order_id) >= 1100 THEN 'High-Value Frequent'
            WHEN SUM(o.order_total_amount) >= 155000 AND COUNT(DISTINCT o.order_id) < 1100 THEN 'High-Value Occasional'
            WHEN SUM(o.order_total_amount) < 150000 AND COUNT(DISTINCT o.order_id) >= 10 THEN 'Low-Value Frequent'
            ELSE 'Low-Value Occasional'
        END AS customer_segment
FROM customer_most_preferred_product c
JOIN customers cust ON c.customer_id = cust.customer_id
JOIN products p ON c.most_preferred_product = p.product_id
JOIN orders o ON cust.customer_id = o.customer_id
JOIN (SELECT customer_id, AVG(order_total_amount) AS coefficient FROM orders GROUP BY customer_id) CLTV ON cust.customer_id = CLTV.customer_id
LEFT JOIN churned_customers ON cust.customer_id = churned_customers.customer_id
WHERE c.rn = 1
GROUP BY cust.customer_id, cust.customer_name, churned_customers.churn_status, p.product_name, c.total_purchase_count, CLTV.coefficient, cust.country
ORDER BY cust.customer_id
