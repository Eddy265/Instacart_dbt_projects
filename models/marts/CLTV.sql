--customer life time value

{{
    config (materialized = 'table')
}}

WITH products AS (
    SELECT * FROM {{ref ('stg_products')}} 
),
orders AS (
    SELECT * FROM {{ ref('fct_orders')}} 
),

customers AS (
    SELECT *
    FROM {{ref ("stg_customers")}}
),

cltv AS (
    SELECT CONCAT(first_name, ' ', last_name) AS customer_name,
            COUNT(order_id) AS number_of_orders,
            sum(order_total_amount) AS cltv
    FROM orders o
    JOIN products USING (product_id)
    JOIN customers c on o.customer_id = c.customer_id
    GROUP BY 1
    ORDER BY cltv desc
)

SELECT * FROM cltv

