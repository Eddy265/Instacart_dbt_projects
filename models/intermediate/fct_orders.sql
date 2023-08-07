{{
    config (
        materialized = 'incremental',
        unique_key = 'order_id')
}}

WITH product AS (
    SELECT product_id, unit_price, unit_cost, department_id
    FROM {{ ref ("stg_products") }} 
),

orders AS (
    SELECT *
    FROM {{ ref('stg_orders') }}
    {% if is_incremental() %}
    WHERE order_id >= (SELECT max(order_id) FROM {{ this }})
    {% endif %}
),

FINAL AS (
    SELECT o.order_id, 
           o.customer_id,
           o.product_id,
           p.department_id,
           o.order_date,
           o.delivery_date,
           o.quantity,
           p.unit_price,
           p.unit_cost,
           order_status,
           updated_at,
           o.quantity * p.unit_price AS order_total_amount,
           (p.unit_price - p.unit_cost) * quantity AS profit,
            o.delivery_date - o.order_date AS days_to_deliver
    FROM product p 
    JOIN orders o ON p.product_id = o.product_id)

SELECT * FROM FINAL