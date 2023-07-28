--fact table for orders

WITH product AS (
    SELECT product_id, unit_price, unit_cost
    FROM {{ref ("stg_products")}} 
),

orders as (
    select order_id,
            customer_id,
            product_id,
            quantity,
            order_date
     from {{ref ('stg_orders')}}
),

FINAL AS (SELECT o.order_id, 
        o.customer_id,
        o.product_id,
        o.order_date,
        o.quantity,
        p.unit_price,
        p.unit_cost,
        o.quantity * p.unit_price AS order_total_amount,
        p.unit_price - p.unit_cost AS profit 
FROM product p 
JOIN orders o on p.product_id = o.product_id
--GROUP BY o.order_id, o.customer_id
)

select * from FINAL