--fact table for orders

WITH orders as (
    select order_id,
            customer_id,
            product_id,
            quantity,
            order_date
     from {{ref ('stg_orders')}}
)

select * from orders