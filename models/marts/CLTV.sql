--customer life time value

{{
    config (materialized = 'table')
}}

WITH products as (
    select * from {{ref ('stg_products')}} 
),
orders as (
    select * from {{ ref('fct_orders')}} 
),
cltv as (
    select customer_id,
            sum(orders.quantity * products.unit_price) as cltv
    from orders
    join products USING (product_id)
    group by 1
    order by cltv desc
)

select * from cltv

