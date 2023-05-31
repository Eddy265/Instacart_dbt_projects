--Revenue generated by each of non-alcoholic drinks

WITH products as (
    select * from {{ref ('stg_products')}} 
),
orders as (
    select * from {{ ref('fct_orders')}} 
),
rev_orders as (
    select 
            products.product_id,
            products.product_name,
            sum(orders.quantity * products.unit_price) as Revenue
    from orders
    join products USING (product_id)
    WHERE products.product_name ILIKE '%non-alcoholic%' 
    OR products.product_name ILIKE '%non alcoholic%' 
    OR products.product_name ILIKE '%non alcohol%'
    OR products.product_name ILIKE '%no alcohol%'
    group by 1,2
    order by Revenue desc
)

select * from rev_orders