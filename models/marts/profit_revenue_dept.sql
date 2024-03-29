--profit generated by departments
{{
    config (materialized = 'table')
}}

WITH pro AS (
  SELECT *
  FROM {{ref ('stg_products')}}
),
departments AS (
  SELECT *
  FROM {{ref ('stg_department')}}
),
orders as (
    select * from {{ref ('fct_orders')}}
),
aisles as (
  select * from {{ref ('stg_aisles')}}
),

revenue AS (
    SELECT  
      d.department_id,
	    d.department,
      SUM(profit) AS profit,
      SUM(order_total_amount) AS total_revenue
FROM pro 
JOIN departments d ON pro.department_id = d.department_id
JOIN aisles ON pro.aisle_id = aisles.aisle_id
JOIN orders o on pro.product_id = o.product_id
GROUP BY 1,2
)

SELECT * FROM revenue

