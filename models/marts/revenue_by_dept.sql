--Total sales by departments

WITH orders AS (
  SELECT * FROM {{ref ("stg_orders")}}
),

products AS (
  SELECT *
  FROM {{ref ("stg_products")}}
),

depts AS (
  SELECT * 
  FROM {{ref ("stg_department")}}
),

sales AS (
  SELECT department,
  sum(unit_price) AS total_revenue
  FROM orders o
  JOIN products p on o.product_id = p.product_id
  JOIN depts d on p.department_id = d.department_id
  GROUP BY 1 
  ORDER BY 2 desc
)

select * from sales