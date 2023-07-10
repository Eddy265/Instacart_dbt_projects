--Total sales by departments

WITH sales AS (
  SELECT department,
   total_sales
  FROM {{ref ('stg_dept_total_sales')}}
  JOIN {{ref('stg_department')}} using (department_id)
  ORDER BY total_sales desc
)

select * from sales