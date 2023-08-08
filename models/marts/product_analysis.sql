
{{
    config (materialized = 'table')
}}

WITH products AS (
    SELECT * FROM {{ref ("stg_products")}}
),
orders AS (
    SELECT * FROM {{ref ("stg_orders")}}
),
department AS (
    SELECT * FROM {{ref ("stg_department")}}
),

pro_analysis AS (
    SELECT p.product_id, 
    p.product_name, 
    p.unit_price,
    count(order_id) as total_order,
    SUM((unit_price - unit_cost) * quantity) as profit,
    ROUND(AVG(quantity)) AS Avg_qty_sold
FROM products p
JOIN department d ON d.department_id = p.department_id
LEFT JOIN orders o on p.product_id = o.product_id
GROUP BY 1,2,3
),

filterd_data AS (
    SELECT product_name, 
    total_order, 
    profit,
    Avg_qty_sold,
    DENSE_RANK () OVER(ORDER BY profit DESC) AS profit_rank,
    DENSE_RANK () OVER(ORDER BY total_order DESC) AS rank_popular_prod,
    DENSE_RANK() OVER(ORDER BY unit_price DESC) AS highest_price_rank,
CASE 
    WHEN ntile(4) OVER(ORDER BY unit_price DESC) = 1 THEN 'High priced product'
    WHEN ntile(4) OVER(ORDER BY unit_price DESC) = 2 THEN 'Mid priced product'
    WHEN ntile(4) OVER(ORDER BY unit_price DESC) = 3 THEN 'Low priced product'
    WHEN ntile(4) OVER(ORDER BY unit_price DESC) = 4 THEN 'Lowest priced product'
END AS product_price_category
FROM pro_analysis)

SELECT * FROM filterd_data