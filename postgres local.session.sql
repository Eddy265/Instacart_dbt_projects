
SELECT * FROM dev.orders_snapshot_timestamp where customer_id = 1001;

SELECT * FROM dev.orders_snapshot_timestamp;


select * from orders where customer_id= 1001

select * from dev.fct_orders where customer_id= 1001


select * from orders where order_status = 'Processing'


select min(order_id) from orders

INSERT INTO orders (
    order_id,
    customer_id,
    order_dow,
    order_hour_of_day,
    days_since_prior_order,
    product_id,
    quantity,
    order_date,
    order_status,
    updated_at
  )
VALUES (
    3424973,
    1001,
    6,
    2,
    8,
    22,
    2,
    CURRENT_DATE,
    'Processing',
    
    now()
  );

 UPDATE orders
 SET order_status = 'Shipped', delivery_date = current_date + interval '3 days', updated_at = NOW()
WHERE order_id = 5;

SELECT * FROM dev.fct_orders limit 5


DROP TABLE dev.orders_snapshot_timestamp

SELECT count(*) FROM orders where order_status = 'Delivered' and 
EXTRACT('Year'from order_date) = 2023

SELECT count(*) FROM orders 


SELECT count(*) FROM dev.fct_orders where order_status = 'Delivered' and 
EXTRACT('Year'from order_date) = 2023

SELECT count(*) FROM dev.fct_orders


SELECT count(*) FROM orders


SELECT MAX(order_id) from orders


SELECT MAX(order_id) from dev.fct_orders






select max(order_date) from orders

select max(order_date) from dev.fct_orders


select * from dev.product_analysis

select max(order_id) from orders

select max(order_id) from dev.fct_orders


--product analysis

WITH CTE AS (
    SELECT p.product_id, 
    p.product_name, 
    p.unit_price,
    count(order_id) as total_order,
    SUM((unit_price - unit_cost) * quantity) as profit,
    ROUND(AVG(quantity)) AS Avg_qty_sold
FROM products p
JOIN department d ON d.department_id = p.department_id
LEFT JOIN orders o on p.product_id = o.product_id
GROUP BY 1
)

select product_name, total_order, 
profit,
Avg_qty_sold,
unit_price,
DENSE_RANK() OVER(ORDER BY profit DESC) AS profit_rank,
DENSE_RANK() OVER(ORDER BY total_order DESC) AS choice_rank,
DENSE_RANK() OVER(ORDER BY unit_price DESC) AS highest_price_rank,
CASE 
    WHEN ntile(3) OVER(ORDER BY unit_price DESC) = 1 THEN 'High priced product'
    WHEN ntile(3) OVER(ORDER BY unit_price DESC) = 2 THEN 'Mid priced product'
    WHEN ntile(3) OVER(ORDER BY unit_price DESC) = 3 THEN 'Low priced product'
END AS product_category
from cte 






WITH CTE AS (
    SELECT 
        p.product_id, 
        p.product_name, 
        p.unit_price,
        count(o.order_id) as total_order,
        SUM((p.unit_price - p.unit_cost) * o.quantity) as profit,
        ROUND(AVG(o.quantity)) AS avg_qty_sold,
        EXTRACT(MONTH FROM o.order_date) AS sold_month,
        CASE 
            WHEN EXTRACT(MONTH FROM o.order_date) IN (12, 1, 2) THEN 'Winter'
            WHEN EXTRACT(MONTH FROM o.order_date) IN (3, 4, 5) THEN 'Spring'
            WHEN EXTRACT(MONTH FROM o.order_date) IN (6, 7, 8) THEN 'Summer'
            WHEN EXTRACT(MONTH FROM o.order_date) IN (9, 10, 11) THEN 'Fall'
        END AS sold_season
    FROM products p
    JOIN department d ON d.department_id = p.department_id
    LEFT JOIN orders o on p.product_id = o.product_id
    GROUP BY p.product_id, p.product_name, p.unit_price, sold_month, sold_season
)

SELECT 
    product_name, 
    total_order, 
    profit,
    avg_qty_sold,
    unit_price,
    DENSE_RANK() OVER(ORDER BY profit DESC) AS profit_rank,
    DENSE_RANK() OVER(ORDER BY total_order DESC) AS choice_rank,
    DENSE_RANK() OVER(ORDER BY unit_price DESC) AS highest_price_rank,
    CASE 
        WHEN ntile(3) OVER(ORDER BY unit_price DESC) = 1 THEN 'High priced product'
        WHEN ntile(3) OVER(ORDER BY unit_price DESC) = 2 THEN 'Mid priced product'
        WHEN ntile(3) OVER(ORDER BY unit_price DESC) = 3 THEN 'Low priced product'
    END AS product_category,
    sold_month,
    sold_season
FROM cte;




CREATE INDEX idx_customer_id ON customers (customer_id);



















select * from dev.product_analysis


select * from p_rank
WHERE rank_of_products_by_unit_price = 1;



update products set unit_price = 53 where product_id = 5904