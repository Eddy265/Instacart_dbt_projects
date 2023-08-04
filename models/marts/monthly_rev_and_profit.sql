
{{
    config (materialized = 'table')
}}

WITH orders AS (
    SELECT * FROM {{ref ("fct_orders")}}
),

monthly_rev_profit AS (
    SELECT
        to_char(order_date, 'YYYY-MM') AS Months,
        --date_trunc('month', order_date)::date as months,
        SUM(order_total_amount) AS Total_Revenue,
        SUM(profit) AS Total_profit
    FROM orders
    GROUP BY to_char(order_date, 'YYYY-MM')
)

SELECT * FROM monthly_rev_profit
