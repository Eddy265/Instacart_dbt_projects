
{{
    config (materialized = 'table')
}}


WITH monthly_rev_profit AS (
    SELECT
        date_trunc('month', order_date)::date as months,
        SUM(order_total_amount) AS Total_Revenue,
        SUM(profit) AS Total_profit
    FROM {{ref ("fct_orders")}}
    GROUP BY date_trunc('month', order_date)::date
)

SELECT * FROM monthly_rev_profit
