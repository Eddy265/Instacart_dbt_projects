# Data Transformation Pipeline Using Data Build Tool (dbt)
Project Description:

The Instacart DBT project is a comprehensive data transformation pipeline that focuses on transforming raw data into actionable insights. The project consists of four main models designed to calculate essential metrics and generate valuable outputs.

The first model is the Customer Lifetime Value (CLV) model, which leverages customer order history and behavior to estimate the long-term value of each customer. This model provides insights into customer loyalty, purchasing patterns, and helps identify high-value customers for targeted marketing strategies.

The second model focuses on Total Sales by Department, providing a comprehensive overview of the sales performance for each department. It aggregates and analyzes data on product sales, quantities, and prices to generate valuable insights into departmental performance and identify areas for potential growth and optimization.

The third model, Profit by Department, delves deeper into financial metrics by calculating the profit generated by each department. By considering factors such as product costs, unit prices, and sales quantities, this model provides key profitability insights that help optimize pricing, cost management, and resource allocation.

The fourth model concentrates on Revenue from Non-Alcoholic Drinks, specifically analyzing sales and revenue generated from this particular product category. By isolating non-alcoholic drinks, valuable insights can be gained regarding consumer preferences, market trends, and potential growth opportunities in this specific segment.

All outputs generated by these models are materialized as tables within the prod schema, providing structured and optimized data for downstream analytics and reporting. Additionally, the intermediate model and staging models are materialized as views within the dev schema of the Instacart database, facilitating iterative development and testing of the data transformation pipeline.

This project combines powerful data transformation techniques with advanced analytics to deliver valuable insights and enable data-driven decision-making within the organization.
