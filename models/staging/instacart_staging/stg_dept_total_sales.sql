select 
    department_id,
    department_total_sales as total_sales
from {{ source('Instacart', 'department_total_sales') }}