select 
    department_id,
    department
from {{ source('Instacart', 'department') }}