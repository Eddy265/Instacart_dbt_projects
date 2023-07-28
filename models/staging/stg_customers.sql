select *
from {{ source('Instacart', 'customers') }}