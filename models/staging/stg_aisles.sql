select 
    aisle_id,
    aisle
from {{ source('Instacart', 'aisles') }}