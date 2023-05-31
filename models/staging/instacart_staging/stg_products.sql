select product_id,
        product_name,
        unit_price,
        department_id,
        aisle_id,
        unit_cost
from {{ source('Instacart', 'products') }}