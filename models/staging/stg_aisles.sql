
WITH source AS (

    SELECT * FROM {{ source('Instacart', 'aisles') }}
),

renamed as (
    SELECT 
    aisle_id,
    aisle
FROM source
)

SELECT * FROM renamed