WITH source AS (
    SELECT 
    *
    FROM {{ source('Instacart', 'department') }}
),
renamed AS (
    SELECT 
    department_id,
    department
    FROM source
)

SELECT * FROM renamed