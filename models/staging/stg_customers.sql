
SELECT customer_id,
        CONCAT(first_name, ' ', last_name) AS customer_name,
        email,
        address,
        phone_number
FROM {{ source('Instacart', 'customers') }}
