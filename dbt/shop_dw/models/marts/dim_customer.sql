SELECT customer_id, country
FROM {{ ref('stg_customers') }}