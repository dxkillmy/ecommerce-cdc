SELECT customer_id, country, _ingested_at
FROM {{ source('cdc', 'customers') }}
WHERE NOT _deleted