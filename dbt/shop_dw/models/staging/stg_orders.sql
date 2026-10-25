SELECT order_id, customer_id, status, order_ts, updated_at, _ingested_at
FROM {{ source('cdc', 'orders') }}
WHERE NOT _deleted