SELECT stock_code, description, unit_price, _ingested_at
FROM {{ source('cdc', 'products') }}
WHERE NOT _deleted