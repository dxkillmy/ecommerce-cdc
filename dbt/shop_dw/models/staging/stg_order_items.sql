SELECT order_id, line_no, stock_code, quantity, unit_price, _ingested_at
FROM {{ source('cdc', 'order_items') }}
WHERE NOT _deleted