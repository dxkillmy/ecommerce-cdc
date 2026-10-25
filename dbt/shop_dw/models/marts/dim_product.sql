SELECT stock_code, description, unit_price as current_unit_price
FROM {{ ref('stg_products') }}