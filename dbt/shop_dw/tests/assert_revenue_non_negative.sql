SELECT * FROM {{ ref('fact_sales') }} 
WHERE revenue < 0