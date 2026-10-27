select
    i.order_id || '-' || i.line_no           as sales_key,
    i.order_id,
    i.line_no,
    o.customer_id,
    i.stock_code,
    to_char(o.order_ts, 'YYYYMMDD')::int     as date_key,
    o.order_ts,
    o.status,
    i.quantity,
    i.unit_price,
    i.quantity * i.unit_price                as revenue,
    i._ingested_at
from {{ ref('stg_order_items') }} i
join {{ ref('stg_orders') }} o using (order_id)