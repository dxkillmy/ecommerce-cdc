with bounds as (
    select min(order_ts)::date as start_date,
           max(order_ts)::date as end_date
    from {{ ref('stg_orders') }}
)
select
    to_char(d, 'YYYYMMDD')::int  as date_key,
    d::date                      as full_date,
    extract(year  from d)::int   as year,
    extract(month from d)::int   as month,
    extract(day   from d)::int   as day,
    extract(isodow from d)::int  as day_of_week,
    extract(isodow from d) in (6, 7) as is_weekend
from bounds,
     generate_series(start_date, end_date, interval '1 day') as d