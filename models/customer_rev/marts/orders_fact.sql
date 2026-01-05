{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

select
    o.order_id,
    o.order_date,
    o.customer_id,
    o.store_id,
    o.status_desc,
    count(distinct o.order_id) as order_count,
    sum(oi.total_price) as revenue,
    o.updated_at
from {{ ref('orders_stg') }} o
join {{ ref('order_items_stg') }} oi
    on o.order_id = oi.order_id

{% if is_incremental() %}
  where o.updated_at > (select max(updated_at) from {{ this }})
{% endif %}

group by
    o.order_id,
    o.order_date,
    o.customer_id,
    o.store_id,
    o.status_desc,
    o.updated_at