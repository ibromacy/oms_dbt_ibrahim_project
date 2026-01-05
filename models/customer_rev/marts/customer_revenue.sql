{{ config(
    materialized='incremental',
    unique_key='customer_id'
) }}

select
    OS.customer_id,
    C.customer_name,
    sum(OS.order_count) as order_count,
    sum(OS.revenue)     as revenue,
    max(OS.updated_at)  as updated_at

from {{ ref('orders_fact') }} OS
join {{ ref('customers_stg') }} C
    on OS.customer_id = C.customer_id

{% if is_incremental() %}
where OS.updated_at > (
    select coalesce(max(updated_at), '1900-01-01')
    from {{ this }}
)
{% endif %}

group by
    OS.customer_id,
    C.customer_name