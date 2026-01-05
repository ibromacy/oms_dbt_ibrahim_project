{{ config(
    materialized='incremental',
    unique_key='order_item_id'
) }}

select
    orderitemid   as order_item_id,
    orderid       as order_id,
    productid     as product_id,
    quantity,
    unitprice     as unit_price,

    -- row-level calculation (SAFE in staging)
    quantity * unitprice as total_price,

    updated_at

from {{ source('raw', 'orderitems') }}

{% if is_incremental() %}
  where updated_at > (select max(updated_at) from {{ this }})
{% endif %}