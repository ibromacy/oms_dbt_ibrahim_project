{{
    config( materialized='incremental',
            unique_key='Order_id')
}}

SELECT
    OrderID    as Order_id,
    OrderDate  as Order_date,
    CustomerID as Customer_id,
    EmployeeID as Employee_id,
    StoreID    as Store_id,
    Status     as StatusCD,
    CASE
        WHEN Status = '01' THEN 'In Progress'
        WHEN Status = '02' THEN 'Completed'
        WHEN Status = '03' THEN 'Cancelled'
        ELSE NULL
    END AS Status_Desc,
    CASE
        WHEN StoreID = 1000 THEN 'Online'
        ELSE 'In-store'
    END AS ORDER_CHANNEL,
    Updated_at,
    current_timestamp as dbt_updated_at
FROM
    {{ source('raw', 'orders') }}

{% if is_incremental() %}
where  Updated_at >= (select max(dbt_updated_at) from {{ this }})
{% endif %}

