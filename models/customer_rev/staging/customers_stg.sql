{{ config(materialized='table') }}   
SELECT
    CustomerID as customer_id,
    FirstName,
    LastName,
    Email,
    Phone,
    Address,
    City,
    State,
    ZipCode,
    Updated_at,
    CONCAT(FirstName, ' ', LastName) AS Customer_Name
FROM
    {{ source('raw', 'customers') }}