{{ config(
    materialized='table'
) }}

WITH base_customers AS (
    SELECT
        customer_id,
        name,
        email,
        address
    FROM {{ ref('stg_customers') }}
)

SELECT * FROM base_customers
