{{ config(
    materialized='view'
) }}

WITH cleaned_customers AS (
    SELECT
        customer_id,
        {{ title_case("name") }} AS name,  -- Correct use of title_case macro
        lower(email) AS email,
        phone,
        {{ safe_cast('address', 'TEXT') }} AS address  -- Correct use of safe_cast macro
    FROM {{ source('stock_warehouse','customers') }}
)

SELECT * FROM cleaned_customers
