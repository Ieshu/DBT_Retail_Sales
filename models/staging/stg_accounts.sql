{{ config(
    materialized='view'
) }}

WITH cleaned_accounts AS (
    SELECT
        account_id,
        customer_id,
        account_type,
        balance::DECIMAL(12,2) AS balance
    FROM {{ source('stock_warehouse','accounts') }}
)

SELECT * FROM cleaned_accounts
