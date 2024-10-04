{{ config(
    materialized='view'
) }}

WITH cleaned_transactions AS (
    SELECT
        t.transaction_id,
        t.account_id,
        t.transaction_date::timestamp AS transaction_date,
        t.amount::DECIMAL(12,2) AS amount,
        t.transaction_type,
        a.customer_id  -- Join with stg_accounts to get customer_id
    FROM {{ source('stock_warehouse', 'transactions') }} t
    LEFT JOIN {{ ref('stg_accounts') }} a ON t.account_id = a.account_id
)

SELECT * FROM cleaned_transactions
