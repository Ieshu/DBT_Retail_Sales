{{ config(
    materialized='table'
) }}

SELECT
    transaction_id,
    account_id,
    transaction_date,
    amount,
    transaction_type
FROM {{ ref('stg_transactions') }}
