{{ config(
    materialized='table'
) }}

SELECT
    account_id,
    customer_id,
    account_type,
    balance
FROM {{ ref('stg_accounts') }}
