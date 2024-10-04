{{ config(
    materialized='table'
) }}

WITH account_balances AS (
    SELECT
        account_id,
        AVG(balance) AS avg_balance,
        MAX(balance) AS max_balance,
        MIN(balance) AS min_balance
    FROM {{ ref('stg_accounts') }}
    GROUP BY account_id
)

SELECT * FROM account_balances
