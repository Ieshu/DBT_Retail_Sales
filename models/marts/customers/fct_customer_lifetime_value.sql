{{ config(
    materialized='table'
) }}

WITH customer_transactions AS (
    -- Ensure that customer_id is fetched by joining transactions with accounts
    SELECT
        c.customer_id,
        SUM(CASE
            WHEN t.transaction_type = 'Credit' THEN t.amount
            ELSE 0
        END) AS total_spent
    FROM {{ ref('stg_customers') }} c
    JOIN {{ ref('stg_accounts') }} a ON c.customer_id = a.customer_id
    JOIN {{ ref('stg_transactions') }} t ON a.account_id = t.account_id
    GROUP BY c.customer_id
)

SELECT
    customer_id,
    total_spent
FROM customer_transactions
