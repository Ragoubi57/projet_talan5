{{ config(materialized='view') }}

SELECT
    TRIM(quarter) AS quarter,
    TRIM(bank_name) AS bank_name,
    CAST(bank_id AS INTEGER) AS bank_id,
    CAST(total_assets AS DOUBLE) AS total_assets,
    CAST(total_deposits AS DOUBLE) AS total_deposits,
    CAST(net_income AS DOUBLE) AS net_income,
    CAST(non_performing_assets AS DOUBLE) AS non_performing_assets,
    CAST(tier1_capital_ratio AS DOUBLE) AS tier1_capital_ratio
FROM {{ source('raw', 'raw_call_reports') }}
WHERE quarter IS NOT NULL
  AND bank_name IS NOT NULL
  AND bank_id IS NOT NULL
  AND total_assets IS NOT NULL
  AND total_assets > 0
