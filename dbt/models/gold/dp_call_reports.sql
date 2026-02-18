{{ config(
    materialized='table',
    meta={
        'owner': 'data_engineering',
        'version': '1.5.0',
        'sensitivity': 'contains_med'
    }
) }}

SELECT
    quarter,
    bank_name,
    bank_id,
    total_assets,
    total_deposits,
    net_income,
    non_performing_assets,
    tier1_capital_ratio,
    CASE
        WHEN total_assets > 0 THEN ROUND(non_performing_assets / total_assets * 100, 4)
        ELSE NULL
    END AS npa_ratio,
    CASE
        WHEN total_assets > 0 THEN ROUND(total_deposits / total_assets * 100, 2)
        ELSE NULL
    END AS deposit_to_asset_ratio
FROM {{ ref('stg_call_reports') }}
