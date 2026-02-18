-- Test: all total_assets values should be positive
SELECT *
FROM {{ ref('dp_call_reports') }}
WHERE total_assets <= 0
