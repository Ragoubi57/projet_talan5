{{ config(materialized='view') }}

SELECT
    CAST(complaint_id AS INTEGER) AS complaint_id,
    CAST(date_received AS DATE) AS date_received,
    TRIM(product) AS product,
    TRIM(sub_product) AS sub_product,
    TRIM(issue) AS issue,
    TRIM(sub_issue) AS sub_issue,
    TRIM(company) AS company,
    UPPER(TRIM(state)) AS state,
    TRIM(zip_code) AS zip_code,
    TRIM(channel) AS channel,
    TRIM(company_response) AS company_response,
    TRIM(timely_response) AS timely_response,
    TRIM(consumer_disputed) AS consumer_disputed,
    consumer_narrative
FROM {{ source('raw', 'raw_complaints') }}
WHERE complaint_id IS NOT NULL
  AND date_received IS NOT NULL
  AND product IS NOT NULL
  AND company IS NOT NULL
