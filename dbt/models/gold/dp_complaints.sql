{{ config(
    materialized='table',
    meta={
        'owner': 'data_engineering',
        'version': '2.0.0',
        'sensitivity': 'contains_high'
    }
) }}

SELECT
    complaint_id,
    date_received,
    DATE_TRUNC('month', date_received) AS date_month,
    EXTRACT(YEAR FROM date_received) AS complaint_year,
    EXTRACT(QUARTER FROM date_received) AS complaint_quarter,
    product,
    sub_product,
    issue,
    sub_issue,
    company,
    state,
    channel,
    company_response,
    timely_response,
    consumer_disputed,
    consumer_narrative
FROM {{ ref('stg_complaints') }}
