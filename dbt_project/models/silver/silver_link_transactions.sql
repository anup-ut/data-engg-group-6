-- dbt_project/models/silver/silver_link_transactions.sql

{{ config(
    materialized='incremental',
    unique_key='link_id',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
) }}

WITH src AS (
  SELECT
      id,
      state,
      linkpay_reference,
      payment_details,
      created_at,
      updated_at
  FROM {{ source('bronze', 'link_transactions') }}
),
norm AS (
  SELECT
      CAST(id AS UInt64)                                      AS link_id,
      /* Normalize created_at / updated_at to DateTime64(6) */
      IF(like(toTypeName(created_at), 'String%'),
         parseDateTime64BestEffortOrNull(replaceRegexpAll(created_at, ' UTC$', ''), 6),
         toDateTime64(created_at, 6)
      )                                                       AS created_at_dt,

      IF(like(toTypeName(updated_at), 'String%'),
         parseDateTime64BestEffortOrNull(replaceRegexpAll(updated_at, ' UTC$', ''), 6),
         toDateTime64(updated_at, 6)
      )                                                       AS updated_at_dt,

      state,
      linkpay_reference,
      payment_details
  FROM src
)

SELECT
    link_id,
    created_at_dt                                             AS created_at,
    updated_at_dt                                             AS completed_at,
    state,
    linkpay_reference                                         AS reference,

    /* JSON extraction (ClickHouse 23+): prefer JSON_VALUE; fallback to JSONExtractString if needed */
    JSON_VALUE(payment_details, '$.payment_reference')        AS payment_ref,
    JSON_VALUE(payment_details, '$.order_reference')          AS order_ref,

    CAST(state = 'completed' AS UInt8)                        AS is_completed,
    toDate(created_at_dt)                                     AS data_date
FROM norm

{% if is_incremental() %}
WHERE toDate(created_at_dt) = toDate('{{ var("ds_lagged") }}')
{% endif %}