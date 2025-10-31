-- dbt_project/models/silver/silver_merchants.sql

{{ config(materialized='table') }}


WITH src AS (
  SELECT
    toUInt64OrNull(id)                                   AS merchant_id,
    anyLast(acquirer_id)                                 AS acquirer_id,
    anyLast(name)                                        AS merchant_name,
    toLowCardinality(anyLast(state))                     AS state,
    parseDateTimeBestEffortOrNull(anyLast(created_at))   AS created_at,
    parseDateTimeBestEffortOrNull(anyLast(updated_at))   AS updated_at,
    toDate32OrNull(max(_snapshot_date))                  AS snapshot_date
  FROM {{ source('bronze', 'merchants') }}
  GROUP BY toUInt64OrNull(id)
)
SELECT *
FROM src
WHERE merchant_id IS NOT NULL;
