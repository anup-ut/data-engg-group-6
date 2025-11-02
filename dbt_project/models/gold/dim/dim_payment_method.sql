{{ config(
  schema='gold',
  alias='dim_payment_method',
  materialized='table',
  engine='MergeTree()',
  order_by=['payment_method_sk']
) }}


WITH mapping AS (
    SELECT toUInt8(1) AS payment_method_sk, CAST('1' AS Nullable(String)) AS original_value, 'Visa'         AS payment_method_name
    UNION ALL
    SELECT toUInt8(2),                       CAST('2' AS Nullable(String)),                                   'Mastercard'
    UNION ALL
    SELECT toUInt8(3),                       CAST(NULL AS Nullable(String)),                                  'ACH transfer'
)
SELECT * FROM mapping




