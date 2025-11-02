{{ config(
  schema='gold',
  alias='dim_payment_state',
  materialized='table',
  engine='MergeTree()',
  order_by=['link_state_sk']
) }}


WITH st AS (
    SELECT toUInt8(1) AS link_state_sk, 'settled'            AS link_state_name, 1 AS is_final_state, 1 AS is_payment_reached_provider
    UNION ALL SELECT 2, 'initial',           0, 0
    UNION ALL SELECT 3, 'failed',            1, 1
    UNION ALL SELECT 4, 'voided',            1, 1
    UNION ALL SELECT 5, 'refunded',          1, 1
    UNION ALL SELECT 6, 'waiting_for_sca',   0, 0
)
SELECT * FROM st
