{{ config(
  schema='gold',
  alias='dim_payment_state',
  materialized='table',
  engine='MergeTree()',
  order_by=['payment_state_sk'] 
) }}

WITH st AS (
    SELECT toUInt8(1) AS payment_state_sk, 'settled'          AS payment_state_name, 1 AS is_final_state, 1 AS is_payment_reached_provider
    UNION ALL SELECT 2, 'initial',         0, 0
    UNION ALL SELECT 3, 'failed',          1, 1
    UNION ALL SELECT 4, 'voided',          1, 1
    UNION ALL SELECT 5, 'refunded',        1, 1
    UNION ALL SELECT 6, 'waiting_for_sca', 0, 0
)
SELECT
  payment_state_sk::UInt8                 AS payment_state_sk,
  payment_state_name::String              AS payment_state_name,
  is_final_state::UInt8                   AS is_final_state,
  is_payment_reached_provider::UInt8      AS is_payment_reached_provider
FROM st