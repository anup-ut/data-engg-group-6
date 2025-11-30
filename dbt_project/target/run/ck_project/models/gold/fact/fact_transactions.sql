
        insert into gold.fact_transactions
        ("sk", "transaction_id", "payment_reference", "hasPayment", "payment_state", "merchant_sk", "payment_method_sk", "TimeFromStartToEndSeconds")

WITH
-- 0) Raw transactions
tx_raw AS (
  SELECT
      assumeNotNull(transaction_id)   AS transaction_id,
      reference                       AS reference_col,
      details                         AS details_json,      -- JSON string
      created_at                      AS tx_started_at      -- already proper type
  FROM silver.link_transactions
),

-- 1) Extract payment reference (JSON → column; NO regex, NO try)
tx_norm AS (
  SELECT
      transaction_id,
      tx_started_at,
      coalesce(
        nullIf(JSONExtractString(details_json, 'payment_reference'), ''),
        nullIf(JSONExtractString(details_json, 'paymentReference'), ''),
        nullIf(JSONExtractString(details_json, 'reference'), ''),
        nullIf(reference_col, '')
      ) AS payment_reference
  FROM tx_raw
),

-- 2) Final payment attributes per reference
pay_final AS (
  SELECT
      reference                                                  AS pay_reference,
      argMax(payment_state, coalesce(updated_at, created_at))    AS final_state,
      argMax(card_type,      coalesce(updated_at, created_at))   AS final_method,      -- '1' | '2' | NULL (LC String)
      argMax(merchant_id,    coalesce(updated_at, created_at))   AS final_merchant_id,
      max(coalesce(updated_at, created_at))                      AS final_state_ts
  FROM silver.payments
  GROUP BY reference
),

-- 3) Dimension lookups
d_state AS (
  SELECT lower(payment_state_name) AS state_name_l, payment_state_sk AS state_sk
  FROM gold.dim_payment_state
),

-- IMPORTANT: join on original_value (string '1','2',NULL), not on display name
d_method AS (
  SELECT original_value, payment_method_sk
  FROM gold.dim_payment_method
),

d_merchant AS (
  SELECT merchant_id, merchant_sk
  FROM gold.dim_merchants
),

-- 4) Join and shape
j AS (
  SELECT
      cityHash64(t.transaction_id, coalesce(t.payment_reference, ''))  AS sk,

      t.transaction_id,
      t.payment_reference,

      toUInt8(pf.pay_reference IS NOT NULL)                            AS hasPayment,

      ds.state_sk                                                      AS payment_state_sk,
      dm.merchant_sk                                                   AS merchant_sk,

      -- keep NULLs as NULL (not 0) when no match; ClickHouse may default to 0 otherwise
      CAST(dpm.payment_method_sk AS Nullable(UInt8))                   AS payment_method_sk,

      if(pf.final_state_ts IS NULL OR t.tx_started_at IS NULL,
         NULL,
         dateDiff('second', t.tx_started_at, pf.final_state_ts)
      )                                                                AS TimeFromStartToEndSeconds,

      t.tx_started_at                                                  AS _gating_ts
  FROM tx_norm t
  LEFT JOIN pay_final   pf   ON pf.pay_reference        = t.payment_reference
  LEFT JOIN d_state     ds   ON ds.state_name_l         = lowerUTF8(coalesce(pf.final_state, ''))
  LEFT JOIN d_method    dpm  ON dpm.original_value      = pf.final_method     -- ← map '1'/'2'/NULL
  LEFT JOIN d_merchant  dm   ON dm.merchant_id          = pf.final_merchant_id
)

SELECT
  sk,
  transaction_id,
  payment_reference,
  hasPayment,
  payment_state_sk  AS payment_state,
  merchant_sk,
  payment_method_sk,
  TimeFromStartToEndSeconds
FROM j

WHERE _gating_ts > (
  SELECT coalesce(max(_gating_ts), toDateTime('1970-01-01')) FROM gold.fact_transactions
)

  
    