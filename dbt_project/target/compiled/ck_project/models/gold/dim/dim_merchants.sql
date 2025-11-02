

-- Build a current (Type-0/1 style) merchant dimension from Silver.
-- We pick the latest attributes per merchant_id using argMax over a timestamp that
-- prefers updated_at, falling back to _snapshot_date.

WITH staged AS (
  SELECT
      merchant_id,
      merchant_name,
      state                     AS merchant_state,
      acquirer_id,
      updated_at,
      _snapshot_date,
      coalesce(updated_at, toDateTime(_snapshot_date)) AS ts
  FROM silver.merchants     -- <-- use your silver MODEL NAME here
  WHERE merchant_id IS NOT NULL
),

agg AS (
  SELECT
      toUInt64(merchant_id)                                              AS merchant_id,
      argMax(merchant_name,   ts)                                        AS merchant_name,
      argMax(merchant_state,  ts)                                        AS merchant_state,
      argMax(acquirer_id,     ts)                                        AS acquirer_id,
      max(ts)                                                            AS scd_start_ts,
      argMax(updated_at,      ts)                                        AS updated_at_raw
  FROM staged
  GROUP BY merchant_id
)

SELECT
    cityHash64(merchant_id)               AS merchant_sk,        
    merchant_id,
    merchant_name,
    merchant_state,
    acquirer_id,
    toDate(scd_start_ts)                  AS scd_start_date,    
    toDate('9999-12-31')                  AS scd_end_date,
    updated_at_raw
FROM agg
ORDER BY merchant_sk