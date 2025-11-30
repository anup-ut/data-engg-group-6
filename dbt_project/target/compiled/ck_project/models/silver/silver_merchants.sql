





with day_rows as (
  select
      toUInt64OrNull(id)                         as merchant_id_raw,
      toUInt64OrNull(acquirer_id)                as acquirer_id_raw,
      nullIf(name,'')                            as merchant_name_raw,
      state                                      as state_raw,
      created_at                                 as created_at_raw,
      updated_at                                 as updated_at_raw,
      _ingested_at,
      toDate(_snapshot_date)                     as _snapshot_date
  from bronze.merchants
  
    where toDate(_snapshot_date) = toDate('2025-11-08')
  
),
agg as (
  select
      merchant_id_raw                                              as merchant_id_nullable,
      _snapshot_date,
      argMax(acquirer_id_raw, _ingested_at)                        as acquirer_id,
      argMax(merchant_name_raw, _ingested_at)                      as merchant_name,
      toLowCardinality(argMax(state_raw, _ingested_at))            as state,
      parseDateTimeBestEffortOrNull(argMax(created_at_raw, _ingested_at)) as created_at,
      parseDateTimeBestEffortOrNull(argMax(updated_at_raw, _ingested_at)) as updated_at
  from day_rows
  where merchant_id_raw is not null
  group by merchant_id_nullable, _snapshot_date
)
select
  toUInt64(merchant_id_nullable)                as merchant_id,      -- <- NON-NULLABLE for ORDER BY
  _snapshot_date,                                                   -- <- NON-NULLABLE Date
  acquirer_id,
  merchant_name,
  state,
  created_at,
  updated_at,
  cityHash64(
    concat(
      toString(merchant_id),'|',
      ifNull(toString(acquirer_id),''),'|',
      ifNull(merchant_name,''),'|',
      ifNull(state,''),'|',
      ifNull(toString(created_at),''),'|',
      ifNull(toString(updated_at),'')
    )
  ) as dim_fingerprint
from agg

where _snapshot_date = toDate('2025-11-08')
