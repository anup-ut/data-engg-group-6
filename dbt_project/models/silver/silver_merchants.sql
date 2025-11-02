{%- set ds_lagged = var('ds_lagged', none) -%}
{%- set pre_hooks = [] -%}
{%- if is_incremental() and ds_lagged is not none -%}
  {%- do pre_hooks.append( ch_drop_partition(this, ds_lagged) ) -%}
{%- endif -%}


{{ config(
  alias = 'merchants',
  schema = 'silver',
  materialized = 'incremental',
  engine       = 'ReplacingMergeTree',
  partition_by = '_snapshot_date',
  order_by     = '(_snapshot_date, assumeNotNull(merchant_id))', 
  on_schema_change = 'append_new_columns'
) }}


{# -- Probe bronze schema -- #}
{%- set q = "
  SELECT
    countIf(name='id')            AS c_id,
    countIf(name='acquirer_id')   AS c_acquirer_id,
    countIf(name='name')          AS c_name,
    countIf(name='state')         AS c_state,
    countIf(name='created_at')    AS c_created_at,
    countIf(name='updated_at')    AS c_updated_at,
    countIf(name='_ingested_at')  AS c_ingested_at,
    countIf(name='_snapshot_date')AS c_snapshot_date
  FROM system.columns
  WHERE database = 'bronze' AND table = 'merchants'
" -%}
{%- set res = run_query(q) -%}
{%- set row = (res and res.rows and res.rows[0]) or {} -%}
{%- set have_core = (row.get('c_id',0)>0 and row.get('c_name',0)>0 and row.get('c_state',0)>0
                     and row.get('c_created_at',0)>0 and row.get('c_updated_at',0)>0
                     and row.get('c_ingested_at',0)>0 and row.get('c_snapshot_date',0)>0) -%}


{%- if not have_core %}


-- No bronze yet → emit a TYPED empty set.
-- Note: key columns are NON-NULLABLE here.
select
  toUInt64(0)                                   as merchant_id,      -- type marker (no rows due to WHERE 0)
  toDate('{{ ds_lagged if ds_lagged else "1970-01-01" }}') as _snapshot_date,
  CAST(NULL AS Nullable(UInt64))                as acquirer_id,
  CAST(NULL AS Nullable(String))                as merchant_name,
  CAST(NULL AS LowCardinality(Nullable(String)))as state,
  CAST(NULL AS Nullable(DateTime))              as created_at,
  CAST(NULL AS Nullable(DateTime))              as updated_at,
  CAST(0 AS UInt64)                             as dim_fingerprint
where 0


{%- else %}


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
  from {{ source('bronze','merchants') }}
  {% if is_incremental() and ds_lagged %}
    where toDate(_snapshot_date) = toDate('{{ ds_lagged }}')
  {% endif %}
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
{% if is_incremental() and ds_lagged %}
where _snapshot_date = toDate('{{ ds_lagged }}')
{% endif %}


{%- endif %}
