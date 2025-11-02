{% set ds_lagged = var('ds_lagged', none) %}

{{ config(
  alias = 'link_transactions',
  schema = 'silver',
  materialized = 'incremental',
  tags = ['silver','linktx'],
  on_schema_change = 'append_new_columns',
  engine = 'ReplacingMergeTree',
  order_by = '(transaction_id, _snapshot_date)',   
  partition_by = 'toDate(_snapshot_date)'
) }}

with base as (
  select
    -- make key non-nullable in the schema; also exclude null id upstream
    assumeNotNull(toUInt64OrNull(id))                      as transaction_id,
    state,
    linkpay_reference                                      as reference,
    payment_details                                        as details,
    created_at,
    updated_at,
    _ingested_at,
    toDate(_snapshot_date)                                 as _snapshot_date
  from {{ source('bronze','link_transactions') }}
  where id is not null
  {% if is_incremental() and ds_lagged is not none %}
    and _snapshot_date = toDate('{{ ds_lagged }}')
  {% endif %}
),

agg_day as (
  select
    transaction_id,
    -- pick latest by ingestion time
    argMax(state, _ingested_at)                                     as state,
    argMax(reference, _ingested_at)                                  as reference,
    argMax(details, _ingested_at)                                    as details,
    parseDateTimeBestEffortOrNull(argMax(created_at, _ingested_at))  as created_at,
    parseDateTimeBestEffortOrNull(argMax(updated_at, _ingested_at))  as updated_at,
    _snapshot_date
  from base
  group by transaction_id, _snapshot_date
)

{% if is_incremental() and ds_lagged is not none %}
-- insert only rows missing in target for this (transaction_id, snapshot)
select a.*
from agg_day a
left join {{ this }} s
  on s.transaction_id = a.transaction_id
 and s._snapshot_date = a._snapshot_date
where s.transaction_id is null
{% else %}
-- first build
select * from agg_day
{% endif %}