{% set ds_lagged = var('ds_lagged', none) %}
{% set pre_hooks = [] %}
{% if is_incremental() and ds_lagged is not none %}
  {% do pre_hooks.append(ch_drop_partition(this, ds_lagged)) %}
{% endif %}

{{ config(
  materialized='incremental',
  tags=['silver','payments'],
  on_schema_change='append_new_columns',
  engine='ReplacingMergeTree',
  order_by='(transaction_id, _snapshot_date)',
  partition_by='toDate(_snapshot_date)',
  pre_hook=pre_hooks
) }}

with base as (
  select
    /* make sort key non-nullable */
    cast(assumeNotNull(toUInt64OrNull(id)) as UInt64)                 as transaction_id,
    toUInt64OrNull(merchant_id)                                        as merchant_id,
    toUInt64OrNull(acquirer_id)                                        as acquirer_id,
    state,
    card_type,
    reference,
    order_reference,
    details,
    created_at,
    updated_at,
    _ingested_at,
    toDate(_snapshot_date)                                             as _snapshot_date
  from {{ source('bronze','payments') }}
  where id is not null
  {% if is_incremental() and ds_lagged is not none %}
    and _snapshot_date = toDate('{{ ds_lagged }}')
  {% endif %}
),

agg_day as (
  select
    transaction_id,
    anyHeavy(merchant_id)                                              as merchant_id,      -- stable enough; not in sort key
    anyHeavy(acquirer_id)                                              as acquirer_id,
    toLowCardinality(argMax(state, _ingested_at))                      as payment_state,
    toLowCardinality(argMax(card_type, _ingested_at))                  as card_type,
    argMax(reference, _ingested_at)                                    as reference,
    argMax(order_reference, _ingested_at)                              as order_reference,
    argMax(details, _ingested_at)                                      as details,
    parseDateTimeBestEffortOrNull(argMax(created_at, _ingested_at))    as created_at,
    parseDateTimeBestEffortOrNull(argMax(updated_at, _ingested_at))    as updated_at,
    _snapshot_date
  from base
  group by transaction_id, _snapshot_date
)

{% if is_incremental() and ds_lagged is not none %}
select a.*
from agg_day a
left join {{ this }} s
  on s.transaction_id = a.transaction_id
 and s._snapshot_date = a._snapshot_date
where s.transaction_id is null
{% else %}
select * from agg_day
{% endif %}