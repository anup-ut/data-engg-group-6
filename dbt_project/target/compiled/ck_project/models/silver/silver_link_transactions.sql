



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
  from bronze.link_transactions
  where id is not null
  
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


-- first build
select * from agg_day
