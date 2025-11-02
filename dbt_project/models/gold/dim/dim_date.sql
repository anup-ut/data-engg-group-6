{{ config(
  schema='gold',
  alias='dim_date',
  materialized='table',
  engine='MergeTree()',
  order_by=['date_key'],
  tags=['gold_dim']
) }}

-- Generate dates from start..end using system.numbers.
-- IMPORTANT: use single-quoted literals from Jinja vars (no tojson).
WITH
  toDate('{{ var("dim_date_start") }}') AS sd,
  toDate('{{ var("dim_date_end") }}') AS ed
SELECT
  toUInt32(toYYYYMMDD(d))           AS date_key,
  d                                 AS full_date,
  toYear(d)                         AS year,
  toMonth(d)                        AS month,
  toDayOfMonth(d)                   AS day,
  toDayOfWeek(d)                    AS day_of_week,   -- 1=Mon..7=Sun
  toUInt8(toDayOfWeek(d) IN (6,7))  AS is_weekend,
  toUInt8(0)                        AS is_holiday
FROM
(
  SELECT addDays(sd, number) AS d
  FROM system.numbers
  LIMIT dateDiff('day', sd, ed) + 1
)
ORDER BY date_key