-- dbt_project/models/silver/silver_payments.sql
-- NOTE: If your bronze 'payments' JSON column is named something else,
-- adjust JSONExtractString(details, ...) accordingly.



WITH raw AS (
    SELECT
        -- Cast IDs
        toUInt64OrNull(id)           AS payment_id,
        toUInt64OrNull(merchant_id)  AS merchant_id,
        toUInt64OrNull(acquirer_id)  AS acquirer_id,

        -- Dimensions
        toLowCardinality(state)      AS state,
        toLowCardinality(card_type)  AS card_type,
        reference,
        order_reference,
        details,

        -- Normalize to string first to avoid mixed types
        /* created */
        CASE
            WHEN toTypeName(created_at) LIKE 'String%'
                THEN replaceRegexpAll(created_at, '\\s+UTC$', '')
            ELSE toString(created_at)
        END AS created_at_str,

        /* updated */
        CASE
            WHEN toTypeName(updated_at) LIKE 'String%'
                THEN replaceRegexpAll(updated_at, '\\s+UTC$', '')
            ELSE toString(updated_at)
        END AS updated_at_str

    FROM `bronze`.`payments`
),

src AS (
    SELECT
        payment_id,
        merchant_id,
        acquirer_id,
        state,
        card_type,
        reference,
        order_reference,
        details,

        -- Single parser => uniform type
        parseDateTime64BestEffortOrNull(created_at_str, 6, 'UTC') AS created_at,
        parseDateTime64BestEffortOrNull(updated_at_str, 6, 'UTC') AS updated_at
    FROM raw
)

SELECT *
FROM src
