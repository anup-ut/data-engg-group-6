-- ============================================================================
-- 1. DATABASE
-- ============================================================================

CREATE DATABASE IF NOT EXISTS analytics;


-- ============================================================================
-- 2. ROLES & USERS
-- ============================================================================

-- Roles
CREATE ROLE IF NOT EXISTS analyst_full;
CREATE ROLE IF NOT EXISTS analyst_limited;

-- Users who will use these roles
CREATE USER IF NOT EXISTS analyst_hardi IDENTIFIED BY 'password123';
CREATE USER IF NOT EXISTS intern_anup IDENTIFIED BY 'password456';

GRANT analyst_full    TO analyst_hardi;
GRANT analyst_limited TO intern_anup;

-- Service user that will own the views and have access to gold schema
CREATE USER IF NOT EXISTS view_definer IDENTIFIED BY 'view_secret';


-- ============================================================================
-- 3. PERMISSIONS ON GOLD TABLES (SERVICE USER ONLY)
-- ============================================================================

-- Only the service user sees raw gold data
GRANT SELECT ON gold.fact_transactions   TO view_definer;
GRANT SELECT ON gold.dim_payment_method  TO view_definer;
GRANT SELECT ON gold.dim_merchants       TO view_definer;
GRANT SELECT ON gold.dim_payment_state   TO view_definer;


-- ============================================================================
-- 4. ANALYTICAL VIEW: OVERALL COUNT OF PAYMENTS THAT NEVER REACH PSP
--    (non-sensitive aggregate → both roles can see this)
-- ============================================================================

CREATE OR REPLACE VIEW analytics.never_reached_psp_overall
DEFINER = view_definer
SQL SECURITY DEFINER
AS
SELECT
    count() AS total_initiated_payments_never_reached_psp
FROM gold.fact_transactions f
LEFT JOIN gold.dim_payment_state s
    ON s.payment_state_sk = f.payment_state
WHERE
      f.hasPayment = 0                               -- never sent to PSP
   OR coalesce(s.is_payment_reached_provider, 0) = 0; -- state says "not reached"


-- ============================================================================
-- 5. ORIGINAL FAILURE-RATE VIEW (METHOD + PROVIDER) – FULL & LIMITED
--    (kept from earlier, uses same pseudonymization rules)
-- ============================================================================

-- FULL VIEW
CREATE OR REPLACE VIEW analytics.payment_failure_rates
DEFINER = view_definer
SQL SECURITY DEFINER
AS
SELECT
    CASE
        WHEN pm.payment_method_name IS NULL OR pm.payment_method_name = ''
            THEN 'ACH transfer'
        ELSE pm.payment_method_name
    END AS payment_method,
    coalesce(m.merchant_name, toString(f.merchant_sk))                 AS provider,
    countIf(f.hasPayment = 1)                                          AS reached_provider,
    countIf(f.hasPayment = 1 AND f.payment_state IN (3, 6))            AS never_completed,
    round(never_completed * 100.0 / nullIf(reached_provider, 0), 2)    AS never_complete_rate_pct
FROM gold.fact_transactions AS f
LEFT JOIN gold.dim_payment_method AS pm ON f.payment_method_sk = pm.payment_method_sk
LEFT JOIN gold.dim_merchants      AS m  ON f.merchant_sk        = m.merchant_sk
WHERE f.hasPayment = 1
GROUP BY
    payment_method,
    provider
HAVING never_completed > 0;


-- LIMITED / PSEUDONYMIZED VIEW
CREATE OR REPLACE VIEW analytics.payment_failure_rates_limited
DEFINER = view_definer
SQL SECURITY DEFINER
AS
SELECT
    -- Pseudonymized payment_method (same mapping used everywhere)
    CASE
        WHEN pm.payment_method_name = 'Visa'       THEN 'Payment_Method_A'
        WHEN pm.payment_method_name = 'Mastercard' THEN 'Payment_Method_B'
        ELSE 'Other_Method'
    END AS payment_method,

    -- Pseudonymized provider (same hash pattern used everywhere)
    concat('Provider_', substring(hex(SHA256(m.merchant_name)), 1, 6)) AS provider,

    -- Bucketed volume instead of exact count
    CASE
        WHEN countIf(f.hasPayment = 1) < 10   THEN 'Low_Volume'
        WHEN countIf(f.hasPayment = 1) < 100  THEN 'Medium_Volume'
        ELSE 'High_Volume'
    END AS reached_provider,

    -- Non-sensitive metrics
    countIf(f.hasPayment = 1 AND f.payment_state IN (3, 6)) AS never_completed,
    round(
        never_completed * 100.0
        / nullIf(countIf(f.hasPayment = 1), 0),
        2
    ) AS never_complete_rate_pct

FROM gold.fact_transactions AS f
LEFT JOIN gold.dim_payment_method AS pm ON f.payment_method_sk = pm.payment_method_sk
LEFT JOIN gold.dim_merchants      AS m  ON f.merchant_sk        = m.merchant_sk
WHERE f.hasPayment = 1
GROUP BY
    payment_method,
    provider
HAVING never_completed > 0;


-- ============================================================================
-- 6. VIEW: ABANDONED AFTER REACHING PSP, BY MERCHANT
-- ============================================================================

-- FULL VIEW
CREATE OR REPLACE VIEW analytics.abandoned_by_merchant
DEFINER = view_definer
SQL SECURITY DEFINER
AS
SELECT
    coalesce(m.merchant_name, toString(f.merchant_sk)) AS merchant,
    count() AS reached_then_abandoned
FROM gold.fact_transactions f
LEFT JOIN gold.dim_merchants m
    ON m.merchant_sk = f.merchant_sk
WHERE coalesce(f.hasPayment, 0) = 1
  AND f.payment_state = 6
GROUP BY
    merchant
ORDER BY
    reached_then_abandoned DESC;


-- LIMITED / PSEUDONYMIZED VIEW
CREATE OR REPLACE VIEW analytics.abandoned_by_merchant_limited
DEFINER = view_definer
SQL SECURITY DEFINER
AS
SELECT
    concat('Provider_', substring(hex(SHA256(m.merchant_name)), 1, 6)) AS merchant,
    count() AS reached_then_abandoned
FROM gold.fact_transactions f
LEFT JOIN gold.dim_merchants m
    ON m.merchant_sk = f.merchant_sk
WHERE coalesce(f.hasPayment, 0) = 1
  AND f.payment_state = 6
GROUP BY
    merchant
ORDER BY
    reached_then_abandoned DESC;


-- ============================================================================
-- 7. VIEW: FAILURE RATE BY PAYMENT METHOD
-- ============================================================================

-- FULL VIEW
CREATE OR REPLACE VIEW analytics.failure_rate_by_payment_method
DEFINER = view_definer
SQL SECURITY DEFINER
AS
SELECT
    CASE
        WHEN pm.payment_method_name IS NULL OR pm.payment_method_name = ''
            THEN 'ACH transfer'
        ELSE pm.payment_method_name
    END AS payment_method,
    countIf(f.hasPayment = 1)                               AS total_reached_provider,
    countIf(f.hasPayment = 1 AND f.payment_state IN (3, 6)) AS total_never_completed,
    round(
        total_never_completed * 100.0
        / nullIf(total_reached_provider, 0),
        2
    ) AS never_complete_rate_pct
FROM gold.fact_transactions AS f
LEFT JOIN gold.dim_payment_method AS pm
    ON pm.payment_method_sk = f.payment_method_sk
GROUP BY
    payment_method
ORDER BY
    never_complete_rate_pct DESC,
    total_reached_provider DESC;


-- LIMITED / PSEUDONYMIZED VIEW
CREATE OR REPLACE VIEW analytics.failure_rate_by_payment_method_limited
DEFINER = view_definer
SQL SECURITY DEFINER
AS
SELECT
    -- Same payment_method pseudonymization as before
    CASE
        WHEN pm.payment_method_name = 'Visa'       THEN 'Payment_Method_A'
        WHEN pm.payment_method_name = 'Mastercard' THEN 'Payment_Method_B'
        ELSE 'Other_Method'
    END AS payment_method,
    countIf(f.hasPayment = 1)                               AS total_reached_provider,
    countIf(f.hasPayment = 1 AND f.payment_state IN (3, 6)) AS total_never_completed,
    round(
        total_never_completed * 100.0
        / nullIf(total_reached_provider, 0),
        2
    ) AS never_complete_rate_pct
FROM gold.fact_transactions AS f
LEFT JOIN gold.dim_payment_method AS pm
    ON pm.payment_method_sk = f.payment_method_sk
GROUP BY
    payment_method
ORDER BY
    never_complete_rate_pct DESC,
    total_reached_provider DESC;


-- ============================================================================
-- 8. VIEW: FAILURE RATE BY PROVIDER (MERCHANT)
-- ============================================================================

-- FULL VIEW
CREATE OR REPLACE VIEW analytics.failure_rate_by_provider
DEFINER = view_definer
SQL SECURITY DEFINER
AS
SELECT
    coalesce(m.merchant_name, toString(f.merchant_sk)) AS provider,
    countIf(f.hasPayment = 1)                               AS total_reached_provider,
    countIf(f.hasPayment = 1 AND f.payment_state IN (3, 6)) AS total_never_completed,
    round(
        total_never_completed * 100.0
        / nullIf(total_reached_provider, 0),
        2
    ) AS never_complete_rate_pct
FROM gold.fact_transactions AS f
LEFT JOIN gold.dim_merchants AS m
    ON m.merchant_sk = f.merchant_sk
GROUP BY
    provider
HAVING
    total_reached_provider > 0
ORDER BY
    never_complete_rate_pct DESC,
    total_reached_provider DESC;


-- LIMITED / PSEUDONYMIZED VIEW
CREATE OR REPLACE VIEW analytics.failure_rate_by_provider_limited
DEFINER = view_definer
SQL SECURITY DEFINER
AS
SELECT
    -- Same provider hashing pattern as other limited views
    concat('Provider_', substring(hex(SHA256(m.merchant_name)), 1, 6)) AS provider,
    countIf(f.hasPayment = 1)                               AS total_reached_provider,
    countIf(f.hasPayment = 1 AND f.payment_state IN (3, 6)) AS total_never_completed,
    round(
        total_never_completed * 100.0
        / nullIf(total_reached_provider, 0),
        2
    ) AS never_complete_rate_pct
FROM gold.fact_transactions AS f
LEFT JOIN gold.dim_merchants AS m
    ON m.merchant_sk = f.merchant_sk
GROUP BY
    provider
HAVING
    total_reached_provider > 0
ORDER BY
    never_complete_rate_pct DESC,
    total_reached_provider DESC;


-- ============================================================================
-- 9. GRANTS ON VIEWS
-- ============================================================================

-- Overall non-sensitive aggregate: both roles can see
GRANT SELECT ON analytics.never_reached_psp_overall           TO analyst_full, analyst_limited;

-- Full vs limited views
GRANT SELECT ON analytics.payment_failure_rates               TO analyst_full;
GRANT SELECT ON analytics.payment_failure_rates_limited       TO analyst_limited;

GRANT SELECT ON analytics.abandoned_by_merchant               TO analyst_full;
GRANT SELECT ON analytics.abandoned_by_merchant_limited       TO analyst_limited;

GRANT SELECT ON analytics.failure_rate_by_payment_method      TO analyst_full;
GRANT SELECT ON analytics.failure_rate_by_payment_method_limited TO analyst_limited;

GRANT SELECT ON analytics.failure_rate_by_provider            TO analyst_full;
GRANT SELECT ON analytics.failure_rate_by_provider_limited    TO analyst_limited;

