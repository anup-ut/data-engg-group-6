-- ============================================================================
-- CREATE DATABASE (if not exists)
-- ============================================================================
CREATE DATABASE IF NOT EXISTS analytics;

-- ============================================================================
-- SETUP ROLES AND USERS
-- ============================================================================

-- Create roles
CREATE ROLE IF NOT EXISTS analyst_full;
CREATE ROLE IF NOT EXISTS analyst_limited;

-- Create users
CREATE USER IF NOT EXISTS analyst_john IDENTIFIED BY 'password123';
CREATE USER IF NOT EXISTS intern_mary IDENTIFIED BY 'password456';

-- Assign roles to users
GRANT analyst_full TO analyst_john;
GRANT analyst_limited TO intern_mary;

-- Grant access to underlying Gold tables
GRANT SELECT ON gold.fact_transactions TO analyst_limited, analyst_full;
GRANT SELECT ON gold.dim_payment_method TO analyst_limited, analyst_full;
GRANT SELECT ON gold.dim_merchants TO analyst_limited, analyst_full;

-- ============================================================================
-- CREATE SIMPLE VIEW FOR FULL ANALYSTS
-- ============================================================================

CREATE VIEW IF NOT EXISTS analytics.payment_failure_rates
AS
SELECT
    coalesce(pm.payment_method_name, 'unknown') as payment_method,
    coalesce(m.merchant_name, toString(f.merchant_sk)) as provider,
    countIf(f.hasPayment = 1) as reached_provider,
    countIf(f.hasPayment = 1 AND f.payment_state IN (3, 6)) as never_completed,
    round(never_completed * 100.0 / nullIf(reached_provider, 0), 2) AS never_complete_rate_pct
FROM gold.fact_transactions AS f
LEFT JOIN gold.dim_payment_method AS pm ON f.payment_method_sk = pm.payment_method_sk
LEFT JOIN gold.dim_merchants AS m ON f.merchant_sk = m.merchant_sk
WHERE f.hasPayment = 1
GROUP BY 
    coalesce(pm.payment_method_name, 'unknown'),
    coalesce(m.merchant_name, toString(f.merchant_sk))
HAVING never_completed > 0; 

-- ============================================================================
-- CREATE SIMPLE PSEUDONYMIZED VIEW FOR LIMITED ANALYSTS
-- ============================================================================

CREATE VIEW IF NOT EXISTS analytics.payment_failure_rates_limited
AS
SELECT
    -- Pseudonymized payment_method (simple approach)
    CASE 
        WHEN pm.payment_method_name = 'Visa' THEN 'Payment_Method_A'
        WHEN pm.payment_method_name = 'Mastercard' THEN 'Payment_Method_B'
        ELSE 'Other_Method'
    END as payment_method,

    -- Pseudonymized provider (simple hash)
    concat('Provider_', substring(hex(SHA256(m.merchant_name)), 1, 6)) as provider,

    -- Pseudonymized reached_provider (bucketed)
    CASE 
        WHEN countIf(f.hasPayment = 1) < 10 THEN 'Low_Volume'
        WHEN countIf(f.hasPayment = 1) < 100 THEN 'Medium_Volume'
        ELSE 'High_Volume'
    END as reached_provider,

    -- Non-sensitive metrics (same for both)
    countIf(f.hasPayment = 1 AND f.payment_state IN (3, 6)) as never_completed,
    round(never_completed * 100.0 / nullIf(countIf(f.hasPayment = 1), 0), 2) AS never_complete_rate_pct

FROM gold.fact_transactions AS f
LEFT JOIN gold.dim_payment_method AS pm ON f.payment_method_sk = pm.payment_method_sk
LEFT JOIN gold.dim_merchants AS m ON f.merchant_sk = m.merchant_sk
WHERE f.hasPayment = 1
GROUP BY
    payment_method,  
    provider       
HAVING never_completed > 0; 

-- ============================================================================
-- GRANT PERMISSIONS
-- ============================================================================

-- Full analyst sees the full view
GRANT SELECT ON analytics.payment_failure_rates TO analyst_full;

-- Limited analyst sees the limited view
GRANT SELECT ON analytics.payment_failure_rates_limited TO analyst_limited;