-- Create analytical view for payment failure analysis
-- This view helps business users understand payment processing performance
CREATE DATABASE IF NOT EXISTS analytics;

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