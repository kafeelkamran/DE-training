CREATE OR REPLACE TEMP VIEW customerfact_view AS
SELECT
  f.customer_fact_sk,
  f.customer_hk,
  hc.customer_id,
  p.city,
  p.state,
  p.loyalty_status,
  f.first_purchase_date,
  f.total_spent,
  f.last_active_date
FROM fact_customer f
LEFT JOIN hub_customer hc ON f.customer_hk = hc.customer_hk
LEFT JOIN pit_customer pit ON f.customer_hk = pit.customer_hk
LEFT JOIN sat_customer_profile p ON f.customer_hk = p.customer_hk;
