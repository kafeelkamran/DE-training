CREATE OR REPLACE TEMP VIEW sales_fact_star AS
SELECT
  f.sale_sk,
  f.sale_date,
  f.amount,
  c.customer_hk,
  hc.customer_id,
  hc.email,
  c.city,
  c.loyalty_status,
  p.product_hk,
  hp.product_id,
  hp.product_name,
  p.price,
  p.category
FROM fact_sales f
LEFT JOIN pit_customer c ON f.customer_hk = c.customer_hk
LEFT JOIN hub_customer hc ON f.customer_hk = hc.customer_hk
LEFT JOIN sat_product p ON f.product_hk = p.product_hk
LEFT JOIN hub_product hp ON f.product_hk = hp.product_hk;
