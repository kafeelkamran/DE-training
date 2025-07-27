CREATE OR REPLACE TEMP VIEW inventoryfact_view AS
SELECT
  f.inventory_fact_sk,
  f.product_hk,
  hp.product_id,
  sp.product_name,
  sp.category,
  f.warehouse_hk,
  hw.warehouse_id,
  sw.city AS warehouse_city,
  f.stock_level,
  f.last_updated
FROM fact_inventory f
LEFT JOIN hub_product hp ON f.product_hk = hp.product_hk
LEFT JOIN sat_product sp ON f.product_hk = sp.product_hk
LEFT JOIN hub_warehouse hw ON f.warehouse_hk = hw.warehouse_hk
LEFT JOIN sat_warehouse sw ON f.warehouse_hk = sw.warehouse_hk;
