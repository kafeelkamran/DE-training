SELECT c.customer_id, c.name, c.registered_on
FROM new_customers c
LEFT JOIN new_orders o 
  ON c.customer_id = o.customer_id 
     AND o.order_date >= CURDATE() - INTERVAL 365 DAY
WHERE o.order_id IS NULL;
