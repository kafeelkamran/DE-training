// -- HUB TABLES

Table hub_customer {
  customer_hk varchar [pk]
  customer_id varchar
  load_date datetime
  record_source varchar
}

Table hub_product {
  product_hk varchar [pk]
  product_id varchar
  load_date datetime
  record_source varchar
}

Table hub_warehouse {
  warehouse_hk varchar [pk]
  warehouse_id varchar
  load_date datetime
  record_source varchar
}

// -- SAT TABLES

Table sat_customer_profile {
  customer_hk varchar [ref: > hub_customer.customer_hk]
  email varchar
  city varchar
  loyalty_status varchar
  load_date datetime
  record_source varchar
}

Table sat_product {
  product_hk varchar [ref: > hub_product.product_hk]
  product_name varchar
  category varchar
  price float
  load_date datetime
  record_source varchar
}

Table sat_warehouse {
  warehouse_hk varchar [ref: > hub_warehouse.warehouse_hk]
  city varchar
  state varchar
  capacity int
  load_date datetime
  record_source varchar
}

// -- LINK TABLES

Table link_sales {
  sales_hk varchar [pk]
  customer_hk varchar [ref: > hub_customer.customer_hk]
  product_hk varchar [ref: > hub_product.product_hk]
  load_date datetime
  record_source varchar
}

// -- FACT TABLES

Table fact_sales {
  sale_sk int [pk]
  customer_hk varchar [ref: > hub_customer.customer_hk]
  product_hk varchar [ref: > hub_product.product_hk]
  sale_date date
  amount float
}

Table fact_inventory {
  inventory_fact_sk int [pk]
  product_hk varchar [ref: > hub_product.product_hk]
  warehouse_hk varchar [ref: > hub_warehouse.warehouse_hk]
  stock_level int
  last_updated date
}

// -- PIT TABLES 

Table pit_customer {
  customer_hk varchar [pk, ref: > hub_customer.customer_hk]
  effective_from datetime
  effective_to datetime
  current_flag boolean
}
