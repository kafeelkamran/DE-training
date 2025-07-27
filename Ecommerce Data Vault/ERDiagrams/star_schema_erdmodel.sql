// -- DIM TABLES

Table dim_customer {
  customer_sk int [pk]
  customer_id varchar
  email varchar
  city varchar
  loyalty_status varchar
}

Table dim_product {
  product_sk int [pk]
  product_id varchar
  product_name varchar
  category varchar
  price float
}

Table dim_warehouse {
  warehouse_sk int [pk]
  warehouse_id varchar
  city varchar
  state varchar
  capacity int
}

// -- FACT TABLES

Table fact_sales {
  sale_sk int [pk]
  sale_date date
  customer_sk int [ref: > dim_customer.customer_sk]
  product_sk int [ref: > dim_product.product_sk]
  amount float
}

Table fact_inventory {
  inventory_fact_sk int [pk]
  product_sk int [ref: > dim_product.product_sk]
  warehouse_sk int [ref: > dim_warehouse.warehouse_sk]
  stock_level int
  last_updated date
}
