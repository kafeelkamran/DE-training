create database star;
use star;
-- Fact Table
CREATE TABLE fact_sales (
  sale_id INT PRIMARY KEY,
  product_id INT,
  customer_id INT,
  location_id INT,
  category_id INT,
  quantity INT,
  amount DECIMAL(10,2),
  sale_date DATE
);

CREATE TABLE dim_product (
  product_id INT PRIMARY KEY,
  product_name VARCHAR(100),
  brand VARCHAR(100),
  category_name VARCHAR(100)
);

CREATE TABLE dim_customer (
  customer_id INT PRIMARY KEY,
  customer_name VARCHAR(100),
  email VARCHAR(100),
  phone VARCHAR(20)
);

CREATE TABLE dim_location (
  location_id INT PRIMARY KEY,
  store_name VARCHAR(100),
  city VARCHAR(100),
  region VARCHAR(100)
);

CREATE TABLE dim_category (
  category_id INT PRIMARY KEY,
  category_name VARCHAR(100)
);

alter table fact_sales
add constraint fk_product_id
foreign key (product_id)
references dim_product (product_id);

alter table fact_sales
add constraint fk_customer_id
foreign key (customer_id)
references dim_customer (customer_id);

alter table fact_sales
add constraint fk_location_id
foreign key (location_id)
references dim_location (location_id);

alter table fact_sales
add constraint fk_category_id
foreign key (category_id)
references dim_category (category_id);