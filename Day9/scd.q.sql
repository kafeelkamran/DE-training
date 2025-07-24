create database SCD;
use SCD;

CREATE TABLE dim_category (
  category_sk INT PRIMARY KEY AUTO_INCREMENT,
  category_id INT UNIQUE,
  category_name VARCHAR(100)
);

CREATE TABLE dim_product (
  product_sk INT PRIMARY KEY AUTO_INCREMENT,
  product_id INT UNIQUE,
  product_name VARCHAR(100),
  brand VARCHAR(100),
  category_id INT,
  FOREIGN KEY (category_id) REFERENCES dim_category(category_id)
);

CREATE TABLE dim_region (
  region_sk INT PRIMARY KEY AUTO_INCREMENT,
  region_id INT UNIQUE,
  region_name VARCHAR(100)
);

CREATE TABLE dim_city (
  city_sk INT PRIMARY KEY AUTO_INCREMENT,
  city_id INT UNIQUE,
  city_name VARCHAR(100),
  region_id INT,
  FOREIGN KEY (region_id) REFERENCES dim_region(region_id)
);

CREATE TABLE dim_store (
  store_sk INT PRIMARY KEY AUTO_INCREMENT,
  store_id INT UNIQUE,
  store_name VARCHAR(100),
  city_id INT,
  FOREIGN KEY (city_id) REFERENCES dim_city(city_id)
);

-- SCD Type 2 Implementation for Customer
CREATE TABLE dim_customer (
  customer_sk INT PRIMARY KEY AUTO_INCREMENT,
  customer_id INT,
  customer_name VARCHAR(100),
  email VARCHAR(100),
  address VARCHAR(200),
  start_date DATE,
  end_date DATE,
  is_current BOOLEAN,
  UNIQUE (customer_id, start_date)
);

CREATE TABLE fact_sales (
  sale_id INT PRIMARY KEY,
  product_sk INT,
  store_sk INT,
  customer_sk INT,
  quantity INT,
  amount DECIMAL(10,2),
  sale_date DATE,
  FOREIGN KEY (product_sk) REFERENCES dim_product(product_sk),
  FOREIGN KEY (store_sk) REFERENCES dim_store(store_sk),
  FOREIGN KEY (customer_sk) REFERENCES dim_customer(customer_sk)
);

CREATE TABLE fact_inventory (
  inventory_id INT PRIMARY KEY,
  product_sk INT,
  store_sk INT,
  snapshot_date DATE,
  quantity_available INT,
  FOREIGN KEY (product_sk) REFERENCES dim_product(product_sk),
  FOREIGN KEY (store_sk) REFERENCES dim_store(store_sk)
);