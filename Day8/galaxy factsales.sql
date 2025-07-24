create database galaxy;
use galaxy;

-- Category Table (for normalization)
CREATE TABLE dim_category (
  category_id INT PRIMARY KEY,
  category_name VARCHAR(100)
);

-- City Table (Store hierarchy)
CREATE TABLE dim_city (
  city_id INT PRIMARY KEY,
  city_name VARCHAR(100)
);

-- Shared Dimension Tables
CREATE TABLE dim_product (
  product_id INT PRIMARY KEY,
  product_name VARCHAR(100),
  brand VARCHAR(100),
  category_id INT,
  FOREIGN KEY (category_id) REFERENCES dim_category(category_id)
);

CREATE TABLE dim_store (
  store_id INT PRIMARY KEY,
  store_name VARCHAR(100),
  city_id INT,
  FOREIGN KEY (city_id) REFERENCES dim_city(city_id)
);

CREATE TABLE dim_customer (
  customer_id INT PRIMARY KEY,
  customer_name VARCHAR(100),
  email VARCHAR(100)
);

-- Sales Fact Table
CREATE TABLE fact_sales (
  sale_id INT PRIMARY KEY,
  product_id INT,
  store_id INT,
  customer_id INT,
  quantity INT,
  amount DECIMAL(10,2),
  sale_date DATE,
  FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
  FOREIGN KEY (store_id) REFERENCES dim_store(store_id),
  FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id)
);

-- Inventory Fact Table
CREATE TABLE fact_inventory (
  inventory_id INT PRIMARY KEY,
  product_id INT,
  store_id INT,
  snapshot_date DATE,
  quantity_available INT,
  FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
  FOREIGN KEY (store_id) REFERENCES dim_store(store_id)
);
