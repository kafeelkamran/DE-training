create database snow;
use snow;

-- Product dimension normalized
CREATE TABLE dim_category (
  category_id INT PRIMARY KEY,
  category_name VARCHAR(100)
);

CREATE TABLE dim_product (
  product_id INT PRIMARY KEY,
  product_name VARCHAR(100),
  brand VARCHAR(100),
  category_id INT
);

-- Location dimension normalized
CREATE TABLE dim_region (
  region_id INT PRIMARY KEY,
  region_name VARCHAR(100)
);

CREATE TABLE dim_city (
  city_id INT PRIMARY KEY,
  city_name VARCHAR(100),
  region_id INT
);

CREATE TABLE dim_store (
  store_id INT PRIMARY KEY,
  store_name VARCHAR(100),
  city_id INT
);

CREATE TABLE dim_customer (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(100),
    email VARCHAR(100)
);

-- Sales fact
CREATE TABLE Sales (
    sales_id INT PRIMARY KEY,
    product_id INT,
    category_id INT,
    customer_id INT,
    city_id INT,
    region_id INT,
    store_id INT,
    quantity INT,
    sales_date DATE,
    total_amount DECIMAL(10,2),

    FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
    FOREIGN KEY (category_id) REFERENCES dim_category(category_id),
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    FOREIGN KEY (store_id) REFERENCES dim_store(store_id),
    FOREIGN KEY (city_id) REFERENCES dim_city(city_id),
    FOREIGN KEY (region_id) REFERENCES dim_region(region_id)
);

-- drop table dim_category;
-- drop table dim_product;
-- drop table dim_region;
-- drop table dim_city;
-- drop table dim_store;
-- drop table dim_customer;
-- drop table fact_sales;