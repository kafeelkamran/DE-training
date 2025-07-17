CREATE TABLE new_customers (
    customer_id INT PRIMARY KEY,
    name VARCHAR(100),
    registered_on DATE
);

INSERT INTO new_customers (customer_id, name, registered_on) VALUES
(1, 'Alice', '2022-01-10'),
(2, 'Bob', '2023-02-15'),
(3, 'Charlie', '2021-08-01'),
(4, 'David', '2023-07-15'),
(5, 'Eve', '2022-11-25'),
(6, 'Frank', '2024-01-01');
CREATE TABLE new_orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    order_date DATE,
    FOREIGN KEY (customer_id) REFERENCES new_customers(customer_id)
);

INSERT INTO new_orders (order_id, customer_id, order_date) VALUES
(101, 1, '2023-05-10'),    -- > 365 days ago
(102, 2, '2024-11-01'),    -- < 365 days ago
(103, 4, '2025-03-20'),    -- < 365 days ago
(104, 6, '2023-06-10');    -- > 365 days ago

