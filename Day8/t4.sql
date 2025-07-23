create database BCNF;
-- Product Table: product-specific data
CREATE TABLE Product (
    ProductID INT PRIMARY KEY AUTO_INCREMENT,
    ProductName VARCHAR(100) UNIQUE,
    ProductCategory VARCHAR(100),
    Price DECIMAL(10, 2)
);

-- Customer Table: optional (if normalization includes customer entity)
CREATE TABLE Customer (
    CustomerID INT PRIMARY KEY AUTO_INCREMENT,
    CustomerName VARCHAR(100) UNIQUE
);

-- Order Table: normalized orders
CREATE TABLE Orders (
    OrderID INT PRIMARY KEY,
    CustomerID INT,
    ProductID INT,
    FOREIGN KEY (CustomerID) REFERENCES Customer(CustomerID),
    FOREIGN KEY (ProductID) REFERENCES Product(ProductID)
);


