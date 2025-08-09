DROP TABLE IF EXISTS Metadata_Control;
CREATE TABLE Metadata_Control (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    TableName NVARCHAR(100) NOT NULL,      -- Name of the source table
    SourceQuery NVARCHAR(MAX) NOT NULL,    -- SQL query to extract data
    SinkPath NVARCHAR(500) NOT NULL,       -- Target storage path
    DQRules NVARCHAR(MAX) NULL             -- Data quality rules
);
INSERT INTO Metadata_Control (TableName, SourceQuery, SinkPath, DQRules)
VALUES
('Customer', 'SELECT * FROM dbo.Customer', 'raw/bronze/Customer/', 'Name!=null;Email!=null'),
('Transactions', 'SELECT * FROM dbo.Transactions', 'raw/bronze/Transactions/', 'Amount>0;TransactionDate!=null'),
('Products', 'SELECT * FROM dbo.Products', 'raw/bronze/Products/', 'Price>0;ProductName!=null');
SELECT * FROM Metadata_Control;
 
-- Customer table
CREATE TABLE dbo.Customer (
    CustomerID INT PRIMARY KEY IDENTITY(1,1),
    Name NVARCHAR(100) NOT NULL,
    Email NVARCHAR(100) NOT NULL,
    Phone NVARCHAR(20) NULL,
    CreatedDate DATETIME DEFAULT GETDATE()
);
 
-- Transactions table
CREATE TABLE dbo.Transactions (
    TransactionID INT PRIMARY KEY IDENTITY(1,1),
    CustomerID INT NOT NULL,
    Amount DECIMAL(18,2) NOT NULL,
    TransactionDate DATETIME NOT NULL,
    Description NVARCHAR(200) NULL,
    FOREIGN KEY (CustomerID) REFERENCES dbo.Customer(CustomerID)
);
 
-- Products table
CREATE TABLE dbo.Products (
    ProductID INT PRIMARY KEY IDENTITY(1,1),
    ProductName NVARCHAR(100) NOT NULL,
    Price DECIMAL(18,2) NOT NULL,
    Category NVARCHAR(50) NULL,
    CreatedDate DATETIME DEFAULT GETDATE()
);
 
-- Sample Customers
INSERT INTO dbo.Customer (Name, Email, Phone)
VALUES 
('Alice Johnson', 'alice@example.com', '555-1234'),
('Bob Smith', 'bob@example.com', '555-5678');
 
-- Sample Transactions
INSERT INTO dbo.Transactions (CustomerID, Amount, TransactionDate, Description)
VALUES
(1, 150.50, '2025-08-01', 'Laptop Purchase'),
(2, 20.00, '2025-08-02', 'Book Purchase');
 
-- Sample Products
INSERT INTO dbo.Products (ProductName, Price, Category)
VALUES
('Laptop', 1200.00, 'Electronics'),
('Book', 15.00, 'Books');
 
 
SELECT * FROM dbo.Customer;
SELECT * FROM dbo.Transactions;
SELECT * FROM dbo.Products;