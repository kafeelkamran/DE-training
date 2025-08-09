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