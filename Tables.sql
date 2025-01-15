create database Datawarehouse;
use Datawarehouse;
drop table Products;
drop table Customer;
drop table FactTable;

CREATE TABLE Products (
    productID INT PRIMARY KEY,
    productName VARCHAR(255) NOT NULL,
    productPrice DECIMAL(10, 2) NOT NULL,
    supplierID INT NOT NULL,
    supplierName VARCHAR(255) NOT NULL,
    storeID INT NOT NULL,
    storeName VARCHAR(255) NOT NULL
);

CREATE TABLE Customer (
customer_id INT PRIMARY KEY,
customer_name VARCHAR(255) NOT NULL,
gender VARCHAR(255) NOT NULL
);

CREATE TABLE Orders (
    OrderID INT NOT NULL,
    OrderDate DATETIME NOT NULL,
    ProductID INT NOT NULL,
    QuantityOrdered INT NOT NULL,
    CustomerID INT NOT NULL,
    TimeID INT NOT NULL
);

CREATE TABLE FactTable (
    ProductID INT,
    ProductName VARCHAR(255),
    ProductPrice DECIMAL(10, 2),
    SupplierID INT,
    SupplierName VARCHAR(255),
    StoreID INT,
    StoreName VARCHAR(255),
    OrderID INT,
    OrderDate DATETIME,
    QuantityOrdered INT,
    CustomerID INT,
    CustomerName VARCHAR(255),
    Gender VARCHAR(10)
);

select * from FactTable;

