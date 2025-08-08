

SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE';

create table customer(c_id int primary key,firstname varchar(20),lastname varchar(20),email varchar(50),city varchar(50),created_date date);

create table products(p_id int primary key, productname varchar(50),category varchar(50),price decimal(18,2),stockquantity int);

create table transactions(t_id int primary key, c_id int, p_id int ,quantity int,transaction_date date);


SELECT * FROM customer;
SELECT * FROM products;
SELECT * FROM transactions;



INSERT INTO Customer
VALUES
(1, 'John', 'Doe', 'john.doe@example.com', 'Mumbai', '2023-01-01'),
(2, 'Jane', 'Smith', 'jane.smith@example.com', 'Delhi', '2023-02-15'),
(3, 'Ravi', 'Kumar', 'ravi.kumar@example.com', 'Bangalore', '2023-03-10');



INSERT INTO products
values
(101, 'Laptop', 'Electronics', 65000.00, 50),
(102, 'Smartphone', 'Electronics', 30000.00, 100),
(103, 'Washing Machine', 'Home Appliances', 15000.00, 20);


INSERT INTO transactions
VALUES
(1001, 1, 101, 1, '2023-07-01'),
(1002, 2, 102, 2, '2023-07-05'),
(1003, 3, 103, 1, '2023-07-10');