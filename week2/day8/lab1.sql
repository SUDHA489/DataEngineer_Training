

create database day8;

use day8;

CREATE TABLE fact_sales (
    sales_date date not null,      
    customer_id int not null,
    product_id int not null,      
    store_id int not null,          
    quantity_sold int,         
    sales_amount decimal(10,2)
);


INSERT INTO fact_sales (sales_date, customer_id, product_id, store_id, quantity_sold, sales_amount) VALUES
('2024-07-20', 201, 101, 1, 2, 50.00),
('2024-07-20', 202, 101, 1, 1, 25.00),
('2024-07-20', 201, 102, 1, 3, 90.00),
('2024-07-20', 203, 101, 2, 1, 25.00),
('2024-07-21', 202, 101, 1, 1, 25.00),
('2024-07-21', 201, 103, 1, 2, 60.00),
('2024-07-21', 203, 102, 2, 1, 30.00),
('2024-07-21', 203, 101, 2, 2, 50.00);



select * from fact_sales;

