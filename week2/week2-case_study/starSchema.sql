create database week2;
use week2;

create table dim_customer(
customer_id int primary key,
first_name varchar(50),
last_name varchar(50),
email varchar(50),
contact_number varchar(20),
country varchar(30),
city varchar(30)
);

create table dim_product(
product_id int primary key,
product_name varchar(50),
category varchar(50),
sub_category varchar(50),
price decimal(10,2),
date_released datetime
);


create table fact_sales(
sales_id int primary key,
customer_id int,
product_id int,
quantity int,
unit_price decimal(10,2),
order_date datetime,
foreign key(customer_id ) references dim_customer(customer_id),
foreign key(product_id) references dim_product(product_id)
);