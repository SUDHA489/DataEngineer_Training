create database day10;
use day10;


-- create table customer(customer_id,name,address,contact)
-- create table product(product_id,name,category,price)
-- create table salesTransactions(sales_id,customer_id,product_id,purchase_date,quantity,sales_amount)


-- creating hubs

create table hub_customer(
customer_hashkey varchar(50) primary key,
customer_id int,
load_date datetime,
record_source varchar(50)
);

create table hub_product(
product_hashkey varchar(50) primary key,
product_id int,
load_date datetime,
record_source varchar(50)
);

create table hub_salesTransactions(
sales_hashkey varchar(50) primary key,
sales_id int,
load_date datetime,
record_source varchar(50)
);

-- creating links

create table links_sales_customer(
links_sales_customer_hashkey varchar(50) primary key,
sales_hashkey varchar(50),
customer_hashkey varchar(50),
LoadDate datetime,
RecordSource varchar(50),
FOREIGN KEY (sales_hashkey) REFERENCES hub_salesTransactions(sales_hashkey),
FOREIGN KEY (customer_hashkey) REFERENCES hub_customer(customer_hashkey)
);

create table link_sales_product (
  link_sales_product_hashkey varchar(50) primary key,
  sales_hashkey varchar(50),
  product_hashkey varchar(50),
  LoadDate datetime,
  RecordSource varchar(50),
  FOREIGN KEY (sales_hashkey) REFERENCES hub_salesTransactions(sales_hashkey),
  FOREIGN KEY (product_hashkey) REFERENCES hub_product(product_hashkey)
);


-- creating satellites

create table sat_customer_details(
  customer_hashkey varchar(50),
  name varchar(50),
  address varchar(50),
  contact_details varchar(50),
  LoadDate datetime,
  RecordSource varchar(50),
  FOREIGN KEY (customer_hashkey) REFERENCES hub_customer(customer_hashkey)
);

create table sat_product_details (
  product_hashkey varchar(50),
  product_name varchar(50),
  category varchar(50),
  price decimal(10, 2),
  LoadDate datetime,
  RecordSource varchar(50),
  FOREIGN KEY (product_hashkey) REFERENCES hub_product(product_hashkey)
);

create table sat_sales_details (
  sales_hashkey varchar(50),
  purchase_date datetime,
  quantity int,
  sales_amount decimal(10, 2),
  LoadDate datetime,
  RecordSource varchar(50),
  FOREIGN KEY (sales_hashkey) REFERENCES hub_salesTransactions(sales_hashkey)
);
