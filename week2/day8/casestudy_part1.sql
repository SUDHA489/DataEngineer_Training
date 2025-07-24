
use day8;

CREATE TABLE DIM_DATE (
  date_key DATE PRIMARY KEY,
  day INT,
  month INT,
  year INT
);


CREATE TABLE DIM_CUSTOMER (
  customer_id INT PRIMARY KEY,
  first_name VARCHAR(50),
  last_name VARCHAR(50),
  gender CHAR(1),
  email VARCHAR(100),
  phone_number VARCHAR(20),
  address VARCHAR(255),
  city VARCHAR(50),
  state VARCHAR(50),
  country VARCHAR(50)
);


CREATE TABLE DIM_PRODUCT (
  product_id INT PRIMARY KEY,
  product_name VARCHAR(100),
  category VARCHAR(50)
);

CREATE TABLE DIM_STORE (
  store_id INT PRIMARY KEY,
  store_name VARCHAR(100),
  store_type VARCHAR(50),
  store_address VARCHAR(255),
  region_id INT
);


CREATE TABLE DIM_REGION (
  region_id INT PRIMARY KEY,
  region_name VARCHAR(50),
  country VARCHAR(50)
);

CREATE TABLE FACT_SALES (
  sales_id INT PRIMARY KEY,
  date_key DATE,
  customer_id INT,
  product_id INT,
  store_id INT,
  region_id INT,
  quantity_sold INT,
  sales_amount DECIMAL(10, 2),

  FOREIGN KEY (date_key) REFERENCES DIM_DATE(date_key),
  FOREIGN KEY (customer_id) REFERENCES DIM_CUSTOMER(customer_id),
  FOREIGN KEY (product_id) REFERENCES DIM_PRODUCT(product_id),
  FOREIGN KEY (store_id) REFERENCES DIM_STORE(store_id),
  FOREIGN KEY (region_id) REFERENCES DIM_REGION(region_id)
);


CREATE TABLE FACT_RETURNS (
  return_id INT PRIMARY KEY,
  date_key DATE,
  customer_id INT,
  product_id INT,
  store_id INT,
  region_id INT,
  quantity_returned INT,
  return_amount DECIMAL(10, 2),

  FOREIGN KEY (date_key) REFERENCES DIM_DATE(date_key),
  FOREIGN KEY (customer_id) REFERENCES DIM_CUSTOMER(customer_id),
  FOREIGN KEY (product_id) REFERENCES DIM_PRODUCT(product_id),
  FOREIGN KEY (store_id) REFERENCES DIM_STORE(store_id),
  FOREIGN KEY (region_id) REFERENCES DIM_REGION(region_id)
);
