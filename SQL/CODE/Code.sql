-- 00_database_setup.sql
DROP DATABASE IF EXISTS shopping_db;
CREATE DATABASE shopping_db
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;
USE shopping_db;

-- 01_create_table.sql
CREATE TABLE shopping_data (
  row_id INT,
  order_id VARCHAR(20),
  order_date DATE,
  ship_date DATE,
  ship_mode VARCHAR(50),
  customer_id VARCHAR(20),
  customer_name VARCHAR(100),
  segment VARCHAR(50),
  country VARCHAR(50),
  city VARCHAR(50),
  states VARCHAR(50),
  postal_code VARCHAR(10),
  region VARCHAR(50),
  product_id VARCHAR(20),
  category VARCHAR(50),
  sub_category VARCHAR(50),
  price DECIMAL(10,2)
);

-- 02_import_data.sql
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/ShoppingDetailsCleaned.csv'
INTO TABLE shopping_data
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(@row_id, order_id, @order_date, @ship_date, ship_mode, customer_id, customer_name, segment, country, city, states, postal_code, region, product_id, category, sub_category, price)
SET 
  row_id = TRIM(@row_id),
  order_date = STR_TO_DATE(@order_date, '%d-%m-%Y'),
  ship_date = STR_TO_DATE(@ship_date, '%d-%m-%Y');

-- 03_create_view.sql
CREATE OR REPLACE VIEW shopping_core AS
SELECT 
  row_id,
  order_id,
  order_date,
  ship_date,
  DATEDIFF(ship_date, order_date) AS shipping_delay,
  customer_id,
  region,
  category,
  sub_category,
  price
FROM shopping_data;

-- 04_date_features.sql
SELECT 
  order_id,
  order_date,
  ship_date,
  DAY(order_date) AS order_day,
  MONTH(order_date) AS order_month,
  YEAR(order_date) AS order_year,
  WEEK(order_date) AS order_week,
  DATEDIFF(ship_date, order_date) AS shipping_delay
FROM shopping_data;

-- 05_category_lists.sql
SELECT DISTINCT segment FROM shopping_data;
SELECT DISTINCT region FROM shopping_data;
SELECT DISTINCT category FROM shopping_data;
SELECT DISTINCT sub_category FROM shopping_data;

-- 06_price_bins.sql
SELECT 
  CASE 
    WHEN price < 100 THEN 'Low'
    WHEN price BETWEEN 100 AND 500 THEN 'Medium'
    ELSE 'High'
  END AS price_band,
  COUNT(*) AS count
FROM shopping_data
GROUP BY price_band;

-- 07_ml_ready_view.sql
CREATE OR REPLACE VIEW shopping_ml_ready AS
SELECT 
  order_id,
  customer_id,
  segment,
  region,
  category,
  sub_category,
  price,
  DAY(order_date) AS order_day,
  MONTH(order_date) AS order_month,
  YEAR(order_date) AS order_year,
  DATEDIFF(ship_date, order_date) AS shipping_delay
FROM shopping_data;

-- 08_outlier_detection.sql
SELECT 
  order_id,
  price,
  DATEDIFF(ship_date, order_date) AS shipping_delay,
  CASE 
    WHEN price > 1000 THEN 'High Price'
    WHEN DATEDIFF(ship_date, order_date) > 10 THEN 'Long Delay'
    ELSE 'Normal'
  END AS outlier_flag
FROM shopping_data
WHERE price > 1000 OR DATEDIFF(ship_date, order_date) > 10;

-- 09_missing_data_check.sql
SELECT 
  COUNT(*) AS total_rows,
  SUM(order_id IS NULL OR order_id = '') AS missing_order_id,
  SUM(order_date IS NULL) AS missing_order_date,
  SUM(ship_date IS NULL) AS missing_ship_date,
  SUM(customer_id IS NULL OR customer_id = '') AS missing_customer_id,
  SUM(price IS NULL) AS missing_price
FROM shopping_data;

-- 10_export_to_csv.sql
SELECT * 
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/shopping_ml_ready.csv'
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
FROM shopping_ml_ready;