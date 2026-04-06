-- 1. Set the environment context
USE CATALOG practice;
USE SCHEMA dirty_cafe_sales;

-- 2. Create the Bronze Table directly from a CSV
-- We cast everything to STRING to avoid schema inference failing on "ERROR" strings.
CREATE OR REPLACE TABLE bronze_cafe_sales AS
SELECT 
  CAST(`Transaction ID` AS STRING) AS transaction_id,
  CAST(`Item` AS STRING) AS item,
  CAST(`Quantity` AS STRING) AS quantity,
  CAST(`Price Per Unit` AS STRING) AS price_per_unit,
  CAST(`Total Spent` AS STRING) AS total_spent,
  CAST(`Payment Method` AS STRING) AS payment_method,
  CAST(`Location` AS STRING) AS location,
  CAST(`Transaction Date` AS STRING) AS transaction_date
FROM read_files(
  '/Volumes/practice/dirty_cafe_sales/raw_data/dirty_cafe_sales.csv', 
  format => 'csv',
  header => true,
  inferSchema => false
);