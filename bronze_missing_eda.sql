-- Checking for dirty values in the 'Item' column as an example
USE CATALOG practice;
USE SCHEMA dirty_cafe_sales;

WITH missing_item AS ( 
  SELECT COUNT(*) as item_count
  FROM bronze_cafe_sales
  WHERE item IN ('NA', 'ERROR', 'UNKNOWN') OR item IS NULL
),
missing_quantity AS (
  SELECT COUNT(*) as quantity_count
  FROM bronze_cafe_sales
  WHERE quantity IN ('NA', 'ERROR', 'UNKNOWN') OR quantity IS NULL
),
missing_payment AS (
  SELECT COUNT(*) as payment_count
  FROM bronze_cafe_sales
  WHERE payment_method IN ('NA', 'ERROR', 'UNKNOWN') OR payment_method IS NULL
),
missing_location AS (
  SELECT COUNT(*) as location_count
  FROM bronze_cafe_sales
  WHERE location IN ('NA', 'ERROR', 'UNKNOWN') OR location IS NULL
)


SELECT 'Item' as column, item_count as missing
FROM missing_item
UNION ALL
SELECT 'Quantity' as column, quantity_count as missing
FROM missing_quantity
UNION ALL
SELECT 'Payment Method' as column, payment_count as missing
FROM missing_payment
UNION ALL
SELECT 'Location' as column, location_count as missing
FROM missing_location;