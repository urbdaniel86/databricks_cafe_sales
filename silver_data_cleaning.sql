-- 1. Set the environment context
USE CATALOG practice;
USE SCHEMA dirty_cafe_sales;

-- 2. Create the Silver Table cleaning the data from the bronze table
DECLARE OR REPLACE VARIABLE dirty_strings ARRAY<STRING> DEFAULT ARRAY('NA', 'ERROR', 'UNKNOWN');

CREATE OR REPLACE TABLE silver_clean_cafe_sales AS
WITH
-- Auxiliary prices table based on item
menu_prices AS (
  SELECT * FROM VALUES 
    ('Cookie', 1.0), ('Tea', 1.5), ('Coffee', 2.0),
    ('Cake', 3.0), ('Juice', 3.0), ('Sandwich', 4.0),
    ('Smoothie', 4.0), ('Salad', 5.0) 
  AS t(item, price_per_unit)
),
-- Auxiliary items table based on the previous table
item_prices AS(
  SELECT min(item) AS item, price_per_unit
  FROM menu_prices
  GROUP BY price_per_unit
),
-- Step 0: Standardize dirty values as NULLs
step0_nulls AS (
  SELECT transaction_id,
        CASE WHEN array_contains(dirty_strings, TRIM(item)) THEN NULL
            ELSE item END AS item,
        CAST(CASE WHEN array_contains(dirty_strings, TRIM(quantity)) THEN NULL
                  ELSE quantity END AS INT) AS quantity,
        CAST(CASE WHEN array_contains(dirty_strings, TRIM(price_per_unit)) THEN NULL
                  ELSE price_per_unit END AS DOUBLE) AS price_per_unit,
        CAST(CASE WHEN array_contains(dirty_strings, TRIM(total_spent)) THEN NULL
                  ELSE total_spent END AS DOUBLE) AS total_spent,
        CASE WHEN array_contains(dirty_strings, TRIM(payment_method)) THEN NULL
            ELSE payment_method END AS payment_method,
        CASE WHEN array_contains(dirty_strings, TRIM(location)) THEN NULL
            ELSE location END AS location,
        CAST(CASE WHEN array_contains(dirty_strings, TRIM(transaction_date)) THEN NULL
                  ELSE transaction_date END AS DATE) AS transaction_date
  FROM bronze_cafe_sales
),
-- Step 1: Impute prices from menu_prices
step1_impute_price AS (
  SELECT s.transaction_id,
        s.item,
        s.quantity,
        COALESCE(s.price_per_unit, m.price_per_unit) AS price_per_unit,
        s.total_spent,
        s.payment_method,
        s.location,
        s.transaction_date
  FROM step0_nulls s
  LEFT JOIN menu_prices m
    ON s.item = m.item
),
-- Step 2: impute missing quantity, price, and total from q x p = t formula
step2_impute_qpt AS (
  SELECT transaction_id,
        item,
        CASE WHEN (quantity IS NULL AND total_spent IS NOT NULL AND price_per_unit IS NOT NULL AND (total_spent / price_per_unit) % 1 = 0)
            THEN CAST(total_spent / NULLIF(price_per_unit, 0) AS INT)
            ELSE quantity END AS quantity,
        CASE WHEN (price_per_unit IS NULL AND total_spent IS NOT NULL AND quantity IS NOT NULL)
            THEN CAST(total_spent / NULLIF(quantity, 0) AS DOUBLE)
            ELSE price_per_unit END AS price_per_unit,
        CASE WHEN (total_spent IS NULL AND quantity IS NOT NULL AND price_per_unit IS NOT NULL)
            THEN CAST(quantity * price_per_unit AS DOUBLE)
            ELSE total_spent END AS total_spent,
        payment_method,
        location,
        transaction_date
  FROM step1_impute_price
),
-- Step 3: impute missing items from item_prices
step3_impute_item AS (
  SELECT s.transaction_id,
        COALESCE(s.item, i.item) AS item,
        s.quantity,
        s.price_per_unit,
        s.total_spent,
        s.payment_method,
        s.location,
        s.transaction_date
  FROM step2_impute_qpt s
  LEFT JOIN item_prices i
    ON s.price_per_unit = i.price_per_unit
),
-- Step 4: impute missing columns from mode
step4_impute_mode AS (
  SELECT transaction_id,
    COALESCE(item, (SELECT item FROM step3_impute_item WHERE item IS NOT NULL GROUP BY item ORDER BY COUNT(*) DESC, item ASC LIMIT 1)) AS item,
    COALESCE(quantity, (SELECT quantity FROM step3_impute_item WHERE quantity IS NOT NULL GROUP BY quantity ORDER BY COUNT(*) DESC, quantity ASC LIMIT 1)) AS quantity,
    COALESCE(price_per_unit, (SELECT price_per_unit FROM step3_impute_item WHERE price_per_unit IS NOT NULL GROUP BY price_per_unit 
                              ORDER BY COUNT(*) DESC, price_per_unit ASC LIMIT 1)) AS price_per_unit,
    COALESCE(total_spent, (SELECT total_spent FROM step3_impute_item WHERE total_spent IS NOT NULL GROUP BY total_spent 
                          ORDER BY COUNT(*) DESC, total_spent ASC LIMIT 1)) AS total_spent,
    COALESCE(payment_method, (SELECT payment_method FROM step3_impute_item WHERE payment_method IS NOT NULL GROUP BY payment_method 
                            ORDER BY COUNT(*) DESC, payment_method ASC LIMIT 1)) AS payment_method,
    COALESCE(location, (SELECT location FROM step3_impute_item WHERE location IS NOT NULL GROUP BY location ORDER BY COUNT(*) DESC, location ASC LIMIT 1)) AS location,
    COALESCE(transaction_date, (SELECT transaction_date FROM step3_impute_item WHERE transaction_date IS NOT NULL GROUP BY transaction_date 
                              ORDER BY COUNT(*) DESC, transaction_date ASC LIMIT 1)) AS transaction_date
  FROM step3_impute_item
)
-- Final step: create the table
SELECT *
FROM step4_impute_mode;