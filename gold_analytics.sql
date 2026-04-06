-- 1. Set the environment context
USE CATALOG practice;
USE SCHEMA dirty_cafe_sales;

-- ==========================================
-- GOLD TABLE 1: Temporal Patterns
-- Identifies months and days with the most/least transactions (and targets Fridays)
-- ==========================================
CREATE OR REPLACE TABLE gold_time_analytics AS
SELECT 
  MONTH(transaction_date) AS sales_month,
  DATE_FORMAT(transaction_date, 'EEEE') AS day_of_week, -- Returns 'Monday', 'Tuesday', etc.
  COUNT(transaction_id) AS total_transactions,
  SUM(quantity) AS total_units_sold,
  ROUND(SUM(total_spent), 2) AS total_revenue
FROM silver_clean_cafe_sales
GROUP BY 1, 2
ORDER BY sales_month, total_transactions DESC;


-- ==========================================
-- GOLD TABLE 2: Item Performance
-- Ranks items by popularity and revenue
-- ==========================================
CREATE OR REPLACE TABLE gold_item_analytics AS
SELECT 
  item,
  COUNT(transaction_id) AS total_transactions,
  SUM(quantity) AS total_units_sold,
  ROUND(SUM(total_spent), 2) AS total_revenue
FROM silver_clean_cafe_sales
GROUP BY item
ORDER BY total_transactions DESC;