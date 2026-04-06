# Dirty Cafe Sales - Databricks Data Pipeline

## 📌 Overview
This project is an end-to-end data engineering pipeline built entirely in **Databricks SQL**. Originating from an academic assignment for the Master's in Data Mining and Knowledge Discovery at Universidad de Buenos Aires (UBA), this repository elevates the original requirements into a modern, industry-standard **Medallion Architecture** (Bronze, Silver, Gold).

The pipeline processes the `dirty_cafe_sales.csv` dataset, handling intentional data quality issues to produce clean, analytics-ready tables for business intelligence.

## 🏗️ Architecture & Data Flow
The project follows the Medallion Architecture to progressively enrich and clean the data:

### 🥉 Bronze Layer (Raw Data)
* **Objective:** Ingest the raw `dirty_cafe_sales.csv` file without dropping any records.
* **Features:** Reads transactional columns such as Transaction ID, Item, Quantity, Price Per Unit, Payment Method, and Location as strings to prevent schema inference failures on messy data. 

### 🥈 Silver Layer (Cleansed & Conformed)
* **Objective:** Cleanse, standardize, and impute missing or corrupted data.
* **Transformations:**
  * **Standardization:** Identifies and handles `NA`, `ERROR`, and `UNKNOWN` text strings, treating them as null values.
  * **Menu Price Imputation:** Fills missing `Price Per Unit` values by joining against a static menu reference table.
  * **Mathematical Imputation:** Calculates exactly one missing variable among `Quantity`, `Price Per Unit`, and `Total Spent` when the other two are available.
  * **Logic-Based Inference:** Infers missing `Item` names based on specific price rules (e.g., assuming "Cake" for a 3.0 price or "Sandwich" for 4.0).
  * **Mode Imputation:** Resolves any remaining gaps using the statistical mode, with strict tie-breaking rules (lowest number, alphabetical order, or oldest date).

### 🥇 Gold Layer (Business Aggregations)
* **Objective:** Serve highly refined, aggregated data for downstream analytics and reporting.
* **Key Tables:**
  * **Temporal Patterns:** Aggregates transactions by day of the week and month to identify peak sales periods and trends.
  * * **Item Performance:** Ranks the most and least frequent items sold.

## 🛠️ Tech Stack
* **Compute & Orchestration:** Databricks SQL / Databricks Free Edition
* **Storage format:** Delta Lake
* **Language:** SQL

## 👨‍💻 Author
**Daniel Ramírez**
*Data & ML Engineer*
