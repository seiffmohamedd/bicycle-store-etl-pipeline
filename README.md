# End-to-End ETL Pipeline for a Bicycle Store

## Project Overview

This project demonstrates a complete **ETL (Extract, Transform, Load)** pipeline designed for a bicycle store using **Python**. The pipeline ingests data from multiple sources including relational databases, data lakes (CSV files), and external APIs. It performs **data quality checks**, **transformation logic**, and prepares clean, structured data for analytics and business intelligence.

---

## Objective

The goal of this project is to:
- Design and implement an automated ETL pipeline using Python.
- Integrate data from a MySQL database, local data lake (CSV files), and an external currency exchange rate API.
- Perform data cleaning, transformation, and enrichments.
- Output reliable and analytics-ready datasets for business insights.

---

---

## Milestones and Stages

### Stage 1: Database & Data Lake Setup
- Create MySQL database and define schemas and tables for orders and items.
- Set up `Source/` directories to simulate external departments and API landings.

### Stage 2: Data Extraction
- Extract from MySQL using SQL queries via `psycopg2` or `SQLAlchemy`.
- Read CSV files from the data lake.
- Consolidate all data and append metadata (timestamp, source).

### Stage 3: Data Quality Checks
- Handle missing/null values in key fields.
- Validate schema, data types, and domain-specific logic (e.g., valid date ranges, price thresholds).
- Output cleaned datasets into the **staging** layer.

### Stage 4: Data Transformation
- Convert prices using the latest exchange rates.
- Add delivery KPIs (late orders, delays).
- Add `locality_flag` flag based on customer-store distance logic.
- Join with lookup tables to replace status codes.
- Store final datasets in the **business** layer for analytics tools or dashboards.





