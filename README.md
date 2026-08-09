# StayNest · Spark Deep Dive  
**Author:** Ashish

This project was completed in a **Databricks notebook**, using Spark DataFrames, SQL, and Unity Catalog Volumes. All outputs were generated directly inside Databricks.

## Tasks Overview
- Loaded and inspected the bookings dataset.  
- Selected key columns and filtered completed high‑value bookings.  
- Added GST amount, value tiers, and booking month.  
- Aggregated revenue and customer metrics by city.  
- Performed inner, left, and anti joins, plus a three‑way join.  
- Used Spark SQL and window functions to rank top hotels.  
- Wrote results as Parquet and Delta into `/Volumes/workspace/default/staynest/`.  
- Built a single chained pipeline to compute top‑revenue cities for 4‑star+ hotels.

## Summary
A compact Spark workflow demonstrating filtering, joins, aggregations, SQL, window functions, and writing data — all executed and validated inside Databricks.
