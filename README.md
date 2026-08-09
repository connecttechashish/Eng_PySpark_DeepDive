# 📘 StayNest — Spark Deep Dive (Databricks Notebook)

## Author  
**Ashish**  
Created and executed entirely inside a **Databricks notebook**, with all outputs generated and validated within the Databricks environment.

---

## Overview
This assignment uses Spark DataFrames, Spark SQL, and Unity Catalog Volumes to process three datasets:

- **bookings.csv** — 12,000 hotel bookings  
- **hotels.csv** — 200 hotels  
- **customers.csv** — 2,000 customers  

All tasks were completed in the notebook `StayNest_S06_assignment_notebook.py`, and all results were displayed or stored directly in Databricks.

---

## 🧩 Task 1 — Read & Inspect Data
Performed basic inspection using:

- `printSchema()`  
- `show(5)`  
- `count()`  
- `describe().show()`  

Outputs were displayed directly in the Databricks notebook.

---

## 🧩 Task 2 — Select & Filter
Selected useful columns and filtered rows where:

- `status == "completed"`  
- `amount > 10000`  
- `city` is **Goa** or **Mumbai**  

Used `col()`, `.isin()`, and combined conditions with `&`.

---

## 🧩 Task 3 — Derived Columns
Added:

1. **`amount_with_gst`** — 12% tax applied  
2. **`value_tier`** — tiering via `when().otherwise()`  
3. **`booking_month`** — extracted using `month(col("booking_date"))`

All results were shown inside Databricks.

---

## 🧩 Task 4 — Aggregations
For **completed** bookings:

- Grouped by `city`  
- Computed:
  - number of bookings  
  - total revenue  
  - average amount  
  - biggest booking  
  - unique customers  
- Ordered by revenue descending  

The aggregated output was displayed in the notebook.

---

## 🧩 Task 5 — Joins
Performed:

1. **Inner join** (bookings ↔ hotels)  
2. **Left join**  
3. **Left‑anti join** (orphan check — expected 0)  

Then a **three‑way join**:

- bookings → hotels → customers  

All join results were shown directly in Databricks.

---

## 🧩 Task 6 — Spark SQL + Window Function
Registered temp views:

- `bookings`  
- `hotels`  
- `customers`

### Part A — Revenue by hotel category  
Used SQL to aggregate completed bookings by `category`.

### Part B — Top 3 hotels by revenue within each city  
Used:

- `ROW_NUMBER()`  
- `PARTITION BY city`  
- `ORDER BY SUM(amount) DESC`  

Ranked results were displayed in the notebook.

---

## 🧩 Task 7 — Write Results (Parquet + Delta)
All outputs were written inside Databricks using **Unity Catalog Volumes**, not DBFS root.

### Parquet Output  
Stored under:

```
/Volumes/workspace/default/staynest/city_revenue_parquet
```

### Delta Output  
Stored using:

```
saveAsTable("workspace.default.city_revenue_delta")
```

Read back using:

```
spark.table("workspace.default.city_revenue_delta")
```

---

## 🧩 Task 8 — One Chained Pipeline
Executed a single chained transformation:

1. Filter completed bookings  
2. Join hotels  
3. Filter `star_rating >= 4.0`  
4. Group by `city`  
5. Sum revenue  
6. Order descending  
7. Limit to top 5  
8. End with `.show()`

Final output was displayed directly in Databricks.

---

## ✔ Summary
This Databricks‑based assignment demonstrates:

- Spark DataFrame inspection  
- Filtering with combined conditions  
- Derived column creation  
- Multi‑aggregation patterns  
- Inner / left / anti joins  
- SQL + window functions  
- Writing Parquet & Delta to **Unity Catalog Volumes**  
- Chained transformations  
- All executed and validated **inside a Databricks notebook**
  
