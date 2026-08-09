# Databricks notebook source
# MAGIC %md
# MAGIC # StayNest - Session 6 Assignment (PySpark Deep Dive)
# MAGIC Work through the 8 tasks below in order. Read the Assignment Questions PDF for the
# MAGIC full detail and acceptance criteria. Fill in each `# TODO` cell, run it, and keep the
# MAGIC output visible. Run on Databricks Free Edition (serverless).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 0 - Setup (already done for you)
# MAGIC Upload `bookings.csv`, `hotels.csv`, `customers.csv` to a Volume, then set `BASE`
# MAGIC to that path and run this cell. Counts should be 12000 / 200 / 2000.

# COMMAND ----------

# Point BASE at YOUR Volume path
BASE = "/Volumes/workspace/default/staynest"

print(spark.version)

read_csv = lambda name: (spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{BASE}/{name}.csv"))

bookings_df   = read_csv("bookings")
hotels_df     = read_csv("hotels")
customers_df  = read_csv("customers")

print(f"bookings: {bookings_df.count()}, "
      f"hotels: {hotels_df.count()}, "
      f"customers: {customers_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 - Read and inspect
# MAGIC Show the schema, a few sample rows, the row count, and summary stats for the
# MAGIC numeric columns of `bookings_df`.

# COMMAND ----------

from pyspark.sql.functions import col

# Show schema
bookings_df.printSchema()

# Show sample rows
bookings_df.show(5)

# Row count (action → triggers Spark job)
print("Row count:", bookings_df.count())

# Summary stats for numeric columns
bookings_df.describe().show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 - Select and filter
# MAGIC From `bookings_df`, select a few useful columns and return the **completed**
# MAGIC bookings with `amount` over 10000 in the cities Goa or Mumbai. Use `col()`, combine
# MAGIC conditions with `&`, and use `.isin(...)`.

# COMMAND ----------

from pyspark.sql.functions import col

filtered_df = (
    bookings_df
        .select(
            col("booking_id"),
            col("customer_id"),
            col("hotel_id"),
            col("city"),
            col("amount"),
            col("status")
        )
        .filter(
            (col("status") == "completed") &
            (col("amount") > 10000) &
            (col("city").isin("Goa", "Mumbai"))
        )
)

filtered_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 - Derived columns
# MAGIC Add: `amount_with_gst` (amount plus 12% tax), a `value_tier`
# MAGIC (premium / standard / budget) using `when`/`otherwise`, and a `booking_month`
# MAGIC from `booking_date`.

# COMMAND ----------

from pyspark.sql.functions import col, when, month

derived_df = (
    bookings_df
        .withColumn(
            "amount_with_gst",
            col("amount") * 1.12
        )
        .withColumn(
            "value_tier",
            when(col("amount") > 20000, "premium")
            .when(col("amount") > 10000, "standard")
            .otherwise("budget")
        )
        .withColumn(
            "booking_month",
            month(col("booking_date"))
        )
)

derived_df.show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 - Aggregations
# MAGIC For **completed** bookings, group by `city` and return: number of bookings, total
# MAGIC revenue, average amount, biggest booking, and the count of unique customers.
# MAGIC Order by revenue, highest first.

# COMMAND ----------

from pyspark.sql.functions import col, count, sum, avg, max, countDistinct

city_agg_df = (
    bookings_df
        .filter(col("status") == "completed")
        .groupBy(col("city"))
        .agg(
            count("*").alias("num_bookings"),
            sum(col("amount")).alias("total_revenue"),
            avg(col("amount")).alias("avg_amount"),
            max(col("amount")).alias("max_amount"),
            countDistinct(col("customer_id")).alias("unique_customers")
        )
        .orderBy(col("total_revenue").desc())
)

city_agg_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5 - Joins
# MAGIC Inner-join bookings to hotels to enrich each booking. Do a left join too. Use
# MAGIC `left_anti` to check for orphaned bookings (expect 0). Then do a three-way join
# MAGIC with customers.

# COMMAND ----------

from pyspark.sql.functions import col

# --- 1. Inner join: bookings enriched with hotel info ---
inner_join_df = (
    bookings_df
        .join(
            hotels_df,
            on="hotel_id",
            how="inner"
        )
)

inner_join_df.show(5)


# --- 2. Left join: keep all bookings, enrich where possible ---
left_join_df = (
    bookings_df
        .join(
            hotels_df,
            on="hotel_id",
            how="left"
        )
)

left_join_df.show(5)


# --- 3. Left-anti join: orphaned bookings (expect 0) ---
orphans_df = (
    bookings_df
        .join(
            hotels_df,
            on="hotel_id",
            how="left_anti"
        )
)

orphans_df.show()   # should be empty


# --- 4. Three-way join: bookings + hotels + customers ---
three_way_df = (
    bookings_df
        .join(hotels_df, on="hotel_id", how="inner")
        .join(customers_df, on="customer_id", how="inner")
)

three_way_df.show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6 - Spark SQL + a window function
# MAGIC Register temp views and use `spark.sql` to get revenue by hotel `category` for
# MAGIC completed bookings. Then use a window function to rank the **top 3 hotels by
# MAGIC revenue within each city**.

# COMMAND ----------

# spark.sql(...) for revenue by category
bookings_df.createOrReplaceTempView("bookings")
hotels_df.createOrReplaceTempView("hotels")
customers_df.createOrReplaceTempView("customers")

category_revenue_df = spark.sql("""
    SELECT
        h.category,
        SUM(b.amount) AS total_revenue
    FROM bookings b
    INNER JOIN hotels h
        ON b.hotel_id = h.hotel_id
    WHERE b.status = 'completed'
    GROUP BY h.category
    ORDER BY total_revenue DESC
""")

category_revenue_df.show()


# COMMAND ----------

# window function for top 3 hotels per city
top_hotels_df = spark.sql("""
    SELECT *
    FROM (
        SELECT
            h.city,
            h.hotel_id,
            h.hotel_name,
            SUM(b.amount) AS total_revenue,
            ROW_NUMBER() OVER (
                PARTITION BY h.city
                ORDER BY SUM(b.amount) DESC
            ) AS rank
        FROM bookings b
        INNER JOIN hotels h
            ON b.hotel_id = h.hotel_id
        WHERE b.status = 'completed'
        GROUP BY h.city, h.hotel_id, h.hotel_name
    )
    WHERE rank <= 3
    ORDER BY city, rank
""")

top_hotels_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 7 - Write the result
# MAGIC Write your city-revenue result as **Parquet**, and also as a **Delta table** with
# MAGIC `saveAsTable`. Read the Delta table back to confirm.

# COMMAND ----------

city_agg_df.write.mode("overwrite").parquet(
    "/Volumes/workspace/default/staynest/city_revenue_parquet"
)

city_agg_df.write.format("delta").mode("overwrite").save(
    "/Volumes/workspace/default/staynest/city_revenue_delta"
)

spark.read.format("delta").load(
    "/Volumes/workspace/default/staynest/city_revenue_delta"
).show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 8 - One chained pipeline
# MAGIC In a single chain: keep completed bookings, join hotels, keep hotels with
# MAGIC `star_rating >= 4.0`, group by `city`, sum revenue, order descending, take the
# MAGIC top 5. End with one `.show()`.

# COMMAND ----------

from pyspark.sql.functions import col, sum

(
    bookings_df
        .filter(col("status") == "completed")
        .join(hotels_df.drop("city"), on="hotel_id", how="inner")
        .filter(col("star_rating") >= 4.0)
        .groupBy(col("city"))
        .agg(sum(col("amount")).alias("total_revenue"))
        .orderBy(col("total_revenue").desc())
        .limit(5)
        .show()
)
