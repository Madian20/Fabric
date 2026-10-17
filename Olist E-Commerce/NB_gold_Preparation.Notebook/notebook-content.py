# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "73781eba-72b7-4dad-9927-1179e19bc079",
# META       "default_lakehouse_name": "LH_silver",
# META       "default_lakehouse_workspace_id": "9d196083-db64-4212-9c53-2f2ad21842f8",
# META       "known_lakehouses": [
# META         {
# META           "id": "73781eba-72b7-4dad-9927-1179e19bc079"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "6caf17b0-89a2-a9b4-42ad-4cb39b8df86a",
# META       "known_warehouses": [
# META         {
# META           "id": "6caf17b0-89a2-a9b4-42ad-4cb39b8df86a",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## prepare environment

# CELL ********************

# import libraries
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# from delta.tables import DeltaTable
import com.microsoft.spark.fabric

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# conecting to the Delta tables in LH_silver

df_customers = spark.sql("""
    SELECT customer_unique_id, customer_city, customer_state
    FROM LH_silver.dbo.customers
""")

df_order_items    = spark.sql("SELECT * FROM LH_silver.dbo.order_items").drop("_ingested_at")

df_order_payments = spark.sql("SELECT * FROM LH_silver.dbo.order_payments").drop("_ingested_at")


df_order_reviews  = spark.sql("SELECT * FROM LH_silver.dbo.order_reviews").drop("_ingested_at")

df_orders = spark.sql("""
    SELECT order_id, customer_id, order_status,
           order_purchase_timestamp, order_approved_at,
           order_delivered_carrier_date, order_delivered_customer_date,
           order_estimated_delivery_date,
           flag_any_timeline_issue
    FROM LH_silver.dbo.orders
""")

df_products = spark.sql("""
    SELECT product_id, product_category_name,
           product_weight_g, product_length_cm, product_height_cm, product_width_cm
    FROM LH_silver.dbo.products
""")

df_product_category_name_translation = spark.sql("SELECT * FROM LH_silver.dbo.product_category_name_translation")

df_sellers = spark.sql("""
    SELECT seller_id, seller_city, seller_state
    FROM LH_silver.dbo.sellers
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### create df_date

# CELL ********************

from pyspark.sql import functions as F

# Date range: first purchase date to the latest date found in the orders dates
bounds = df_orders.agg(
    F.min(F.to_date("order_purchase_timestamp")).alias("min_date"),
    F.greatest(
        F.max(F.to_date("order_estimated_delivery_date")),
        F.max(F.to_date("order_delivered_customer_date"))
    ).alias("max_date")
).first()

# Extend to full calendar years (Jan 1 to Dec 31) so time intelligence has no gaps
start_date = bounds["min_date"].replace(month=1, day=1)
end_date   = bounds["max_date"].replace(month=12, day=31)

# One row per day, with only date and date_key
df_date = (
    spark.range(1)
    .select(F.explode(F.sequence(F.lit(start_date), F.lit(end_date), F.expr("interval 1 day"))).alias("date"))
    .select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),  # join key
        F.col("date"),
    )
)

print(f"dim_date: {df_date.count()} rows, {start_date} to {end_date}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## cleaning the tables

# MARKDOWN ********************

# ### cleaning order_payments table

# CELL ********************

df_order_payments = (
    df_order_payments
    # Remove payments with an undefined type
    .filter(F.col("payment_type") != "not_defined")
    # Remove payments with zero installments (invalid value)
    .filter(F.col("payment_installments") != 0)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### cleaning orders table

# CELL ********************

# Log how many rows will be removed (for the documentation issue)
total = df_orders.count()
removed = df_orders.filter(F.col("flag_any_timeline_issue")).count()
print(f"Orders removed due to timeline issues: {removed} of {total}")

# Keep only clean orders, then drop the flag column
df_orders = (
    df_orders
    .filter(~F.col("flag_any_timeline_issue"))
    .drop("flag_any_timeline_issue")
)

# Remove child rows whose order was removed (left_semi keeps matching order_ids only)
valid_orders = df_orders.select("order_id")
df_order_items    = df_order_items.join(valid_orders, "order_id", "left_semi")
df_order_payments = df_order_payments.join(valid_orders, "order_id", "left_semi")
df_order_reviews  = df_order_reviews.join(valid_orders, "order_id", "left_semi")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### cleaning product table

# CELL ********************

# Remove products with a null category
df_products = df_products.filter(F.col("product_category_name").isNotNull())

# Remove order_items rows whose product_id no longer exists in products (orphans)
valid_products = df_products.select("product_id")

orphan_items = df_order_items.join(valid_products, "product_id", "left_anti").count()
print(f"order_items rows removed (orphan product_id): {orphan_items}")

df_order_items = df_order_items.join(valid_products, "product_id", "left_semi")

# Join with the translation table (keep only the English name)
df_translation = df_product_category_name_translation.select(
    "product_category_name", "product_category_name_english"
)
df_products = df_products.join(df_translation, "product_category_name", "left")

# If there is no translation, keep the original (Portuguese) name instead of null
df_products = df_products.withColumn(
    "product_category_name_english",
    F.coalesce("product_category_name_english", "product_category_name")
)

# Drop the Portuguese name and keep the English one under the original column name
df_products = (
    df_products
    .drop("product_category_name")
    .withColumnRenamed("product_category_name_english", "product_category_name")
)

# Check: should be 0
print(f"Products with null category: {df_products.filter(F.col('product_category_name').isNull()).count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## load to gold

# CELL ********************

WAREHOUSE = "WH_gold"   # replace with your exact warehouse name
SCHEMA = "dbo"


# ---- Rename DataFrames to their Gold names 
# Facts
fact_order_items    = df_order_items
fact_order_reviews  = df_order_reviews
fact_order_payments = df_order_payments

# Dimensions
dim_orders    = df_orders
dim_customers = df_customers
dim_products  = df_products
dim_sellers   = df_sellers
dim_date      = df_date


# ---- Full load: overwrite the whole table in the Warehouse ----
def write_full(df, name):
    df.write.mode("overwrite").synapsesql(f"{WAREHOUSE}.{SCHEMA}.{name}")
    print(f"{name}: {df.count()} rows loaded")

tables = {
    "dim_date":            dim_date,
    "dim_customers":       dim_customers,
    "dim_products":        dim_products,
    "dim_sellers":         dim_sellers,
    "dim_orders":          dim_orders,
    "fact_order_items":    fact_order_items,
    "fact_order_payments": fact_order_payments,
    "fact_order_reviews":  fact_order_reviews,
}

for name, df in tables.items():
    write_full(df, name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 
