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
# META         },
# META         {
# META           "id": "f48d08f6-b124-4022-9225-c78ec27438ae"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Import libraries & prepare environment

# CELL ********************

import json
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

CONFIG_PATH = r"Files/silver_cleaning_config (4).json"

BRONZE_FILES_ROOT = r"abfss://9d196083-db64-4212-9c53-2f2ad21842f8@onelake.dfs.fabric.microsoft.com/f48d08f6-b124-4022-9225-c78ec27438ae/Files"

cfg = json.loads(spark.read.text(CONFIG_PATH, wholetext=True).first()[0])
print("Tables in config:", list(cfg["tables"].keys()))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Cleaning functions 

# CELL ********************

# Accented characters and their plain equivalents (positions match 1:1 for translate)
ACCENTS_FROM = "áàâãäéèêëíìîïóòôõöúùûüçñ"
ACCENTS_TO   = "aaaaaeeeeiiiiooooouuuucn"


def bronze_path(t):
    # Build the full path to one raw CSV: root + folder from config + file name
    rel = cfg["bronze_base_path"].split("Files/", 1)[-1].strip("/")
    return f"{BRONZE_FILES_ROOT}/{rel}/{t['source_file']}"


def read_raw(path):
    # all columns are read as strings; types are applied from the config
    return (spark.read
            .option("header", True)         # first row = column names
            .option("inferSchema", False)   # keep everything as string
            .option("multiLine", True)      # allow line breaks inside quoted text (reviews)
            .option("quote", '"')           # text is wrapped in double quotes
            .option("escape", '"')          # "" inside a quoted field = one literal quote
            .csv(path))


def apply_global(df, g):
    # Rules applied to every table, switched on/off from the config
    if g.get("lowercase_column_names"):
        df = df.toDF(*[c.lower() for c in df.columns])
    exprs = []
    for c in df.columns:
        col = F.col(c)
        if g.get("trim_strings"):
            col = F.trim(col)                            # remove leading/trailing spaces
        if g.get("empty_string_to_null"):
            col = F.when(col == "", None).otherwise(col) # "" becomes a real null
        exprs.append(col.alias(c))
    return df.select(*exprs)


def apply_renames(df, mapping):
    # Rename columns per config (e.g. fix "lenght" typos); skip names that don't exist
    for old, new in mapping.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
    return df


def apply_text(df, rules):
    # Per-column text rules from config: lower, upper, strip_accents
    for c, ops in rules.items():
        col = F.col(c)
        for op in ops:
            if op == "lower":
                col = F.lower(col)
            elif op == "upper":
                col = F.upper(col)
            elif op == "strip_accents":
                # swap accented letters for plain ones, both lower and upper case
                col = F.translate(col, ACCENTS_FROM + ACCENTS_FROM.upper(),
                                  ACCENTS_TO + ACCENTS_TO.upper())
        df = df.withColumn(c, col)
    return df


def cast_col(col, t):
    if t == "int":
        return col.cast("double").cast("int")   # handles values like "40.0"
    return col.cast(t)


def apply_types(df, types):
    if not types:
        return df, {}
    # count values that were present but became null after the cast
    fail_exprs = [
        F.sum((F.col(c).isNotNull() & cast_col(F.col(c), t).isNull()).cast("int")).alias(c)
        for c, t in types.items()
    ]
    failures = df.agg(*fail_exprs).first().asDict()
    # apply the casts for real, after the failures were counted
    for c, t in types.items():
        df = df.withColumn(c, cast_col(F.col(c), t))
    return df, failures



def clean_table(t):
    # Full cleaning pipeline for one table; order matters
    raw = read_raw(bronze_path(t))
    rows_raw = raw.count()                                  # row count before any cleaning
    df = apply_global(raw, cfg["global"])                   # 1. trim, empty -> null, lowercase names
    df = apply_renames(df, t.get("rename_columns", {}))     # 2. fix column names
    df = apply_text(df, t.get("text_standardize", {}))      # 3. standardize text values
    df, cast_fail = apply_types(df, t.get("types", {}))     # 4. convert to real data types
    
    if cfg["global"].get("drop_exact_duplicates"):
        df = df.dropDuplicates()                            # 5. remove fully identical rows
    return df, rows_raw, cast_fail

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### checks

# CELL ********************

def check_table(df, t):
    # Quality report for one cleaned table (nothing is modified here)
    n = df.count()                                       # total rows
    pk = t["primary_key"]
    dup_on_key = n - df.dropDuplicates(pk).count()      # report only, not dropped
    # per column: count how many values are null (True -> 1, then sum)
    null_counts = df.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).first().asDict()
    # convert counts to % of rows; empty table -> empty dict (avoids divide by zero)
    null_pct = {c: round(100 * v / n, 2) for c, v in null_counts.items()} if n else {}
    return n, dup_on_key, null_pct

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Referential check for product_id

# CELL ********************

def check_fk(child_df, child_col, parent_df, parent_col):
    """Rows in child_df whose key does not exist in parent_df (reported, not removed)."""
    # unique list of valid keys from the parent table
    parent_keys = parent_df.select(F.col(parent_col).alias("_pk")).distinct()
    # nulls are counted separately below, so exclude them from the orphan check
    child_nonnull = child_df.filter(F.col(child_col).isNotNull())
    # left_anti join = keep only child rows with NO match in parent_keys
    orphans = child_nonnull.join(parent_keys, F.col(child_col) == F.col("_pk"), "left_anti")
    return {
        "orphan_rows": orphans.count(),                                   # how many bad rows
        "distinct_orphan_keys": orphans.select(child_col).distinct().count(),  # how many unique missing keys
        "null_fk_rows": child_df.filter(F.col(child_col).isNull()).count(),    # rows with no key at all
        "orphans": orphans,                                               # the rows themselves, for inspection
    }


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### check for delivery flags

# CELL ********************

def add_order_flags(df):
    """Adds boolean flag columns for timeline problems. Original values are not changed."""
    def flag(cond):
        return F.coalesce(cond, F.lit(False))   # null dates -> False (not a violation)

    # short names for the four timeline columns
    p  = F.col("order_purchase_timestamp")
    a  = F.col("order_approved_at")
    ca = F.col("order_delivered_carrier_date")
    cu = F.col("order_delivered_customer_date")

    # each flag is True when a step happened before the step it should follow
    df = (df
          .withColumn("flag_approved_before_purchase",  flag(a < p))
          .withColumn("flag_carrier_before_purchase",  flag(ca < p))
          .withColumn("flag_delivered_before_purchase", flag(cu < p))
          .withColumn("flag_delivered_before_approval", flag(cu < a))
          .withColumn("flag_delivered_before_carrier", flag(cu < ca))
          # status says delivered but there is no delivery date
          .withColumn("flag_delivered_status_no_date",
                      flag((F.col("order_status") == "delivered") & cu.isNull())))

    # combine all flags: True if the order has at least one problem
    flag_cols = [c for c in df.columns if c.startswith("flag_")]
    any_issue = F.lit(False)
    for c in flag_cols:
        any_issue = any_issue | F.col(c)
    return df.withColumn("flag_any_timeline_issue", any_issue)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Run all tables

# CELL ********************

clean = {}
summary, null_report, cast_report = [], {}, {}

for name, t in cfg["tables"].items():
    df, rows_raw, cast_fail = clean_table(t)
    n, dup_on_key, null_pct = check_table(df, t)
    if name == "orders":
        df = add_order_flags(df)
    clean[name] = df
    null_report[name] = null_pct
    cast_report[name] = {c: v for c, v in cast_fail.items() if v}
    summary.append({
        "table": name,
        "rows_raw": rows_raw,
        "rows_clean": n,
        "exact_dups_removed": rows_raw - n,## if name != "product_category_name_translation" else None,
        "dup_rows_on_primary_key": dup_on_key,
    })

display(pd.DataFrame(summary))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Columns where values became null because the type cast failed
print({k: v for k, v in cast_report.items() if v} or "No cast failures")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Results: product_id check

# CELL ********************

# Every product_id in order_items should exist in products
fk = check_fk(clean["order_items"], "product_id", clean["products"], "product_id")
print(f"order_items rows with product_id not in products: {fk['orphan_rows']} "
      f"({fk['distinct_orphan_keys']} distinct ids)")
print(f"order_items rows with null product_id: {fk['null_fk_rows']}")
if fk["orphan_rows"]:
    display(fk["orphans"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Results: delivery timeline flags (orders)

# CELL ********************

orders = clean["orders"]
flag_cols = [c for c in orders.columns if c.startswith("flag_")]

counts = orders.agg(
    F.count("*").alias("total_orders"),
    *[F.sum(F.col(c).cast("int")).alias(c) for c in flag_cols]
).first().asDict()
display(
    pd.DataFrame(list(counts.items()), columns=["check", "count"])
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Write to Silver **incremental**
# First run creates each table. Later runs read only rows at or after the latest `watermark_column` value already in Silver (tables without a watermark are re-read in full) and **merge** them on `primary_key`. Adds `_ingested_at` to every table (`_source_file` is added when reading).

# CELL ********************

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

FULL_REFRESH = False   # True = overwrite every Silver table from scratch


def filter_incremental(df, t):
    wm = t.get("watermark_column")
    target = t["target_table"]
    if FULL_REFRESH or not wm or not spark.catalog.tableExists(target):
        return df
    last = spark.table(target).agg(F.max(wm)).first()[0]
    if last is None:
        return df
    # ">=" plus the merge makes re-reading the boundary rows safe; null watermarks are always kept
    return df.filter((F.col(wm) >= F.lit(last)) | F.col(wm).isNull())


def write_silver(df, t):
    target, pk = t["target_table"], t["primary_key"]
    df = df.withColumn("_ingested_at", F.current_timestamp())
    # merge needs one source row per key
    new_rows = filter_incremental(df, t).dropDuplicates(pk).cache()
    n = new_rows.count()

    if FULL_REFRESH or not spark.catalog.tableExists(target):
        (new_rows.write.format("delta").mode("overwrite")
                 .option("overwriteSchema", "true").saveAsTable(target))
        action = "created"
    else:
        cond = " AND ".join(f"t.`{c}` = s.`{c}`" for c in pk)
        (DeltaTable.forName(spark, target).alias("t")
            .merge(new_rows.alias("s"), cond)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
        action = "merged"

    new_rows.unpersist()
    return {"table": target, "action": action, "rows_processed": n}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

results = [write_silver(clean[name], t) for name, t in cfg["tables"].items()]
display(pd.DataFrame(results))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
