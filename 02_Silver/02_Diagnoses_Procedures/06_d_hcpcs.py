# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — HCPCS Reference Dictionary (`d_hcpcs`)
# MAGIC
# MAGIC **Source:** `cdr_mimiciv.bronze.d_hcpcs`  
# MAGIC **Target:** `cdr_mimiciv.silver_v2.d_hcpcs`
# MAGIC
# MAGIC **Objective:** Standardize HCPCS reference codes, categories and descriptions while preserving their original clinical meaning.
# MAGIC
# MAGIC **Expected grain:** One record per HCPCS code (`code`). Uniqueness will be validated against the source data.
# MAGIC
# MAGIC **Silver rules:** Preserve all Bronze records, standardize data types and whitespace, validate code completeness and uniqueness, and reconcile Bronze–Silver counts. No deletion, deduplication, filtering, imputation or aggregation.

# COMMAND ----------

# Import PySpark functions
from pyspark.sql import functions as F

# COMMAND ----------

# Configure source and target tables
CATALOG = "cdr_mimiciv"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver_v2"

SOURCE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.d_hcpcs"
TARGET_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.d_hcpcs"

# COMMAND ----------

# Load and inspect the HCPCS reference dictionary
bronze_df = spark.table(SOURCE_TABLE)

bronze_df.printSchema()
display(bronze_df.limit(20))

print("Bronze rows:", bronze_df.count())

# COMMAND ----------

# Profile source row counts and missing values
bronze_profile = bronze_df.select(
    F.count("*").alias("total_rows"),
    *[
        F.sum(F.col(c).isNull().cast("int")).alias(f"null_{c}")
        for c in bronze_df.columns
    ]
)

display(bronze_profile)

# COMMAND ----------

# Standardize HCPCS codes, categories and descriptions
silver_df = bronze_df.select(
    F.trim(F.col("code").cast("string")).alias("code"),
    F.col("category").cast("int").alias("category"),
    F.trim(F.col("long_description").cast("string")).alias("long_description"),
    F.trim(F.col("short_description").cast("string")).alias("short_description")
)

silver_df.printSchema()
display(silver_df.limit(20))

# COMMAND ----------

# Check repeated HCPCS codes without removing records
duplicate_df = (
    silver_df
    .groupBy("code")
    .count()
    .filter(F.col("count") > 1)
)

duplicate_count = duplicate_df.count()

print("Duplicate HCPCS code groups:", duplicate_count)
display(duplicate_df.limit(20))

# COMMAND ----------

# Check missing codes, categories and descriptions
quality_df = silver_df.select(
    *[
        F.sum(F.col(c).isNull().cast("int")).alias(f"null_{c}")
        for c in silver_df.columns
    ],
    F.sum(
        F.when(F.col("code") == "", 1).otherwise(0)
    ).alias("blank_codes"),
    F.sum(
        F.when(F.col("long_description") == "", 1).otherwise(0)
    ).alias("blank_long_descriptions"),
    F.sum(
        F.when(F.col("short_description") == "", 1).otherwise(0)
    ).alias("blank_short_descriptions")
)

display(quality_df)

# COMMAND ----------

# Inspect HCPCS categories and record distribution
category_profile = (
    silver_df
    .groupBy("category")
    .agg(
        F.count("*").alias("records"),
        F.countDistinct("code").alias("distinct_codes")
    )
    .orderBy("category")
)

display(category_profile)

# COMMAND ----------

# Verify that all Bronze rows are preserved
bronze_count = bronze_df.count()
silver_count = silver_df.count()

print("Bronze rows:", bronze_count)
print("Silver rows:", silver_count)

assert bronze_count == silver_count, "Bronze–Silver row-count mismatch"

# COMMAND ----------

# Validate HCPCS keys and record preservation
quality = quality_df.first().asDict()

assert duplicate_count == 0, f"Duplicate HCPCS codes: {duplicate_count}"
assert quality["null_code"] == 0, "Missing HCPCS codes"
assert quality["blank_codes"] == 0, "Blank HCPCS codes"
assert bronze_count == silver_count, "Record counts do not match"

print("All critical Silver quality checks passed.")

# COMMAND ----------

# Check whether the Silver HCPCS dictionary already exists
target_exists = spark.catalog.tableExists(TARGET_TABLE)

print("Target table already exists:", target_exists)
print("Target:", TARGET_TABLE)

# COMMAND ----------

# Create the validated Silver HCPCS dictionary without overwrite
assert not spark.catalog.tableExists(TARGET_TABLE), (
    f"Target already exists; publication stopped: {TARGET_TABLE}"
)

silver_df.write.format("delta").saveAsTable(TARGET_TABLE)

print("Published:", TARGET_TABLE)

# COMMAND ----------

# Verify the published HCPCS dictionary
published_df = spark.table(TARGET_TABLE)

published_count = published_df.count()

print("Bronze rows:", bronze_count)
print("Silver rows:", silver_count)
print("Published rows:", published_count)

assert published_count == silver_count, "Published row-count mismatch"
assert published_df.columns == silver_df.columns, "Published columns differ"

display(published_df.limit(20))

print("Silver d_hcpcs table verified successfully.")

# COMMAND ----------

