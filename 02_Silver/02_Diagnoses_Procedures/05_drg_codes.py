# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Diagnosis-Related Groups (`drgcodes`)
# MAGIC
# MAGIC **Source:** `cdr_mimiciv.bronze.drgcodes`  
# MAGIC **Target:** `cdr_mimiciv.silver_v2.drgcodes`
# MAGIC
# MAGIC **Objective:** Standardize DRG assignments while preserving patient identifiers, hospital admissions, DRG systems, codes, descriptions and severity/mortality information.
# MAGIC
# MAGIC **Grain:** One source DRG assignment record. Multiple DRG records per admission are permitted; no uniqueness constraint is assumed without validating the data.
# MAGIC
# MAGIC **Silver rules:** Preserve all records and clinical meaning, standardize data types and whitespace, validate data quality, and reconcile Bronze–Silver counts. No automatic deletion, deduplication, imputation, filtering or aggregation.

# COMMAND ----------

# Import PySpark functions
from pyspark.sql import functions as F

# COMMAND ----------

# Configure Bronze source and Silver target
CATALOG = "cdr_mimiciv"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver_v2"

SOURCE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.drgcodes"
TARGET_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.drgcodes"

# COMMAND ----------

# Load and inspect DRG assignment records
bronze_df = spark.table(SOURCE_TABLE)

bronze_df.printSchema()
display(bronze_df.limit(20))

print("Bronze rows:", bronze_df.count())

# COMMAND ----------

# Profile row counts and missing values
bronze_profile = bronze_df.select(
    F.count("*").alias("total_rows"),
    *[
        F.sum(F.col(c).isNull().cast("int")).alias(f"null_{c}")
        for c in bronze_df.columns
    ]
)

display(bronze_profile)

# COMMAND ----------

# Standardize DRG identifiers, classifications and descriptions
silver_df = bronze_df.select(
    F.col("subject_id").cast("long").alias("subject_id"),
    F.col("hadm_id").cast("long").alias("hadm_id"),
    F.trim(F.col("drg_type").cast("string")).alias("drg_type"),
    F.trim(F.col("drg_code").cast("string")).alias("drg_code"),
    F.trim(F.col("description").cast("string")).alias("description"),
    F.col("drg_severity").cast("int").alias("drg_severity"),
    F.col("drg_mortality").cast("int").alias("drg_mortality")
)

silver_df.printSchema()
display(silver_df.limit(20))

# COMMAND ----------

# Inspect exact duplicate DRG records without removing them
duplicate_df = (
    silver_df
    .groupBy(*silver_df.columns)
    .count()
    .filter(F.col("count") > 1)
)

duplicate_count = duplicate_df.count()

print("Exact duplicate DRG groups:", duplicate_count)
display(duplicate_df.limit(20))

# COMMAND ----------

# Check missing identifiers, DRG codes and optional clinical fields
quality_df = silver_df.select(
    *[
        F.sum(F.col(c).isNull().cast("int")).alias(f"null_{c}")
        for c in silver_df.columns
    ],
    F.sum(
        F.when(F.col("drg_type") == "", 1).otherwise(0)
    ).alias("blank_drg_types"),
    F.sum(
        F.when(F.col("drg_code") == "", 1).otherwise(0)
    ).alias("blank_drg_codes"),
    F.sum(
        F.when(F.col("description") == "", 1).otherwise(0)
    ).alias("blank_descriptions")
)

display(quality_df)

# COMMAND ----------

# Inspect DRG system distribution and severity/mortality availability
drg_profile = (
    silver_df
    .groupBy("drg_type")
    .agg(
        F.count("*").alias("records"),
        F.countDistinct("hadm_id").alias("admissions"),
        F.sum(F.col("drg_severity").isNull().cast("int")).alias("null_severity"),
        F.sum(F.col("drg_mortality").isNull().cast("int")).alias("null_mortality")
    )
    .orderBy("drg_type")
)

display(drg_profile)

# COMMAND ----------

# Confirm every Bronze row is represented in Silver
bronze_count = bronze_df.count()
silver_count = silver_df.count()

print("Bronze rows:", bronze_count)
print("Silver rows:", silver_count)

assert bronze_count == silver_count, "Bronze–Silver row-count mismatch"

# COMMAND ----------

# Validate required fields and row-count preservation
quality = quality_df.first().asDict()

required_checks = [
    "null_subject_id",
    "null_hadm_id",
    "null_drg_type",
    "null_drg_code",
    "blank_drg_types",
    "blank_drg_codes"
]

issues = {k: quality[k] for k in required_checks if quality[k] != 0}

assert not issues, f"Critical DRG quality issues: {issues}"
assert bronze_count == silver_count, "Record counts do not match"

print("All critical Silver quality checks passed.")
print("Exact duplicate groups (review only):", duplicate_count)

# COMMAND ----------

# Check whether the Silver DRG table already exists
target_exists = spark.catalog.tableExists(TARGET_TABLE)

print("Target table already exists:", target_exists)
print("Target:", TARGET_TABLE)

# COMMAND ----------

# Create the validated Silver DRG table without overwrite
assert not spark.catalog.tableExists(TARGET_TABLE), (
    f"Target already exists; publication stopped: {TARGET_TABLE}"
)

assert duplicate_count == 0, (
    "Exact duplicate DRG records require review before publication"
)

silver_df.write.format("delta").saveAsTable(TARGET_TABLE)

print("Published:", TARGET_TABLE)

# COMMAND ----------

# Verify published DRG records and table structure
published_df = spark.table(TARGET_TABLE)

published_count = published_df.count()

print("Bronze rows:", bronze_count)
print("Silver rows:", silver_count)
print("Published rows:", published_count)

assert published_count == silver_count, "Published row-count mismatch"
assert published_df.columns == silver_df.columns, "Published columns differ"

display(published_df.limit(20))

print("Silver drgcodes table verified successfully.")

# COMMAND ----------

