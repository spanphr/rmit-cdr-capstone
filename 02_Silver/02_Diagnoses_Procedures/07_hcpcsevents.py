# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — HCPCS Events (`hcpcsevents`)
# MAGIC
# MAGIC **Source:** `cdr_mimiciv.bronze.hcpcsevents`  
# MAGIC **Target:** `cdr_mimiciv.silver_v2.hcpcsevents`
# MAGIC
# MAGIC **Objective:** Standardize patient-level HCPCS events while preserving patient identifiers, hospital admissions, sequence numbers, HCPCS codes and source descriptions.
# MAGIC
# MAGIC **Grain:** One source HCPCS event record. Repeated codes or sequence numbers will be investigated rather than automatically removed.
# MAGIC
# MAGIC **Silver rules:** Preserve every Bronze record and its clinical meaning, standardize data types and whitespace, validate data quality, and reconcile Bronze–Silver counts. No deletion, deduplication, filtering, imputation or aggregation.

# COMMAND ----------

# Import PySpark functions
from pyspark.sql import functions as F

# COMMAND ----------

# Configure source and target tables
CATALOG = "cdr_mimiciv"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver_v2"

SOURCE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.hcpcsevents"
TARGET_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.hcpcsevents"

# COMMAND ----------

# Load and inspect patient HCPCS event records
bronze_df = spark.table(SOURCE_TABLE)

bronze_df.printSchema()
display(bronze_df.limit(20))

print("Bronze rows:", bronze_df.count())

# COMMAND ----------

# Profile source records and missing values
bronze_profile = bronze_df.select(
    F.count("*").alias("total_rows"),
    *[
        F.sum(F.col(c).isNull().cast("int")).alias(f"null_{c}")
        for c in bronze_df.columns
    ]
)

display(bronze_profile)

# COMMAND ----------

# Standardize HCPCS event identifiers, dates, codes and descriptions
silver_df = bronze_df.select(
    F.col("subject_id").cast("long").alias("subject_id"),
    F.col("hadm_id").cast("long").alias("hadm_id"),
    F.col("chartdate").cast("date").alias("chartdate"),
    F.trim(F.col("hcpcs_cd").cast("string")).alias("hcpcs_cd"),
    F.col("seq_num").cast("int").alias("seq_num"),
    F.trim(F.col("short_description").cast("string")).alias("short_description")
)

silver_df.printSchema()
display(silver_df.limit(20))

# COMMAND ----------

# Check exact duplicate HCPCS events without removing any records
duplicate_df = (
    silver_df
    .groupBy(*silver_df.columns)
    .count()
    .filter(F.col("count") > 1)
)

duplicate_count = duplicate_df.count()

print("Exact duplicate HCPCS event groups:", duplicate_count)
display(duplicate_df.limit(20))

# COMMAND ----------

# Check missing event identifiers, codes and optional fields
quality_df = silver_df.select(
    *[
        F.sum(F.col(c).isNull().cast("int")).alias(f"null_{c}")
        for c in silver_df.columns
    ],
    F.sum(
        F.when(F.col("hcpcs_cd") == "", 1).otherwise(0)
    ).alias("blank_hcpcs_codes"),
    F.sum(
        F.when(F.col("short_description") == "", 1).otherwise(0)
    ).alias("blank_descriptions")
)

display(quality_df)

# COMMAND ----------

# Check which event codes match the HCPCS reference dictionary
hcpcs_reference_df = (
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.d_hcpcs")
    .select(F.col("code").alias("reference_code"))
    .distinct()
)

code_coverage_df = (
    silver_df
    .select("hcpcs_cd")
    .distinct()
    .join(
        hcpcs_reference_df,
        F.col("hcpcs_cd") == F.col("reference_code"),
        "left"
    )
    .select(
        "hcpcs_cd",
        F.col("reference_code").isNotNull().alias("found_in_dictionary")
    )
)

display(
    code_coverage_df
    .groupBy("found_in_dictionary")
    .count()
)

# COMMAND ----------

# Confirm every Bronze record is preserved by row count
bronze_count = bronze_df.count()
silver_count = silver_df.count()

print("Bronze rows:", bronze_count)
print("Silver rows:", silver_count)

assert bronze_count == silver_count, "Bronze–Silver row-count mismatch"

# COMMAND ----------

# Validate required identifiers, HCPCS codes and row counts
quality = quality_df.first().asDict()

required_checks = [
    "null_subject_id",
    "null_hadm_id",
    "null_hcpcs_cd",
    "blank_hcpcs_codes"
]

issues = {k: quality[k] for k in required_checks if quality[k] != 0}

assert not issues, f"Critical HCPCS event quality issues: {issues}"
assert bronze_count == silver_count, "Record counts do not match"

print("All critical Silver quality checks passed.")
print("Exact duplicate groups (review only):", duplicate_count)

# COMMAND ----------

# Check whether the Silver HCPCS events table already exists
target_exists = spark.catalog.tableExists(TARGET_TABLE)

print("Target table already exists:", target_exists)
print("Target:", TARGET_TABLE)

# COMMAND ----------

# Create the validated HCPCS events table without overwrite
assert not spark.catalog.tableExists(TARGET_TABLE), (
    f"Target already exists; publication stopped: {TARGET_TABLE}"
)

assert duplicate_count == 0, (
    "Exact duplicate HCPCS events require review before publication"
)

silver_df.write.format("delta").saveAsTable(TARGET_TABLE)

print("Published:", TARGET_TABLE)

# COMMAND ----------

# Verify published HCPCS event records
published_df = spark.table(TARGET_TABLE)

published_count = published_df.count()

print("Bronze rows:", bronze_count)
print("Silver rows:", silver_count)
print("Published rows:", published_count)

assert published_count == silver_count, "Published row-count mismatch"
assert published_df.columns == silver_df.columns, "Published columns differ"

display(published_df.limit(20))

print("Silver hcpcsevents table verified successfully.")

# COMMAND ----------

