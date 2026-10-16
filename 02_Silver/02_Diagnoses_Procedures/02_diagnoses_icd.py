# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Patient ICD Diagnoses (`diagnoses_icd`)
# MAGIC
# MAGIC **Source:** `cdr_mimiciv.bronze.diagnoses_icd`  
# MAGIC **Target:** `cdr_mimiciv.silver_v2.diagnoses_icd`
# MAGIC
# MAGIC **Objective:** Standardize patient-level ICD diagnosis records while preserving patient identifiers, hospital admission relationships, diagnosis sequence, and ICD-9/ICD-10 distinctions.
# MAGIC
# MAGIC **Expected grain:** One diagnosis assignment per patient, hospital admission and diagnosis sequence (`subject_id`, `hadm_id`, `seq_num`).
# MAGIC
# MAGIC **Silver rules:** Preserve clinical meaning and source records; standardize types and whitespace; validate key completeness, uniqueness and ICD versions; avoid imputation, unjustified deletion and analytical aggregation.

# COMMAND ----------

# Import PySpark functions
from pyspark.sql import functions as F

# COMMAND ----------

# Configure Bronze source and Silver target
CATALOG = "cdr_mimiciv"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver_v2"

SOURCE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.diagnoses_icd"
TARGET_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.diagnoses_icd"

# COMMAND ----------

# Load and inspect the source diagnosis records
bronze_df = spark.table(SOURCE_TABLE)

bronze_df.printSchema()
display(bronze_df.limit(20))
print("Bronze rows:", bronze_df.count())

# COMMAND ----------

# Profile record counts and missing values in source columns
bronze_profile = bronze_df.select(
    F.count("*").alias("total_rows"),
    *[
        F.sum(F.col(c).isNull().cast("int")).alias(f"null_{c}")
        for c in ["subject_id", "hadm_id", "seq_num", "icd_code", "icd_version"]
    ]
)

display(bronze_profile)

# COMMAND ----------

# Standardize data types and ICD code whitespace without dropping records
silver_df = bronze_df.select(
    F.col("subject_id").cast("long").alias("subject_id"),
    F.col("hadm_id").cast("long").alias("hadm_id"),
    F.col("seq_num").cast("int").alias("seq_num"),
    F.trim(F.col("icd_code").cast("string")).alias("icd_code"),
    F.col("icd_version").cast("int").alias("icd_version")
)

display(silver_df.limit(20))
silver_df.printSchema()

# COMMAND ----------

# Check duplicate patient diagnosis assignments using the full diagnosis key
duplicate_df = (
    silver_df
    .groupBy(
        "subject_id",
        "hadm_id",
        "seq_num",
        "icd_code",
        "icd_version"
    )
    .count()
    .filter(F.col("count") > 1)
)

duplicate_count = duplicate_df.count()

print("Duplicate full diagnosis records:", duplicate_count)
display(duplicate_df)

# COMMAND ----------

# Check missing identifiers, blank ICD codes and invalid ICD versions
quality_df = silver_df.select(
    *[
        F.sum(F.col(c).isNull().cast("int")).alias(f"null_{c}")
        for c in ["subject_id", "hadm_id", "seq_num", "icd_code", "icd_version"]
    ],
    F.sum(
        F.when(F.col("icd_code") == "", 1).otherwise(0)
    ).alias("blank_icd_codes"),
    F.sum(
        F.when(
            F.col("icd_version").isNotNull() &
            (~F.col("icd_version").isin(9, 10)), 1
        ).otherwise(0)
    ).alias("invalid_icd_versions")
)

display(quality_df)

# COMMAND ----------

# Confirm that Silver transformation preserves all source records
bronze_count = bronze_df.count()
silver_count = silver_df.count()

print("Bronze rows:", bronze_count)
print("Silver rows:", silver_count)

assert bronze_count == silver_count, "Bronze–Silver row-count mismatch"

# COMMAND ----------

# Validate diagnosis uniqueness, data quality and record preservation
quality = quality_df.first().asDict()

assert duplicate_count == 0, f"Duplicate diagnosis records: {duplicate_count}"
assert all(value == 0 for value in quality.values()), f"Quality issues: {quality}"
assert bronze_count == silver_count, "Bronze and Silver counts do not match"

print("All critical Silver quality checks passed.")

# COMMAND ----------

# Check whether the Silver target table already exists
target_exists = spark.catalog.tableExists(TARGET_TABLE)

print("Target table already exists:", target_exists)
print("Target:", TARGET_TABLE)

# COMMAND ----------

# Create the Silver table only if it does not already exist
assert not spark.catalog.tableExists(TARGET_TABLE), (
    f"Target already exists; publication stopped: {TARGET_TABLE}"
)

# Publish validated diagnosis records without overwrite mode
(
    silver_df.write
    .format("delta")
    .saveAsTable(TARGET_TABLE)
)

print("Published:", TARGET_TABLE)

# COMMAND ----------

# Verify the published Silver table and its record count
published_df = spark.table(TARGET_TABLE)

published_count = published_df.count()

print("Bronze rows:", bronze_count)
print("Silver rows:", silver_count)
print("Published rows:", published_count)

assert published_count == silver_count, "Published row-count mismatch"
assert published_df.columns == silver_df.columns, "Published columns differ"

display(published_df.limit(20))

print("Silver diagnoses_icd table verified successfully.")

# COMMAND ----------

