# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Patient ICD Procedures (`procedures_icd`)
# MAGIC
# MAGIC **Source:** `cdr_mimiciv.bronze.procedures_icd`  
# MAGIC **Target:** `cdr_mimiciv.silver_v2.procedures_icd`
# MAGIC
# MAGIC **Objective:** Standardize patient-level ICD procedure records while preserving patient identifiers, admission relationships, procedure dates, sequence numbers, and ICD-9/ICD-10 distinctions.
# MAGIC
# MAGIC **Expected grain:** One recorded procedure assignment per patient, admission and procedure sequence. Uniqueness will be assessed using the available procedure identifiers and ICD coding fields.
# MAGIC
# MAGIC **Silver rules:** Preserve all source records and clinical meaning, standardize data types, validate completeness and duplicate assignments, and reconcile Bronze–Silver counts. No imputation, automatic deduplication or analytical aggregation.

# COMMAND ----------

# Import PySpark functions
from pyspark.sql import functions as F

# COMMAND ----------

# Configure source and target tables
CATALOG = "cdr_mimiciv"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver_v2"

SOURCE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.procedures_icd"
TARGET_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.procedures_icd"

# COMMAND ----------

# Load and inspect patient ICD procedure records
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
        for c in [
            "subject_id", "hadm_id", "seq_num",
            "chartdate", "icd_code", "icd_version"
        ]
    ]
)

display(bronze_profile)

# COMMAND ----------

# Standardize identifiers, procedure dates and ICD coding fields
silver_df = bronze_df.select(
    F.col("subject_id").cast("long").alias("subject_id"),
    F.col("hadm_id").cast("long").alias("hadm_id"),
    F.col("seq_num").cast("int").alias("seq_num"),
    F.col("chartdate").cast("date").alias("chartdate"),
    F.trim(F.col("icd_code").cast("string")).alias("icd_code"),
    F.col("icd_version").cast("int").alias("icd_version")
)

display(silver_df.limit(20))
silver_df.printSchema()

# COMMAND ----------

# Check exact duplicate procedure records without removing any rows
duplicate_df = (
    silver_df
    .groupBy(
        "subject_id", "hadm_id", "seq_num",
        "chartdate", "icd_code", "icd_version"
    )
    .count()
    .filter(F.col("count") > 1)
)

duplicate_count = duplicate_df.count()

print("Exact duplicate procedure groups:", duplicate_count)
display(duplicate_df)

# COMMAND ----------

# Check missing identifiers, procedure dates and invalid ICD versions
quality_df = silver_df.select(
    *[
        F.sum(F.col(c).isNull().cast("int")).alias(f"null_{c}")
        for c in [
            "subject_id", "hadm_id", "seq_num",
            "chartdate", "icd_code", "icd_version"
        ]
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

# Confirm the transformation preserves every Bronze record
bronze_count = bronze_df.count()
silver_count = silver_df.count()

print("Bronze rows:", bronze_count)
print("Silver rows:", silver_count)

assert bronze_count == silver_count, "Bronze–Silver row-count mismatch"

# COMMAND ----------

# Stop publication if critical procedure quality checks fail
quality = quality_df.first().asDict()

assert duplicate_count == 0, f"Duplicate procedure groups: {duplicate_count}"
assert all(value == 0 for value in quality.values()), f"Quality issues: {quality}"
assert bronze_count == silver_count, "Record counts do not match"

print("All critical Silver quality checks passed.")

# COMMAND ----------

# Check whether the Silver procedure table already exists
target_exists = spark.catalog.tableExists(TARGET_TABLE)

print("Target table already exists:", target_exists)
print("Target:", TARGET_TABLE)

# COMMAND ----------

# Create the Silver table only if it does not already exist
assert not spark.catalog.tableExists(TARGET_TABLE), (
    f"Target already exists; publication stopped: {TARGET_TABLE}"
)

silver_df.write.format("delta").saveAsTable(TARGET_TABLE)

print("Published:", TARGET_TABLE)

# COMMAND ----------

# Verify published records against the validated Silver DataFrame
published_df = spark.table(TARGET_TABLE)

published_count = published_df.count()

print("Bronze rows:", bronze_count)
print("Silver rows:", silver_count)
print("Published rows:", published_count)

assert published_count == silver_count, "Published row-count mismatch"
assert published_df.columns == silver_df.columns, "Published columns differ"

display(published_df.limit(20))

print("Silver procedures_icd table verified successfully.")

# COMMAND ----------

