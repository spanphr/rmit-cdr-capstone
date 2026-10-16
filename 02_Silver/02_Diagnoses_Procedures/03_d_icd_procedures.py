# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — ICD Procedure Dictionary (`d_icd_procedures`)
# MAGIC
# MAGIC **Source:** `cdr_mimiciv.bronze.d_icd_procedures`  
# MAGIC **Target:** `cdr_mimiciv.silver_v2.d_icd_procedures`
# MAGIC
# MAGIC **Objective:** Standardize the ICD procedure reference dictionary while preserving ICD-9 and ICD-10 distinctions and original procedure descriptions.
# MAGIC
# MAGIC **Expected grain:** One row per ICD procedure code and ICD version (`icd_code`, `icd_version`).
# MAGIC
# MAGIC **Silver rules:** Preserve all source records and clinical meaning, standardize data types and whitespace, validate uniqueness and completeness, and reconcile Bronze–Silver counts. No imputation, automatic deduplication or analytical aggregation.

# COMMAND ----------

# Import Spark functions
from pyspark.sql import functions as F

# COMMAND ----------

# Configure Bronze source and Silver target
CATALOG = "cdr_mimiciv"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver_v2"

SOURCE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.d_icd_procedures"
TARGET_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.d_icd_procedures"

# COMMAND ----------

# Read and inspect the ICD procedure dictionary
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
        for c in ["icd_code", "icd_version", "long_title"]
    ]
)

display(bronze_profile)

# COMMAND ----------

# Standardize procedure codes, ICD versions and descriptions
silver_df = bronze_df.select(
    F.trim(F.col("icd_code").cast("string")).alias("icd_code"),
    F.col("icd_version").cast("int").alias("icd_version"),
    F.trim(F.col("long_title").cast("string")).alias("long_title")
)

display(silver_df.limit(20))
silver_df.printSchema()

# COMMAND ----------

# Check uniqueness of ICD procedure code-version combinations
duplicate_df = (
    silver_df
    .groupBy("icd_code", "icd_version")
    .count()
    .filter(F.col("count") > 1)
)

duplicate_count = duplicate_df.count()

print("Duplicate ICD procedure keys:", duplicate_count)
display(duplicate_df)

# COMMAND ----------

# Check missing procedure codes, versions and descriptions
quality_df = silver_df.select(
    F.sum(
        F.when(
            F.col("icd_code").isNull() |
            (F.col("icd_code") == ""), 1
        ).otherwise(0)
    ).alias("missing_icd_codes"),

    F.sum(
        F.col("icd_version").isNull().cast("int")
    ).alias("null_icd_versions"),

    F.sum(
        F.when(
            F.col("icd_version").isNotNull() &
            (~F.col("icd_version").isin(9, 10)), 1
        ).otherwise(0)
    ).alias("invalid_icd_versions"),

    F.sum(
        F.when(
            F.col("long_title").isNull() |
            (F.col("long_title") == ""), 1
        ).otherwise(0)
    ).alias("missing_descriptions")
)

display(quality_df)

# COMMAND ----------

# Verify that the Silver transformation preserves all Bronze rows
bronze_count = bronze_df.count()
silver_count = silver_df.count()

print("Bronze rows:", bronze_count)
print("Silver rows:", silver_count)

assert bronze_count == silver_count, "Bronze–Silver row-count mismatch"

# COMMAND ----------

# Stop publication if critical procedure dictionary checks fail
quality = quality_df.first().asDict()

assert duplicate_count == 0, f"Duplicate procedure keys: {duplicate_count}"
assert quality["missing_icd_codes"] == 0, "Missing procedure codes"
assert quality["null_icd_versions"] == 0, "Missing ICD versions"
assert quality["invalid_icd_versions"] == 0, "Invalid ICD versions"
assert quality["missing_descriptions"] == 0, "Missing procedure descriptions"
assert bronze_count == silver_count, "Record counts do not match"

print("All critical Silver quality checks passed.")

# COMMAND ----------

# Check whether the Silver procedure dictionary already exists
target_exists = spark.catalog.tableExists(TARGET_TABLE)

print("Target table already exists:", target_exists)
print("Target:", TARGET_TABLE)

# COMMAND ----------

# Create the validated Silver ICD procedure dictionary
assert not spark.catalog.tableExists(TARGET_TABLE), (
    f"Target already exists: {TARGET_TABLE}"
)

silver_df.write.format("delta").saveAsTable(TARGET_TABLE)

print("Published:", TARGET_TABLE)

# COMMAND ----------

# Verify the published table and record counts
published_df = spark.table(TARGET_TABLE)

published_count = published_df.count()

print("Bronze rows:", bronze_count)
print("Silver rows:", silver_count)
print("Published rows:", published_count)

assert published_count == silver_count, "Published row-count mismatch"
assert published_df.columns == silver_df.columns, "Published columns differ"

display(published_df.limit(20))

print("Silver d_icd_procedures table verified successfully.")

# COMMAND ----------

