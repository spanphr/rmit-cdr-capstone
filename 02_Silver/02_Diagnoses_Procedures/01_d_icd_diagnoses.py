# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Hospital Procedure Events
# MAGIC
# MAGIC ## Source
# MAGIC
# MAGIC `cdr_mimiciv.bronze.procedureevents`
# MAGIC
# MAGIC ## Target
# MAGIC
# MAGIC `cdr_mimiciv.silver_v2.procedureevents`
# MAGIC
# MAGIC ## Grain
# MAGIC
# MAGIC One row represents one recorded hospital procedure event, identified by `orderid` where the source confirms its uniqueness.
# MAGIC
# MAGIC ## Silver objective
# MAGIC
# MAGIC Create a reusable and clinically interpretable procedure-event dataset while preserving patient, admission, procedure timing and care-delivery information.
# MAGIC
# MAGIC ## Engineering principles
# MAGIC
# MAGIC - Preserve patient and admission identifiers.
# MAGIC - Preserve original procedure item identifiers.
# MAGIC - Preserve procedure start and end timestamps.
# MAGIC - Preserve original procedure values and units.
# MAGIC - Preserve order identifiers and event status information.
# MAGIC - Validate procedure timing without automatically modifying records.
# MAGIC - Do not impute missing clinical timestamps or values.
# MAGIC - Do not remove clinically valid repeated procedures.
# MAGIC - Preserve references to relevant clinical dictionaries.
# MAGIC - Defer procedure duration, resource utilization and operational analytics to Gold.

# COMMAND ----------

# Import PySpark functions
from pyspark.sql import functions as F

# COMMAND ----------

# Configure catalog and layer schemas
CATALOG = "cdr_mimiciv"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver_v2"

# COMMAND ----------

# Configure source and target tables
TABLE = "d_icd_diagnoses"
SOURCE_TABLE = f"{BRONZE}.{TABLE}"
TARGET_TABLE = f"{SILVER}.{TABLE}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Bronze source inspection
# MAGIC
# MAGIC Read the original source table from the Bronze layer.
# MAGIC
# MAGIC Inspect the available records, column names and datatypes to understand the source structure before applying transformations.
# MAGIC
# MAGIC No source records are modified during this stage.

# COMMAND ----------

# Read the source Bronze table
bronze_df = spark.table(SOURCE_TABLE)

display(bronze_df.limit(20))

# COMMAND ----------

# Inspect source datatypes
bronze_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Source data profiling
# MAGIC
# MAGIC Evaluate source data quality before applying transformations.
# MAGIC
# MAGIC The profiling stage checks:
# MAGIC
# MAGIC - Total number of records.
# MAGIC - Missing values in source columns.
# MAGIC - Duplicate records based on the expected business grain.
# MAGIC - Unexpected or rescued source data.
# MAGIC - Validity of clinical coding systems and identifiers.
# MAGIC
# MAGIC Profiling results determine which Silver transformations are necessary. Records are not automatically removed or corrected without validated rules.

# COMMAND ----------

# Count source records
print("Bronze rows:", bronze_df.count())

# COMMAND ----------

# Count null values in every source column
null_profile = bronze_df.select(
    *[
        F.sum(
            F.when(F.col(f"`{c}`").isNull(), 1).otherwise(0)
        ).alias(c)
        for c in bronze_df.columns
    ]
)

display(null_profile)

# COMMAND ----------

# Check records containing rescued source data
if "_rescued_data" in bronze_df.columns:
    rescued_count = bronze_df.filter(
        F.col("_rescued_data").isNotNull()
    ).count()
    print("Rows with rescued data:", rescued_count)
else:
    print("No rescued-data column found")

# COMMAND ----------

# Identify duplicate ICD code-version combinations
duplicate_records = (
    bronze_df
    .groupBy("icd_code", "icd_version")
    .count()
    .filter(F.col("count") > 1)
)

display(duplicate_records)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver transformations
# MAGIC
# MAGIC Transform the Bronze source into a reusable, standardized Silver entity.
# MAGIC
# MAGIC Apply only justified, deterministic transformations, including:
# MAGIC
# MAGIC - Appropriate datatype casting.
# MAGIC - Safe standardization of identifiers and text fields.
# MAGIC - Preservation of original clinical codes and meanings.
# MAGIC - Preservation of source relationships and record granularity.
# MAGIC - Separation of ingestion metadata from clinically relevant fields.
# MAGIC
# MAGIC Do not impute missing clinical information, fabricate records or apply Gold-level analytical aggregations.

# COMMAND ----------

# Create the Silver ICD diagnosis dictionary
silver_df = (
    bronze_df
    .select(
        F.trim(F.col("icd_code").cast("string")).alias("icd_code"),
        F.col("icd_version").cast("int").alias("icd_version"),
        F.trim(F.col("long_title").cast("string")).alias("long_title")
    )
)

# Preview standardized Silver records
display(silver_df.limit(20))

# Inspect Silver schema
silver_df.printSchema()

# COMMAND ----------

# Identify duplicate ICD code-version combinations
duplicate_codes = (
    silver_df
    .groupBy("icd_code", "icd_version")
    .count()
    .filter(F.col("count") > 1)
)

# Display duplicate validation results
print("Duplicate ICD combinations:", duplicate_codes.count())
display(duplicate_codes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Silver Data Quality Validation
# MAGIC
# MAGIC Evaluate the completeness and validity of the standardized diagnosis dictionary.
# MAGIC
# MAGIC Checks performed:
# MAGIC - Missing or blank ICD codes.
# MAGIC - Missing ICD versions.
# MAGIC - Invalid ICD versions outside ICD-9 and ICD-10.
# MAGIC - Missing or blank diagnosis descriptions.
# MAGIC
# MAGIC No missing clinical information is fabricated or automatically corrected.

# COMMAND ----------

# Validate missing and invalid values in Silver ICD diagnosis columns
icd_quality = silver_df.select(
    F.sum(
        F.when(
            F.col("icd_code").isNull() |
            (F.col("icd_code") == ""), 1
        ).otherwise(0)
    ).alias("missing_icd_codes"),

    F.sum(
        F.when(
            F.col("icd_version").isNull(), 1
        ).otherwise(0)
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

# Display Silver quality results
display(icd_quality)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Bronze-to-Silver Record Reconciliation
# MAGIC
# MAGIC Compare Bronze and Silver record counts.
# MAGIC
# MAGIC Since this reference-table transformation does not intentionally remove records, the total number of records should remain unchanged.
# MAGIC
# MAGIC Any unexpected difference prevents publication.

# COMMAND ----------

# Compare Bronze and Silver record counts
bronze_count = bronze_df.count()
silver_count = silver_df.count()

print("Bronze rows:", bronze_count)
print("Silver rows:", silver_count)

# Stop processing if the record counts differ
assert bronze_count == silver_count, "Unexpected row-count change"

# COMMAND ----------

# Check missing values in Silver ICD diagnosis columns
null_check = silver_df.select(
    *[
        F.sum(
            F.when(F.col(c).isNull(), 1).otherwise(0)
        ).alias(c)
        for c in [
            "icd_code",
            "icd_version",
            "long_title"
        ]
    ]
)

display(null_check)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Final Silver Quality Gate
# MAGIC
# MAGIC Validate critical conditions before publishing the Silver table.
# MAGIC
# MAGIC Publication requirements:
# MAGIC - No duplicate ICD code-version combinations.
# MAGIC - No missing or blank ICD codes.
# MAGIC - No missing ICD versions.
# MAGIC - No invalid ICD versions.
# MAGIC - No missing or blank diagnosis descriptions.
# MAGIC - Bronze and Silver record counts match.
# MAGIC
# MAGIC Failed checks stop publication and require investigation.

# COMMAND ----------

# Collect data quality metrics
quality = icd_quality.first()

# Count duplicate ICD combinations
duplicate_count = duplicate_codes.count()

# Validate unique diagnosis identifiers
assert duplicate_count == 0, "Duplicate ICD code-version combinations found"

# Validate required ICD codes
assert quality["missing_icd_codes"] == 0, "Missing ICD codes found"

# Validate ICD versions
assert quality["null_icd_versions"] == 0, "Missing ICD versions found"
assert quality["invalid_icd_versions"] == 0, "Invalid ICD versions found"

# Validate diagnosis descriptions
assert quality["missing_descriptions"] == 0, "Missing diagnosis descriptions found"

# Confirm record-count preservation
assert bronze_count == silver_count, "Bronze and Silver counts do not match"

print("All Silver quality checks passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Publish Silver ICD Diagnosis Dictionary
# MAGIC
# MAGIC Write the validated Silver dataset as a managed Delta table in Unity Catalog.
# MAGIC
# MAGIC Target:
# MAGIC `cdr_mimiciv.silver_v2.d_icd_diagnoses`
# MAGIC
# MAGIC The Silver table provides a reusable diagnosis reference entity for downstream clinical analytics and Gold-layer development.

# COMMAND ----------

# Publish the validated ICD diagnosis dictionary to Silver
(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TARGET_TABLE)
)

# Confirm successful publication
print(f"Created: {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Published Silver Verification
# MAGIC
# MAGIC Read the published Delta table and verify:
# MAGIC
# MAGIC - The table is accessible through Unity Catalog.
# MAGIC - The published record count matches the validated Silver dataset.
# MAGIC - The published schema contains the expected columns.
# MAGIC - The data remains available for downstream Gold transformations.

# COMMAND ----------

# Read the published Silver table
published_df = spark.table(TARGET_TABLE)

# Verify published record count
published_count = published_df.count()

print("Bronze rows:", bronze_count)
print("Silver rows:", silver_count)
print("Published rows:", published_count)

# Confirm published data completeness
assert published_count == silver_count, "Published record count mismatch"

# Display published schema and sample records
published_df.printSchema()
display(published_df.limit(20))

print("Silver ICD diagnosis dictionary verified successfully.")

# COMMAND ----------

