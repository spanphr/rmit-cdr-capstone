# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Electronic Medication Administration Details (`emar_detail`)
# MAGIC
# MAGIC **Source:** `cdr_mimiciv.bronze.emar_detail`  
# MAGIC **Target:** `cdr_mimiciv.silver_v2.emar_detail`
# MAGIC
# MAGIC **Objective:** Standardize detailed electronic medication administration records while preserving medication administration identifiers, dose and route information, administration details, and Bronze ingestion metadata.
# MAGIC
# MAGIC **Grain:** One source eMAR detail record. Multiple detail records may belong to the same medication administration event.
# MAGIC
# MAGIC **Silver rules:** Preserve every source record, column, and clinical value. Apply data type conversions only when necessary and validated. Do not delete, filter, deduplicate, impute, aggregate, or automatically correct records.
# MAGIC
# MAGIC **Data quality:** Inspect missing values, exact duplicates, identifiers, administration details, and Bronze–Silver row counts. Document anomalies without altering source data.
# MAGIC
# MAGIC **Publication:** Create the Delta table only after validation. Never overwrite an existing Silver table.
# MAGIC
# MAGIC **Source-data quality finding:** 4,861 exact duplicate groups were
# MAGIC identified across all 36 columns. Each group contains two records,
# MAGIC representing 9,722 records in duplicate groups and 4,861 additional
# MAGIC duplicate occurrences. All records are retained without deduplication.
# MAGIC
# MAGIC **Missing-value findings:** The source contains 42,808,593 NULL
# MAGIC `parent_field_ordinal` values, 44,569,078 NULL `administration_type`
# MAGIC values, 45,761,669 NULL `dose_due` values, 44,955,793 NULL
# MAGIC `dose_due_unit` values, and 46,269,155 NULL `dose_given` values.
# MAGIC All missing values are retained without imputation or record removal.

# COMMAND ----------

# Import PySpark functions
from pyspark.sql import functions as F

# COMMAND ----------

# Configure Bronze source and Silver target
CATALOG = "cdr_mimiciv"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver_v2"

SOURCE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.emar_detail"
TARGET_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.emar_detail"

print("Source:", SOURCE_TABLE)
print("Target:", TARGET_TABLE)

# COMMAND ----------

# Read all eMAR detail records without modifying data
bronze_df = spark.table(SOURCE_TABLE)

bronze_df.printSchema()
display(bronze_df.limit(20))

print("Bronze rows:", bronze_df.count())

# COMMAND ----------

# Inspect source columns, data types, and nullability
schema_df = spark.createDataFrame(
    [
        (field.name, field.dataType.simpleString(), field.nullable)
        for field in bronze_df.schema.fields
    ],
    ["column_name", "bronze_type", "nullable"]
)

display(schema_df)

# COMMAND ----------

# Preserve every eMAR detail record and its existing data types
silver_df = bronze_df

silver_df.printSchema()

# COMMAND ----------

# Identify exact duplicate records without removing them
duplicate_df = (
    silver_df
    .groupBy(*silver_df.columns)
    .count()
    .filter(F.col("count") > 1)
)

duplicate_count = duplicate_df.count()

print("Exact duplicate eMAR detail groups:", duplicate_count)
display(duplicate_df.limit(20))

# COMMAND ----------

# Summarize exact duplicate groups without removing records
duplicate_summary_df = duplicate_df.select(
    F.count("*").alias("duplicate_groups"),
    F.sum("count").alias("records_in_duplicate_groups"),
    F.sum(F.col("count") - 1).alias("additional_duplicate_occurrences"),
    F.max("count").alias("maximum_group_size")
)

display(duplicate_summary_df)

# COMMAND ----------

# Profile missing values across all 36 source columns
null_profile_df = silver_df.select(
    *[
        F.sum(F.col(c).isNull().cast("int")).alias(c)
        for c in silver_df.columns
    ]
)

display(null_profile_df)

# COMMAND ----------

# Validate patient and eMAR detail identifiers
critical_columns = [
    "subject_id",
    "emar_id",
    "emar_seq"
]

critical_null_df = silver_df.select(
    *[
        F.sum(F.col(c).isNull().cast("int")).alias(f"null_{c}")
        for c in critical_columns
    ]
)

display(critical_null_df)

# COMMAND ----------

# Inspect medication dose and administration fields without altering values
detail_columns = [
    "administration_type",
    "dose_due",
    "dose_due_unit",
    "dose_given",
    "dose_given_unit",
    "complete_dose_not_given",
    "product_amount_given"
]

detail_quality_df = silver_df.select(
    *[
        F.sum(F.col(c).isNull().cast("int")).alias(f"null_{c}")
        for c in detail_columns
        if c in silver_df.columns
    ]
)

display(detail_quality_df)

# COMMAND ----------

# Confirm every Bronze record is retained by row count
bronze_count = bronze_df.count()
silver_count = silver_df.count()

print("Bronze rows:", bronze_count)
print("Silver rows:", silver_count)

assert bronze_count == silver_count, "Bronze–Silver row-count mismatch"

# COMMAND ----------

# Check source preservation and report critical data-quality findings
critical_nulls = critical_null_df.first().asDict()

print("Critical NULL counts:", critical_nulls)
print("Exact duplicate groups:", duplicate_count)

assert bronze_count == silver_count, "Record counts do not match"
assert silver_df.schema == bronze_df.schema, "Source schema changed"

print("Bronze–Silver row count and schema checks passed.")

if any(v > 0 for v in critical_nulls.values()):
    print("WARNING: Missing critical identifiers require review.")

if duplicate_count > 0:
    print("WARNING: Exact duplicate records require review.")

# COMMAND ----------

# Check whether the Silver target already exists
target_exists = spark.catalog.tableExists(TARGET_TABLE)

print("Target table already exists:", target_exists)
print("Target:", TARGET_TABLE)

# COMMAND ----------

# Publish all eMAR detail records, including documented exact duplicates
assert bronze_count == silver_count, (
    "Bronze–Silver row-count mismatch"
)

assert silver_df.schema == bronze_df.schema, (
    "Bronze–Silver schema mismatch"
)

assert duplicate_count == 4861, (
    "Duplicate group count changed; review before publication"
)

assert not any(
    value > 0 for value in critical_nulls.values()
), "Missing critical identifiers require review"

assert not spark.catalog.tableExists(TARGET_TABLE), (
    f"Target already exists; publication stopped: {TARGET_TABLE}"
)

silver_df.write.format("delta").saveAsTable(TARGET_TABLE)

print("Published:", TARGET_TABLE)
print("Preserved exact duplicate groups:", duplicate_count)

# COMMAND ----------

# Verify published record counts and schema
published_df = spark.table(TARGET_TABLE)
published_count = published_df.count()

print("Bronze rows:", bronze_count)
print("Silver rows:", silver_count)
print("Published rows:", published_count)

assert published_count == bronze_count, "Published row-count mismatch"
assert published_df.schema == silver_df.schema, "Published schema mismatch"

print("Silver eMAR detail table verified successfully.")

# COMMAND ----------

