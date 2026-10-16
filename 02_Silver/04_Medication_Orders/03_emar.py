# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Electronic Medication Administration Record (`emar`)
# MAGIC
# MAGIC **Source:** `cdr_mimiciv.bronze.emar`  
# MAGIC **Target:** `cdr_mimiciv.silver_v2.emar`
# MAGIC
# MAGIC **Objective:** Standardize electronic medication administration records while preserving patient and admission identifiers, administration identifiers, medication information, event timing, event status, and Bronze ingestion metadata.
# MAGIC
# MAGIC **Grain:** One source eMAR record. Repeated administration identifiers or other apparent duplicates will be investigated, not automatically removed.
# MAGIC
# MAGIC **Silver rules:** Preserve every source record and all source columns. Convert supported identifier and timestamp fields only after validating that conversions are safe. Retain medication names, event descriptions, statuses, and source metadata without changing their meanings.
# MAGIC
# MAGIC **Data quality:** Validate identifiers, missing values, timestamp conversions, exact duplicates, and Bronze–Silver row counts. Document source anomalies without automatically correcting or removing records.
# MAGIC
# MAGIC **Publication:** Create a new Delta table only after validation. Never overwrite an existing table.
# MAGIC
# MAGIC **Source-data quality findings:** 1,417,390 eMAR records have a missing
# MAGIC `hadm_id`, and 23,249 records have a missing `scheduletime`.
# MAGIC These values are preserved as NULL without imputation or record removal.
# MAGIC The Bronze and Silver row counts and schemas match, and no exact
# MAGIC duplicate groups were identified.

# COMMAND ----------

# Import PySpark functions
from pyspark.sql import functions as F

# COMMAND ----------

# Configure Bronze source and Silver target
CATALOG = "cdr_mimiciv"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver_v2"

SOURCE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.emar"
TARGET_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.emar"

print("Source:", SOURCE_TABLE)
print("Target:", TARGET_TABLE)

# COMMAND ----------

# Load eMAR records without filtering or modifying data
bronze_df = spark.table(SOURCE_TABLE)

bronze_df.printSchema()
display(bronze_df.limit(20))

print("Bronze rows:", bronze_df.count())

# COMMAND ----------

# Inspect the actual Bronze schema before choosing Silver conversions
schema_df = spark.createDataFrame(
    [(field.name, field.dataType.simpleString(), field.nullable)
     for field in bronze_df.schema.fields],
    ["column_name", "bronze_type", "nullable"]
)

display(schema_df)

# COMMAND ----------

# Preserve all eMAR records, columns, and existing data types
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

print("Exact duplicate eMAR groups:", duplicate_count)
display(duplicate_df.limit(20))

# COMMAND ----------

# Profile missing values across all eMAR columns
null_profile_df = silver_df.select(
    *[
        F.sum(F.col(c).isNull().cast("int")).alias(c)
        for c in silver_df.columns
    ]
)

display(null_profile_df)

# COMMAND ----------

# Check required patient and administration identifiers
critical_columns = ["subject_id", "emar_id", "emar_seq"]

critical_null_df = silver_df.select(
    *[
        F.sum(F.col(c).isNull().cast("int")).alias(f"null_{c}")
        for c in critical_columns
    ]
)

display(critical_null_df)

# COMMAND ----------

# Profile eMAR timestamp completeness without changing event times
timestamp_quality_df = silver_df.select(
    F.count("*").alias("total_records"),
    F.sum(F.col("charttime").isNull().cast("int")).alias("null_charttime"),
    F.sum(F.col("scheduletime").isNull().cast("int")).alias("null_scheduletime"),
    F.sum(F.col("storetime").isNull().cast("int")).alias("null_storetime"),
    F.sum(
        (
            F.col("charttime").isNotNull() &
            F.col("scheduletime").isNotNull() &
            (F.col("charttime") < F.col("scheduletime"))
        ).cast("int")
    ).alias("charttime_before_scheduletime"),
    F.sum(
        (
            F.col("storetime").isNotNull() &
            F.col("charttime").isNotNull() &
            (F.col("storetime") < F.col("charttime"))
        ).cast("int")
    ).alias("storetime_before_charttime")
)

display(timestamp_quality_df)

# COMMAND ----------

# Verify that every Bronze record is retained by row count
bronze_count = bronze_df.count()
silver_count = silver_df.count()

print("Bronze rows:", bronze_count)
print("Silver rows:", silver_count)

assert bronze_count == silver_count, "Bronze–Silver row-count mismatch"

# COMMAND ----------

# Validate critical identifiers and preservation of all source records
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

# Check that the target does not already exist
target_exists = spark.catalog.tableExists(TARGET_TABLE)

print("Target table already exists:", target_exists)
print("Target:", TARGET_TABLE)

# COMMAND ----------

# Publish all eMAR records without overwrite
assert bronze_count == silver_count, "Bronze–Silver row-count mismatch"
assert silver_df.schema == bronze_df.schema, "Source schema changed"

assert not spark.catalog.tableExists(TARGET_TABLE), (
    f"Target already exists; publication stopped: {TARGET_TABLE}"
)

silver_df.write.format("delta").saveAsTable(TARGET_TABLE)

print("Published:", TARGET_TABLE)

# COMMAND ----------

# Verify published eMAR row count and schema
published_df = spark.table(TARGET_TABLE)
published_count = published_df.count()

print("Bronze rows:", bronze_count)
print("Silver rows:", silver_count)
print("Published rows:", published_count)

assert published_count == bronze_count, "Published row-count mismatch"
assert published_df.schema == silver_df.schema, "Published schema mismatch"

print("Silver eMAR table verified successfully.")

# COMMAND ----------

