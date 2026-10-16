# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Provider Order Entry (`poe`)
# MAGIC
# MAGIC **Source:** `cdr_mimiciv.bronze.poe`  
# MAGIC **Target:** `cdr_mimiciv.silver_v2.poe`
# MAGIC
# MAGIC **Objective:** Prepare provider order entry records for the Silver layer while preserving patient and admission identifiers, order identifiers, order types, order timing, provider information, clinical order details and Bronze ingestion metadata.
# MAGIC
# MAGIC **Grain:** One source provider order entry record. Repeated order identifiers or apparent duplicate records will be investigated rather than automatically removed.
# MAGIC
# MAGIC **Silver rules:** Preserve every Bronze record and source column. Standardize data types only when necessary and validated. Do not delete, filter, deduplicate, impute, aggregate, or automatically correct clinical data.
# MAGIC
# MAGIC **Data quality:** Inspect missing values, exact duplicates, order identifiers, timestamps, and Bronze–Silver row counts. Document anomalies without changing the source data.
# MAGIC
# MAGIC **Publication:** Publish only after validation, using create-only Delta table creation. Never overwrite an existing Silver table.
# MAGIC
# MAGIC **Data quality findings:** All 52,212,109 POE records were preserved
# MAGIC during Silver transformation. No exact duplicate groups, missing
# MAGIC critical identifiers, unsafe type conversions, or NULL order
# MAGIC timestamps were detected. Order timestamps range from
# MAGIC 2105-10-04 to 2214-12-24. These deidentified timestamps are
# MAGIC preserved without correction.

# COMMAND ----------

# Import PySpark functions
from pyspark.sql import functions as F

# COMMAND ----------

# Configure Bronze source and Silver target
CATALOG = "cdr_mimiciv"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver_v2"

SOURCE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.poe"
TARGET_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.poe"

print("Source:", SOURCE_TABLE)
print("Target:", TARGET_TABLE)

# COMMAND ----------

# Load all provider order records without modifying them
bronze_df = spark.table(SOURCE_TABLE)

bronze_df.printSchema()
display(bronze_df.limit(20))

print("Bronze rows:", bronze_df.count())

# COMMAND ----------

# Inspect actual source columns, types, and nullability
schema_df = spark.createDataFrame(
    [
        (
            field.name,
            field.dataType.simpleString(),
            field.nullable
        )
        for field in bronze_df.schema.fields
    ],
    ["column_name", "bronze_type", "nullable"]
)

display(schema_df)

# COMMAND ----------

# Check for values that cannot be converted without information loss
cast_types = {
    "poe_seq": "bigint",
    "subject_id": "bigint",
    "hadm_id": "bigint",
    "ordertime": "timestamp"
}

cast_quality_df = bronze_df.select(
    *[
        F.sum(
            F.when(
                F.col(c).isNotNull() &
                F.expr(f"try_cast(`{c}` AS {dtype})").isNull(),
                1
            ).otherwise(0)
        ).alias(f"invalid_cast_{c}")
        for c, dtype in cast_types.items()
    ]
)

display(cast_quality_df)

# COMMAND ----------

# Preserve all source columns and safely standardize selected types
cast_issues = {
    k: v for k, v in cast_quality_df.first().asDict().items()
    if v != 0
}

assert not cast_issues, f"Unsafe conversions detected: {cast_issues}"

silver_df = bronze_df

for c in ["poe_seq", "subject_id", "hadm_id"]:
    silver_df = silver_df.withColumn(c, F.col(c).cast("long"))

silver_df = silver_df.withColumn(
    "ordertime", F.col("ordertime").cast("timestamp")
)

silver_df.printSchema()

# COMMAND ----------

# Identify exact duplicate orders without removing records
duplicate_df = (
    silver_df
    .groupBy(*silver_df.columns)
    .count()
    .filter(F.col("count") > 1)
)

duplicate_count = duplicate_df.count()

print("Exact duplicate POE groups:", duplicate_count)
display(duplicate_df.limit(20))

# COMMAND ----------

# Profile missing values across all POE columns
null_profile_df = silver_df.select(
    *[
        F.sum(F.col(c).isNull().cast("int")).alias(c)
        for c in silver_df.columns
    ]
)

display(null_profile_df)

# COMMAND ----------

# Check essential patient and order identifiers
critical_columns = ["poe_id", "poe_seq", "subject_id"]

critical_null_df = silver_df.select(
    *[
        F.sum(F.col(c).isNull().cast("int")).alias(f"null_{c}")
        for c in critical_columns
    ]
)

display(critical_null_df)

# COMMAND ----------

# Inspect missing order timestamps without modifying them
timestamp_quality_df = silver_df.select(
    F.count("*").alias("total_records"),
    F.sum(F.col("ordertime").isNull().cast("int")).alias("null_ordertime"),
    F.min("ordertime").alias("earliest_ordertime"),
    F.max("ordertime").alias("latest_ordertime")
)

display(timestamp_quality_df)

# COMMAND ----------

# Confirm that every Bronze row is retained
bronze_count = bronze_df.count()
silver_count = silver_df.count()

print("Bronze rows:", bronze_count)
print("Silver rows:", silver_count)

assert bronze_count == silver_count, "Bronze–Silver row-count mismatch"

# COMMAND ----------

# Validate safe conversions and source-record preservation
critical_nulls = critical_null_df.first().asDict()

print("Critical NULL counts:", critical_nulls)
print("Exact duplicate groups:", duplicate_count)
print("Unsafe casts:", cast_issues)

assert bronze_count == silver_count, "Record counts do not match"
assert not cast_issues, "Unsafe type conversions detected"

print("Bronze–Silver record-count and conversion checks passed.")

if any(v > 0 for v in critical_nulls.values()):
    print("WARNING: Missing critical identifiers require review.")

if duplicate_count > 0:
    print("WARNING: Exact duplicate records require review.")

# COMMAND ----------

# Check that the Silver target does not already exist
target_exists = spark.catalog.tableExists(TARGET_TABLE)

print("Target table already exists:", target_exists)
print("Target:", TARGET_TABLE)

# COMMAND ----------

# Publish validated POE records without overwriting an existing table
assert bronze_count == silver_count, "Bronze–Silver row-count mismatch"
assert not cast_issues, "Unsafe type conversions detected"
assert duplicate_count == 0, "Unexpected exact duplicate groups"
assert all(v == 0 for v in critical_nulls.values()), (
    "Missing critical identifiers require review"
)

assert not spark.catalog.tableExists(TARGET_TABLE), (
    f"Target already exists; publication stopped: {TARGET_TABLE}"
)

silver_df.write.format("delta").saveAsTable(TARGET_TABLE)

print("Published:", TARGET_TABLE)

# COMMAND ----------

# Verify published POE record counts and columns
published_df = spark.table(TARGET_TABLE)
published_count = published_df.count()

print("Bronze rows:", bronze_count)
print("Silver rows:", silver_count)
print("Published rows:", published_count)

assert published_count == bronze_count, "Published row-count mismatch"
assert published_df.schema == silver_df.schema, "Published schema mismatch"

print("Silver POE table verified successfully.")

# COMMAND ----------

