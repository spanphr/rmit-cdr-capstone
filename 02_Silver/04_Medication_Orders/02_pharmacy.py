# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Pharmacy (`pharmacy`)
# MAGIC
# MAGIC **Source:** `cdr_mimiciv.bronze.pharmacy`  
# MAGIC **Target:** `cdr_mimiciv.silver_v2.pharmacy`
# MAGIC
# MAGIC **Objective:** Standardize pharmacy medication-order records while preserving patient and admission identifiers, pharmacy and order identifiers, medication details, dosing instructions, routes, frequencies, order statuses, timestamps and Bronze ingestion metadata.
# MAGIC
# MAGIC **Grain:** One source pharmacy record. Repeated pharmacy or order identifiers will be investigated rather than automatically removed.
# MAGIC
# MAGIC **Silver rules:** Preserve every Bronze record and its clinical meaning. Standardize supported data types, validate identifiers and timestamps, inspect exact duplicates, and reconcile Bronze–Silver counts. No deletion, filtering, deduplication, imputation or aggregation.
# MAGIC
# MAGIC **Data quality:** Missing values, repeated records and unusual medication timestamps will be documented and investigated. Source anomalies will not be automatically corrected.
# MAGIC
# MAGIC **Source-data quality finding:** 717,515 pharmacy records
# MAGIC (approximately 4.02%) have `stoptime` earlier than `starttime`.
# MAGIC The same anomaly count was confirmed in Bronze and Silver.
# MAGIC The minimum reversal is 1 hour, the median is 3 hours, and the
# MAGIC maximum is 26,297,458 hours. All source timestamps and records
# MAGIC are preserved without correction.

# COMMAND ----------

# Import PySpark functions
from pyspark.sql import functions as F

# COMMAND ----------

# Configure Bronze source and Silver target
CATALOG = "cdr_mimiciv"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver_v2"

SOURCE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.pharmacy"
TARGET_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.pharmacy"

# COMMAND ----------

# Load and inspect pharmacy medication-order records
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

# Detect identifier and timestamp values that would fail conversion
cast_checks = {
    "subject_id": "bigint",
    "hadm_id": "bigint",
    "pharmacy_id": "bigint",
    "poe_seq": "bigint",
    "starttime": "timestamp",
    "stoptime": "timestamp"
}

available_cast_checks = {
    c: dtype for c, dtype in cast_checks.items()
    if c in bronze_df.columns
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
        for c, dtype in available_cast_checks.items()
    ]
)

display(cast_quality_df)

# COMMAND ----------

# Preserve all columns and standardize supported identifiers and timestamps
silver_df = bronze_df

long_columns = [
    "subject_id", "hadm_id", "pharmacy_id", "poe_seq"
]

timestamp_columns = [
    "starttime", "stoptime"
]

for c in long_columns:
    if c in silver_df.columns:
        silver_df = silver_df.withColumn(
            c, F.col(c).cast("long")
        )

for c in timestamp_columns:
    if c in silver_df.columns:
        silver_df = silver_df.withColumn(
            c, F.col(c).cast("timestamp")
        )

silver_df.printSchema()
display(silver_df.limit(20))

# COMMAND ----------

# Inspect exact duplicate pharmacy records without removing them
duplicate_df = (
    silver_df
    .groupBy(*silver_df.columns)
    .count()
    .filter(F.col("count") > 1)
)

duplicate_count = duplicate_df.count()

print("Exact duplicate pharmacy groups:", duplicate_count)
display(duplicate_df.limit(20))

# COMMAND ----------

# Profile missing important pharmacy identifiers and medication fields
critical_columns = [
    "subject_id", "hadm_id", "pharmacy_id",
    "poe_id", "starttime", "stoptime", "medication"
]

available_critical_columns = [
    c for c in critical_columns if c in silver_df.columns
]

quality_df = silver_df.select(
    *[
        F.sum(F.col(c).isNull().cast("int")).alias(f"null_{c}")
        for c in available_critical_columns
    ]
)

display(quality_df)

# COMMAND ----------

# Inspect pharmacy orders with stop times earlier than start times
if {"starttime", "stoptime"}.issubset(silver_df.columns):
    invalid_time_count = silver_df.filter(
        F.col("starttime").isNotNull() &
        F.col("stoptime").isNotNull() &
        (F.col("stoptime") < F.col("starttime"))
    ).count()
else:
    invalid_time_count = None

print("Pharmacy orders with stoptime before starttime:", invalid_time_count)

# COMMAND ----------

# Verify that all Bronze records are preserved by row count
bronze_count = bronze_df.count()
silver_count = silver_df.count()

print("Bronze rows:", bronze_count)
print("Silver rows:", silver_count)

assert bronze_count == silver_count, "Bronze–Silver row-count mismatch"

# COMMAND ----------

# Validate required identifiers and record preservation
quality = quality_df.first().asDict()
cast_quality = cast_quality_df.first().asDict()

required_columns = ["subject_id", "hadm_id"]

required_checks = [
    f"null_{c}" for c in required_columns
    if c in available_critical_columns
]

issues = {
    k: quality[k]
    for k in required_checks
    if quality[k] != 0
}

cast_issues = {
    k: v for k, v in cast_quality.items()
    if v != 0
}

assert not issues, f"Critical pharmacy quality issues: {issues}"
assert not cast_issues, f"Unsafe type conversions: {cast_issues}"
assert bronze_count == silver_count, "Record counts do not match"

print("All critical Silver quality checks passed.")
print("Exact duplicate groups (review only):", duplicate_count)
print("Invalid timestamp ranges (review only):", invalid_time_count)

# COMMAND ----------

# Check whether the Silver pharmacy table already exists
target_exists = spark.catalog.tableExists(TARGET_TABLE)

print("Target table already exists:", target_exists)
print("Target:", TARGET_TABLE)

# COMMAND ----------

# Compare reversed pharmacy timestamps in Bronze and Silver
bronze_invalid_times = bronze_df.filter(
    F.expr("try_cast(starttime AS timestamp)").isNotNull() &
    F.expr("try_cast(stoptime AS timestamp)").isNotNull() &
    (
        F.expr("try_cast(stoptime AS timestamp)") <
        F.expr("try_cast(starttime AS timestamp)")
    )
).count()

silver_invalid_times = silver_df.filter(
    F.col("starttime").isNotNull() &
    F.col("stoptime").isNotNull() &
    (F.col("stoptime") < F.col("starttime"))
).count()

print("Bronze reversed timestamps:", bronze_invalid_times)
print("Silver reversed timestamps:", silver_invalid_times)

# COMMAND ----------

# Summarize reversed pharmacy intervals without modifying records
time_review_df = (
    silver_df
    .filter(
        F.col("starttime").isNotNull() &
        F.col("stoptime").isNotNull() &
        (F.col("stoptime") < F.col("starttime"))
    )
    .withColumn(
        "difference_hours",
        (
            F.unix_timestamp("starttime") -
            F.unix_timestamp("stoptime")
        ) / 3600.0
    )
)

display(
    time_review_df.select(
        F.count("*").alias("affected_records"),
        F.min("difference_hours").alias("minimum_hours"),
        F.expr("percentile_approx(difference_hours, 0.5)").alias("median_hours"),
        F.max("difference_hours").alias("maximum_hours")
    )
)

# COMMAND ----------

# Publish all pharmacy records, preserving documented source anomalies
assert bronze_count == silver_count, "Bronze–Silver row-count mismatch"

assert duplicate_count == 0, "Exact duplicate groups require review"

assert bronze_invalid_times == silver_invalid_times, (
    "Bronze–Silver timestamp anomaly counts differ"
)

assert silver_invalid_times == 717515, (
    "Timestamp anomaly count changed; review before publication"
)

assert not spark.catalog.tableExists(TARGET_TABLE), (
    f"Target already exists; publication stopped: {TARGET_TABLE}"
)

silver_df.write.format("delta").saveAsTable(TARGET_TABLE)

print("Published:", TARGET_TABLE)
print("Preserved source timestamp anomalies:", silver_invalid_times)

# COMMAND ----------

# Verify published pharmacy records and columns
published_df = spark.table(TARGET_TABLE)
published_count = published_df.count()

print("Bronze rows:", bronze_count)
print("Silver rows:", silver_count)
print("Published rows:", published_count)

assert published_count == bronze_count, "Published row-count mismatch"
assert published_df.columns == silver_df.columns, "Published columns differ"

print("Silver pharmacy table verified successfully.")

# COMMAND ----------

