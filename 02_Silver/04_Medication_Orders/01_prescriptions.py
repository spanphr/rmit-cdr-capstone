# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Prescriptions (`prescriptions`)
# MAGIC
# MAGIC **Source:** `cdr_mimiciv.bronze.prescriptions`  
# MAGIC **Target:** `cdr_mimiciv.silver_v2.prescriptions`
# MAGIC
# MAGIC **Objective:** Standardize prescription records while preserving patient and admission identifiers, medication orders, drug names, formulary codes, doses, routes, quantities, timing and other source clinical information.
# MAGIC
# MAGIC **Grain:** One source prescription record. Repeated medication orders or prescription identifiers will be investigated rather than automatically removed.
# MAGIC
# MAGIC **Silver rules:** Preserve every Bronze record and its clinical meaning. Standardize supported data types and non-clinical whitespace, validate identifiers and timestamps, inspect duplicates, and reconcile Bronze–Silver counts. No deletion, filtering, deduplication, imputation or aggregation.

# COMMAND ----------

# Import PySpark functions
from pyspark.sql import functions as F

# COMMAND ----------

# Configure Bronze source and Silver target
CATALOG = "cdr_mimiciv"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver_v2"

SOURCE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.prescriptions"
TARGET_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.prescriptions"

# COMMAND ----------

# Load and inspect prescription records
bronze_df = spark.table(SOURCE_TABLE)

bronze_df.printSchema()
display(bronze_df.limit(20))

print("Bronze rows:", bronze_df.count())

# COMMAND ----------

# Check whether typed conversions would lose non-NULL source values
cast_checks = {
    "subject_id": "bigint",
    "hadm_id": "bigint",
    "pharmacy_id": "bigint",
    "poe_seq": "bigint",
    "starttime": "timestamp",
    "stoptime": "timestamp"
}

cast_quality_df = bronze_df.select(
    *[
        F.sum(
            F.when(
                F.col(c).isNotNull()
                & F.expr(f"try_cast(`{c}` AS {dtype})").isNull(),
                1
            ).otherwise(0)
        ).alias(f"invalid_cast_{c}")
        for c, dtype in cast_checks.items()
    ]
)

display(cast_quality_df)

# COMMAND ----------

# Preserve all columns and standardize supported identifiers and timestamps
silver_df = bronze_df

long_columns = [
    "subject_id", "hadm_id", "pharmacy_id",
    "poe_seq"
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

# Inspect exact duplicate prescriptions without removing records
duplicate_df = (
    silver_df
    .groupBy(*silver_df.columns)
    .count()
    .filter(F.col("count") > 1)
)

duplicate_count = duplicate_df.count()

print("Exact duplicate prescription groups:", duplicate_count)
display(duplicate_df.limit(20))

# COMMAND ----------

# Profile missing prescription identifiers and medication information
critical_columns = [
    "subject_id", "hadm_id", "pharmacy_id",
    "poe_id", "starttime", "stoptime", "drug"
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

# Inspect prescriptions with stop times earlier than start times
if {"starttime", "stoptime"}.issubset(silver_df.columns):
    invalid_time_count = silver_df.filter(
        F.col("starttime").isNotNull() &
        F.col("stoptime").isNotNull() &
        (F.col("stoptime") < F.col("starttime"))
    ).count()
else:
    invalid_time_count = None

print("Prescriptions with stoptime before starttime:", invalid_time_count)

# COMMAND ----------

# Compare reversed timestamp ranges in Bronze and Silver
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

# Summarize reversed prescription time intervals without changing records
time_review_df = (
    silver_df
    .filter(
        F.col("starttime").isNotNull() &
        F.col("stoptime").isNotNull() &
        (F.col("stoptime") < F.col("starttime"))
    )
    .withColumn(
        "difference_hours",
        (F.unix_timestamp("starttime") - F.unix_timestamp("stoptime")) / 3600.0
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

# Confirm that the transformation preserves every Bronze row
bronze_count = bronze_df.count()
silver_count = silver_df.count()

print("Bronze rows:", bronze_count)
print("Silver rows:", silver_count)

assert bronze_count == silver_count, "Bronze–Silver row-count mismatch"

# COMMAND ----------

# Validate required identifiers and row-count preservation
quality = quality_df.first().asDict()

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

assert not issues, f"Critical prescription quality issues: {issues}"
assert bronze_count == silver_count, "Record counts do not match"

print("All critical Silver quality checks passed.")
print("Exact duplicate groups (review only):", duplicate_count)
print("Invalid timestamp ranges (review only):", invalid_time_count)

# COMMAND ----------

# Confirm that the Silver target does not already exist
print("Target table already exists:", spark.catalog.tableExists(TARGET_TABLE))
print("Target:", TARGET_TABLE)

# COMMAND ----------

# Publish all validated records, including documented source timestamp anomalies
assert bronze_count == silver_count, "Bronze–Silver row-count mismatch"

assert duplicate_count == 0, (
    "Exact duplicate prescriptions require review"
)

assert bronze_invalid_times == silver_invalid_times, (
    "Timestamp anomaly counts differ between Bronze and Silver"
)

assert not spark.catalog.tableExists(TARGET_TABLE), (
    f"Target already exists; publication stopped: {TARGET_TABLE}"
)

silver_df.write.format("delta").saveAsTable(TARGET_TABLE)

print("Published:", TARGET_TABLE)
print("Preserved source timestamp anomalies:", silver_invalid_times)

# COMMAND ----------

# Verify the published Silver prescriptions table
published_df = spark.table(TARGET_TABLE)

published_count = published_df.count()

print("Bronze rows:", bronze_count)
print("Silver rows:", silver_count)
print("Published rows:", published_count)

assert published_count == bronze_count, "Published row-count mismatch"
assert published_df.columns == silver_df.columns, "Published columns differ"

print("Silver prescriptions table verified successfully.")

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT *
# MAGIC FROM cdr_mimiciv.bronze.prescriptions
# MAGIC WHERE drug_type IS NULL;