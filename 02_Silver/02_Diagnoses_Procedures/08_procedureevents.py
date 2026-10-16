# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Procedure Events (`procedureevents`)
# MAGIC
# MAGIC **Source:** `cdr_mimiciv.bronze.procedureevents`  
# MAGIC **Target:** `cdr_mimiciv.silver_v2.procedureevents`
# MAGIC
# MAGIC **Objective:** Standardize procedure event records while preserving patient and admission identifiers, ICU stay identifiers, procedure identifiers, timestamps, status information, and clinical measurements.
# MAGIC
# MAGIC **Expected grain:** One source procedure event, identified by `procedureevent_id` if present. Otherwise, the source event grain will be established from the available identifiers.
# MAGIC
# MAGIC **Silver rules:** Preserve every Bronze record and its clinical meaning. Standardize data types and whitespace, validate identifiers and timestamps, inspect duplicates, and reconcile Bronze–Silver counts. No deletion, filtering, deduplication, imputation or aggregation.

# COMMAND ----------

# Import PySpark functions
from pyspark.sql import functions as F

# COMMAND ----------

# Configure Bronze source and Silver target
CATALOG = "cdr_mimiciv"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver_v2"

SOURCE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.procedureevents"
TARGET_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.procedureevents"

# COMMAND ----------

# Load and inspect ICU procedure event records
bronze_df = spark.table(SOURCE_TABLE)

bronze_df.printSchema()
display(bronze_df.limit(20))

print("Bronze rows:", bronze_df.count())

# COMMAND ----------

# Profile source row counts and NULL values across all columns
bronze_profile = bronze_df.select(
    F.count("*").alias("total_rows"),
    *[
        F.sum(F.col(c).isNull().cast("int")).alias(f"null_{c}")
        for c in bronze_df.columns
    ]
)

display(bronze_profile)

# COMMAND ----------

# Preserve all source fields while standardizing known data types
silver_df = bronze_df

long_columns = [
    "subject_id", "hadm_id", "stay_id",
    "itemid", "orderid", "linkorderid"
]

timestamp_columns = [
    "starttime", "endtime", "storetime"
]

string_columns = [
    "valueuom", "location", "locationcategory",
    "ordercategoryname", "secondaryordercategoryname",
    "ordercategorydescription", "patientweight",
    "statusdescription", "comments_date",
    "comments_canceledby", "comments"
]

for column in long_columns:
    if column in silver_df.columns:
        silver_df = silver_df.withColumn(
            column, F.col(column).cast("long")
        )

for column in timestamp_columns:
    if column in silver_df.columns:
        silver_df = silver_df.withColumn(
            column, F.col(column).cast("timestamp")
        )

for column in string_columns:
    if column in silver_df.columns:
        if dict(silver_df.dtypes)[column] == "string":
            silver_df = silver_df.withColumn(
                column, F.trim(F.col(column))
            )

silver_df.printSchema()
display(silver_df.limit(20))

# COMMAND ----------

# Inspect exact duplicate rows without removing records
duplicate_df = (
    silver_df
    .groupBy(*silver_df.columns)
    .count()
    .filter(F.col("count") > 1)
)

duplicate_count = duplicate_df.count()

print("Exact duplicate procedure event groups:", duplicate_count)
display(duplicate_df.limit(20))

# COMMAND ----------

# Inspect NULL values in important event identifiers and timestamps
critical_columns = [
    "subject_id", "hadm_id", "stay_id",
    "itemid", "starttime", "endtime"
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

# Check procedure events with end times before start times
if {"starttime", "endtime"}.issubset(silver_df.columns):
    invalid_time_count = silver_df.filter(
        F.col("starttime").isNotNull() &
        F.col("endtime").isNotNull() &
        (F.col("endtime") < F.col("starttime"))
    ).count()
else:
    invalid_time_count = None

print("Events with endtime before starttime:", invalid_time_count)

# COMMAND ----------

# Confirm all Bronze records are preserved by row count
bronze_count = bronze_df.count()
silver_count = silver_df.count()

print("Bronze rows:", bronze_count)
print("Silver rows:", silver_count)

assert bronze_count == silver_count, "Bronze–Silver row-count mismatch"

# COMMAND ----------

# Validate required event identifiers and record preservation
quality = quality_df.first().asDict()

required_columns = ["subject_id", "stay_id", "itemid"]

required_checks = [
    f"null_{c}" for c in required_columns
    if c in available_critical_columns
]

issues = {
    k: quality[k]
    for k in required_checks
    if quality[k] != 0
}

assert not issues, f"Critical procedure event quality issues: {issues}"
assert bronze_count == silver_count, "Record counts do not match"

print("All critical Silver quality checks passed.")
print("Exact duplicate groups (review only):", duplicate_count)
print("Invalid timestamp ranges (review only):", invalid_time_count)

# COMMAND ----------

# Check whether the Silver procedure events table already exists
target_exists = spark.catalog.tableExists(TARGET_TABLE)

print("Target table already exists:", target_exists)
print("Target:", TARGET_TABLE)

# COMMAND ----------

# Create the validated Silver procedure events table without overwrite
assert not spark.catalog.tableExists(TARGET_TABLE), (
    f"Target already exists; publication stopped: {TARGET_TABLE}"
)

assert duplicate_count == 0, (
    "Exact duplicate procedure events require review before publication"
)

assert invalid_time_count in (None, 0), (
    "Invalid procedure timestamp ranges require review before publication"
)

silver_df.write.format("delta").saveAsTable(TARGET_TABLE)

print("Published:", TARGET_TABLE)

# COMMAND ----------

# Verify published procedure events and table structure
published_df = spark.table(TARGET_TABLE)

published_count = published_df.count()

print("Bronze rows:", bronze_count)
print("Silver rows:", silver_count)
print("Published rows:", published_count)

assert published_count == silver_count, "Published row-count mismatch"
assert published_df.columns == silver_df.columns, "Published columns differ"

display(published_df.limit(20))

print("Silver procedureevents table verified successfully.")

# COMMAND ----------

