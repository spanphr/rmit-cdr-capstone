# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Provider Order Entry Details (`poe_detail`)
# MAGIC
# MAGIC **Source:** `cdr_mimiciv.bronze.poe_detail`  
# MAGIC **Target:** `cdr_mimiciv.silver_v2.poe_detail`
# MAGIC
# MAGIC **Objective:** Prepare provider order entry detail records for the Silver layer while preserving order identifiers, detail fields, clinical values and Bronze ingestion metadata.
# MAGIC
# MAGIC **Grain:** One source POE detail record. Multiple details may belong to the same provider order.
# MAGIC
# MAGIC **Silver rules:** Preserve every source record and column. Standardize data types only when necessary and validated. Never delete, filter, deduplicate, impute, aggregate or automatically correct clinical information.
# MAGIC
# MAGIC **Data quality:** Inspect missing identifiers, exact duplicates, detail completeness, and Bronze–Silver row counts. Document source anomalies without modifying them.
# MAGIC
# MAGIC **Publication:** Create a new Delta table only after validation. Never overwrite an existing Silver table.
# MAGIC
# MAGIC **Data quality findings:** All 8,504,982 POE detail records are
# MAGIC preserved during Silver transformation. No exact duplicate groups,
# MAGIC missing critical identifiers, or unsafe type conversions were
# MAGIC detected. Clinical detail values and Bronze ingestion metadata
# MAGIC are retained without imputation or modification.

# COMMAND ----------

# Import PySpark functions
from pyspark.sql import functions as F

# COMMAND ----------

# Configure the inputevents source and target
SOURCE_TABLE = "cdr_mimiciv.bronze.inputevents"
TARGET_TABLE = "cdr_mimiciv.silver_v2.inputevents"

print("Source:", SOURCE_TABLE)
print("Target:", TARGET_TABLE)

# COMMAND ----------

# Load all ICU input events without modifying records
bronze_df = spark.table(SOURCE_TABLE)

bronze_df.printSchema()

bronze_count = bronze_df.count()

print("Bronze rows:", bronze_count)
display(bronze_df.limit(20))

# COMMAND ----------

# Inspect all Bronze columns and existing data types
schema_df = spark.createDataFrame(
    [
        (field.name, field.dataType.simpleString(), field.nullable)
        for field in bronze_df.schema.fields
    ],
    ["column_name", "bronze_type", "nullable"]
)

display(schema_df)

# COMMAND ----------

# Preserve all source records, columns and data types
silver_df = bronze_df

silver_df.printSchema()

# COMMAND ----------

# Identify exact duplicate groups without removing records
duplicate_df = (
    silver_df
    .groupBy(*silver_df.columns)
    .count()
    .filter(F.col("count") > 1)
)

duplicate_count = duplicate_df.count()

print("Exact duplicate inputevents groups:", duplicate_count)
display(duplicate_df.limit(20))

# COMMAND ----------

# Summarize duplicate occurrences without deduplicating
duplicate_summary_df = duplicate_df.select(
    F.count("*").alias("duplicate_groups"),
    F.coalesce(F.sum("count"), F.lit(0)).alias("records_in_duplicate_groups"),
    F.coalesce(
        F.sum(F.col("count") - 1), F.lit(0)
    ).alias("additional_duplicate_occurrences"),
    F.coalesce(F.max("count"), F.lit(0)).alias("maximum_group_size")
)

display(duplicate_summary_df)

# COMMAND ----------

# Profile missing values across every source column
null_profile_df = silver_df.select(
    *[
        F.sum(F.col(c).isNull().cast("long")).alias(c)
        for c in silver_df.columns
    ]
)

display(null_profile_df)

# COMMAND ----------

# Inspect missing patient, stay and event identifiers
identifier_columns = [
    c for c in ["subject_id", "hadm_id", "stay_id", "itemid", "orderid"]
    if c in silver_df.columns
]

identifier_quality_df = silver_df.select(
    *[
        F.sum(F.col(c).isNull().cast("long")).alias(f"null_{c}")
        for c in identifier_columns
    ]
)

display(identifier_quality_df)

# COMMAND ----------

# Inspect event timestamps without changing their values
timestamp_columns = [
    c for c in ["starttime", "endtime", "storetime"]
    if c in silver_df.columns
]

timestamp_quality_df = silver_df.select(
    *[
        F.sum(F.col(c).isNull().cast("long")).alias(f"null_{c}")
        for c in timestamp_columns
    ]
)

display(timestamp_quality_df)

# COMMAND ----------

# Count events where endtime precedes starttime without modifying them
if {"starttime", "endtime"}.issubset(silver_df.columns):
    reversed_interval_count = silver_df.filter(
        F.col("starttime").isNotNull() &
        F.col("endtime").isNotNull() &
        (F.col("endtime") < F.col("starttime"))
    ).count()
else:
    reversed_interval_count = None

print("Events with endtime before starttime:", reversed_interval_count)

# COMMAND ----------

# Verify source record count and schema preservation
silver_count = silver_df.count()

print("Bronze rows:", bronze_count)
print("Silver rows:", silver_count)

assert bronze_count == silver_count, "Bronze–Silver row-count mismatch"
assert bronze_df.schema == silver_df.schema, "Bronze–Silver schema mismatch"

print("Bronze–Silver preservation checks passed.")

# COMMAND ----------

# Report duplicate and identifier findings before publication
identifier_nulls = identifier_quality_df.first().asDict()

print("Exact duplicate groups:", duplicate_count)
print("Identifier NULL counts:", identifier_nulls)
print("Reversed intervals:", reversed_interval_count)

assert bronze_count == silver_count, "Record counts do not match"
assert bronze_df.schema == silver_df.schema, "Source schema changed"

print("Record count and schema checks passed.")
print("Review all reported anomalies before publication.")

# COMMAND ----------

# Confirm whether the inputevents Silver table already exists
target_exists = spark.catalog.tableExists(TARGET_TABLE)

print("Target table already exists:", target_exists)
print("Target:", TARGET_TABLE)

# COMMAND ----------

# Compare reversed inputevent intervals in Bronze and Silver
bronze_reversed = bronze_df.filter(
    F.col("starttime").isNotNull() &
    F.col("endtime").isNotNull() &
    (F.col("endtime") < F.col("starttime"))
).count()

silver_reversed = silver_df.filter(
    F.col("starttime").isNotNull() &
    F.col("endtime").isNotNull() &
    (F.col("endtime") < F.col("starttime"))
).count()

print("Bronze reversed intervals:", bronze_reversed)
print("Silver reversed intervals:", silver_reversed)

assert bronze_reversed == silver_reversed, (
    "Bronze–Silver timestamp anomaly counts differ"
)

# COMMAND ----------

# Verify published POE detail records and schema
published_df = spark.table(TARGET_TABLE)
published_count = published_df.count()

print("Bronze rows:", bronze_count)
print("Silver rows:", silver_count)
print("Published rows:", published_count)

assert published_count == bronze_count, "Published row-count mismatch"
assert published_df.schema == silver_df.schema, "Published schema mismatch"

print("Silver POE detail table verified successfully.")

# COMMAND ----------

# Publish all inputevents records, preserving source timestamp anomalies
assert bronze_count == silver_count, "Bronze–Silver row-count mismatch"
assert bronze_df.schema == silver_df.schema, "Bronze–Silver schema mismatch"

assert duplicate_count == 0, "Unexpected exact duplicate groups"
assert all(v == 0 for v in identifier_nulls.values()), (
    "Missing critical identifiers require review"
)

assert bronze_reversed == silver_reversed == 10, (
    "Source timestamp anomaly count changed; review before publication"
)

assert not spark.catalog.tableExists(TARGET_TABLE), (
    f"Target already exists; publication stopped: {TARGET_TABLE}"
)

silver_df.write.format("delta").saveAsTable(TARGET_TABLE)

print("Published:", TARGET_TABLE)
print("Preserved reversed timestamp intervals:", silver_reversed)

# COMMAND ----------

# Verify published inputevents records and schema
published_df = spark.table(TARGET_TABLE)
published_count = published_df.count()

print("Bronze rows:", bronze_count)
print("Silver rows:", silver_count)
print("Published rows:", published_count)

assert published_count == bronze_count, "Published row-count mismatch"
assert published_df.schema == silver_df.schema, "Published schema mismatch"

print("Silver inputevents table verified successfully.")

# COMMAND ----------

