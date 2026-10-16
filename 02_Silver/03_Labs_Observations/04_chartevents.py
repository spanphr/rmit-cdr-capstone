# Databricks notebook source

# ICU CHARTEVENTS — STEP 1: SOURCE AND TARGET CHECK
# READ ONLY — NO TABLES MODIFIED

SOURCE = "cdr_mimiciv.bronze.chartevents"
TARGET = "cdr_mimiciv.silver_v2.chartevents"
from pyspark.sql import functions as F
source_exists = spark.catalog.tableExists(SOURCE)
target_exists = spark.catalog.tableExists(TARGET)


print("=== CHARTEVENTS STATUS ===")
print("Bronze source exists:", source_exists)
print("Silver V2 target exists:", target_exists)

if source_exists:
    print("\n=== BRONZE SCHEMA ===")
    spark.table(SOURCE).printSchema()

if target_exists:
    print("\n=== EXISTING SILVER SCHEMA ===")
    spark.table(TARGET).printSchema()

print("\nSTEP 1 COMPLETE — NO DATA MODIFIED")


# COMMAND ----------


# CHARTEVENTS — STEP 2

SOURCE = "cdr_mimiciv.bronze.chartevents"

# Retrieve Delta table metadata without scanning all records
details = spark.sql(f"DESCRIBE DETAIL {SOURCE}")

display(
    details.select(
        "format",
        "numFiles",
        "sizeInBytes",
        "partitionColumns"
    )
)

# Inspect a small sample of clinical observations
sample = (
    spark.table(SOURCE)
    .select(
        "subject_id",
        "hadm_id",
        "stay_id",
        "itemid",
        "charttime",
        "value",
        "valuenum",
        "valueuom",
        "warning"
    )
    .limit(10)
)

display(sample)

print("STEP 2 COMPLETE — NO DATA MODIFIED")


# COMMAND ----------



from pyspark.sql import functions as F

SOURCE = "cdr_mimiciv.bronze.chartevents"

df = spark.table(SOURCE)

def count_where(condition, name):
    return F.sum(
        F.when(condition, 1).otherwise(0)
    ).alias(name)

quality = df.agg(
    F.count("*").alias("total_rows"),

    count_where(
        F.col("subject_id").isNull(),
        "null_subject_ids"
    ),

    count_where(
        F.col("hadm_id").isNull(),
        "null_hadm_ids"
    ),

    count_where(
        F.col("stay_id").isNull(),
        "null_stay_ids"
    ),

    count_where(
        F.col("itemid").isNull(),
        "null_itemids"
    ),

    count_where(
        F.col("charttime").isNull(),
        "null_charttimes"
    ),

    count_where(
        F.col("valuenum").isNull(),
        "null_numeric_values"
    ),

    count_where(
        F.col("value").isNull()
        & F.col("valuenum").isNull(),
        "missing_both_values"
    ),

    count_where(
        F.col("storetime") < F.col("charttime"),
        "earlier_storetimes"
    )
)

display(quality)

print("STEP 3 COMPLETE — READ ONLY")


# COMMAND ----------


from pyspark.sql import functions as F

df = spark.table("cdr_mimiciv.bronze.chartevents")

# 1. Inspect records with neither text nor numeric values
missing_observations = (
    df.filter(
        F.col("value").isNull() &
        F.col("valuenum").isNull()
    )
    .select(
        "subject_id", "stay_id", "itemid",
        "charttime", "value", "valuenum",
        "valueuom", "warning"
    )
    .limit(5)
)

print("=== MISSING OBSERVATIONS ===")
display(missing_observations)

# 2. Examine examples of reversed timestamps
timestamp_examples = (
    df.filter(
        F.col("storetime").isNotNull() &
        F.col("charttime").isNotNull() &
        (F.col("storetime") < F.col("charttime"))
    )
    .select(
        "stay_id",
        "itemid",
        "charttime",
        "storetime",
        (
            F.unix_timestamp("charttime") -
            F.unix_timestamp("storetime")
        ).alias("seconds_early")
    )
    .limit(10)
)

print("=== TIMESTAMP EXAMPLES ===")
display(timestamp_examples)

print("STEP 4 COMPLETE — NO DATA MODIFIED")


# COMMAND ----------


from pyspark.sql import functions as F

SOURCE = "cdr_mimiciv.bronze.chartevents"
TARGET = "cdr_mimiciv.silver_v2.chartevents"

# Safety checks: prevent accidental replacement
assert spark.catalog.tableExists(SOURCE), "Bronze source missing"
assert not spark.catalog.tableExists(TARGET), (
    "Silver target already exists. Stop to avoid overwriting."
)

bronze = spark.table(SOURCE)

# Build a row-preserving Silver candidate
# No filters, joins, deduplication or datatype changes
silver_candidate = (
    bronze
    .withColumn(
        "_dq_missing_observation",
        F.col("value").isNull() &
        F.col("valuenum").isNull()
    )
    .withColumn(
        "_dq_early_storetime",
        F.coalesce(
            F.col("storetime") < F.col("charttime"),
            F.lit(False)
        )
    )
    .withColumn(
        "_silver_processed_at",
        F.current_timestamp()
    )
)

print("=== SILVER CANDIDATE SCHEMA ===")
silver_candidate.printSchema()

print("=== CANDIDATE SAMPLE ===")
display(
    silver_candidate.select(
        "subject_id",
        "stay_id",
        "itemid",
        "charttime",
        "value",
        "valuenum",
        "_dq_missing_observation",
        "_dq_early_storetime",
        "_silver_processed_at"
    ).limit(5)
)

print("STEP 5 COMPLETE — NOT PUBLISHED")


# COMMAND ----------


from pyspark.sql import functions as F

# STEP 6 — SILVER CANDIDATE VALIDATION
# Read-only: no tables modified

expected_rows = 432997491
expected_missing = 1
expected_early = 36530969

# Verify original columns remain unchanged
expected_columns = bronze.columns + [
    "_dq_missing_observation",
    "_dq_early_storetime",
    "_silver_processed_at"
]

assert silver_candidate.columns == expected_columns, (
    "Column preservation check failed"
)

# One aggregation for all candidate checks
audit_df = silver_candidate.agg(
    F.count("*").alias("total_rows"),
    F.sum(
        F.col("_dq_missing_observation").cast("long")
    ).alias("missing_observation_flags"),
    F.sum(
        F.col("_dq_early_storetime").cast("long")
    ).alias("early_storetime_flags"),
    F.sum(
        F.col("_silver_processed_at").isNull().cast("long")
    ).alias("missing_processing_timestamps")
)

result = audit_df.first().asDict()

print("=== CHARTEVENTS CANDIDATE AUDIT ===")
for key, value in result.items():
    print(f"{key}: {value}")

# Fail immediately on unexpected results
assert result["total_rows"] == expected_rows
assert result["missing_observation_flags"] == expected_missing
assert result["early_storetime_flags"] == expected_early
assert result["missing_processing_timestamps"] == 0

print("\nALL STEP 6 VALIDATION GATES PASSED")
print("STATUS: READY FOR REFERENCE VALIDATION")
print("NO TABLES MODIFIED")


# COMMAND ----------


from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

# STEP 7 — READ-ONLY REFERENCE VALIDATION

CATALOG = "cdr_mimiciv"

references = {
    "ICU": f"{CATALOG}.silver_v2.icustays",
    "ITEMS": f"{CATALOG}.silver_v2.d_items",
    "CAREGIVERS": f"{CATALOG}.silver_v2.caregiver"
}

for name, table in references.items():
    assert spark.catalog.tableExists(table), (
        f"Missing reference: {table}"
    )

# Reference tables
icu_ref = (
    spark.table(references["ICU"])
    .select("stay_id", "subject_id", "hadm_id")
    .distinct()
)

item_ref = (
    spark.table(references["ITEMS"])
    .select("itemid")
    .distinct()
)

caregiver_ref = (
    spark.table(references["CAREGIVERS"])
    .select(
        F.trim("caregiver_id").alias("caregiver_key")
    )
    .distinct()
)

# Check unique patient-stay combinations
event_stays = (
    silver_candidate
    .select("stay_id", "subject_id", "hadm_id")
    .distinct()
)

invalid_stays = event_stays.join(
    broadcast(icu_ref),
    ["stay_id", "subject_id", "hadm_id"],
    "left_anti"
)

# Check unique dictionary item references
event_items = (
    silver_candidate
    .select("itemid")
    .distinct()
)

invalid_items = event_items.join(
    broadcast(item_ref),
    "itemid",
    "left_anti"
)

# Check non-null caregiver references
event_caregivers = (
    silver_candidate
    .where(F.col("caregiver_id").isNotNull())
    .select(
        F.col("caregiver_id")
        .cast("string")
        .alias("caregiver_key")
    )
    .distinct()
)

invalid_caregivers = event_caregivers.join(
    broadcast(caregiver_ref),
    "caregiver_key",
    "left_anti"
)

results = {
    "invalid_icu_relationships": invalid_stays.count(),
    "missing_dictionary_items": invalid_items.count(),
    "missing_caregivers": invalid_caregivers.count()
}

print("=== CHARTEVENTS REFERENCE AUDIT ===")

for name, value in results.items():
    print(f"{name}: {value}")

print("STEP 7 COMPLETE — NO TABLES MODIFIED")


# COMMAND ----------


# STEP 8 — SAFE SILVER PUBLICATION
# Creates a new Delta table.

SOURCE = "cdr_mimiciv.bronze.chartevents"
TARGET = "cdr_mimiciv.silver_v2.chartevents"

assert spark.catalog.tableExists(SOURCE), \
    "Bronze source is missing!"

assert not spark.catalog.tableExists(TARGET), \
    "STOP: Silver target already exists!"

assert "silver_candidate" in globals(), \
    "Silver candidate unavailable. Rerun Step 5."

# Verify all original columns are preserved
original_columns = spark.table(SOURCE).columns

expected_columns = original_columns + [
    "_dq_missing_observation",
    "_dq_early_storetime",
    "_silver_processed_at"
]

assert silver_candidate.columns == expected_columns, \
    "Unexpected Silver schema!"

print("All pre-publication safety checks passed.")
print("Publishing:", TARGET)

# Publish without overwriting
(
    silver_candidate.write
    .format("delta")
    .mode("error")
    .saveAsTable(TARGET)
)

print("SUCCESS: Silver CHARTEVENTS created!")
print("STEP 8 COMPLETE — PUBLISHED")


# COMMAND ----------


# ============================================================
# ICU SILVER — CHARTEVENTS
# STEP 9: FINAL PERSISTED-TABLE AUDIT
# READ-ONLY — SERVERLESS COMPATIBLE
# ============================================================

from pyspark.sql import functions as F

SOURCE = "cdr_mimiciv.bronze.chartevents"
TARGET = "cdr_mimiciv.silver_v2.chartevents"

EXPECTED_ROWS = 432997491
EXPECTED_MISSING = 1
EXPECTED_EARLY = 36530969

source = spark.table(SOURCE)
saved = spark.table(TARGET)

# 1. Verify original columns and datatypes
expected_columns = source.columns + [
    "_dq_missing_observation",
    "_dq_early_storetime",
    "_silver_processed_at"
]

assert saved.columns == expected_columns, \
    "Silver column structure mismatch"

for field in source.schema.fields:
    assert saved.schema[field.name].dataType == field.dataType, \
        f"Original datatype changed: {field.name}"

assert saved.schema["_dq_missing_observation"].dataType.simpleString() == "boolean"
assert saved.schema["_dq_early_storetime"].dataType.simpleString() == "boolean"
assert saved.schema["_silver_processed_at"].dataType.simpleString() == "timestamp"

print("Schema verification: PASSED")

# 2. Single-pass persisted-data audit
audit = saved.agg(
    F.count("*").alias("silver_rows"),

    F.count_if(
        F.col("subject_id").isNull()
        | F.col("hadm_id").isNull()
        | F.col("stay_id").isNull()
        | F.col("itemid").isNull()
        | F.col("charttime").isNull()
    ).alias("missing_critical_fields"),

    F.count_if(
        F.col("_silver_processed_at").isNull()
    ).alias("missing_processing_timestamps"),

    F.count_if(
        F.col("_dq_missing_observation")
    ).alias("missing_observation_flags"),

    F.count_if(
        F.col("_dq_early_storetime")
    ).alias("early_storetime_flags"),

    F.count_if(
        F.col("_dq_missing_observation").isNull()
        | F.col("_dq_early_storetime").isNull()
    ).alias("null_quality_flags"),

    F.count_if(
        F.col("_dq_missing_observation")
        != (
            F.col("value").isNull()
            & F.col("valuenum").isNull()
        )
        |
        (
            F.col("_dq_early_storetime")
            != F.coalesce(
                F.col("storetime") < F.col("charttime"),
                F.lit(False)
            )
        )
    ).alias("flag_mismatches")
).first().asDict()

# 3. Validate persisted results
assert audit["silver_rows"] == EXPECTED_ROWS
assert audit["missing_critical_fields"] == 0
assert audit["missing_processing_timestamps"] == 0
assert audit["missing_observation_flags"] == EXPECTED_MISSING
assert audit["early_storetime_flags"] == EXPECTED_EARLY
assert audit["null_quality_flags"] == 0
assert audit["flag_mismatches"] == 0

print("=== FINAL CHARTEVENTS AUDIT ===")

for name, value in audit.items():
    print(f"{name}: {value}")

print("SCHEMA: PASSED")
print("FINAL AUDIT: PASSED")
print("NO TABLES MODIFIED")
