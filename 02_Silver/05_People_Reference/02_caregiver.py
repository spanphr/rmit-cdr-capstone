# Databricks notebook source

# ============================================================
# ICU SILVER — CAREGIVER
# STEP 1: SOURCE PROFILING & IDENTIFIER VALIDATION
# ============================================================

from pyspark.sql import functions as F

SOURCE = "cdr_mimiciv.bronze.caregiver"
TARGET = "cdr_mimiciv.silver_v2.caregiver"

caregiver_bronze = spark.table(SOURCE)

print("SOURCE:", SOURCE)
print("TARGET:", TARGET)

# Inspect actual Bronze schema
caregiver_bronze.printSchema()

# Protect against duplicate Silver table creation
print("Target already exists:",
      spark.catalog.tableExists(TARGET))

# Single aggregation for source quality checks
id_col = F.col("caregiver_id")
trimmed_id = F.trim(id_col)

caregiver_profile = caregiver_bronze.agg(
    F.count("*").alias("total_rows"),

    F.countDistinct(id_col)
        .alias("unique_original_ids"),

    F.countDistinct(trimmed_id)
        .alias("unique_trimmed_ids"),

    F.count_if(id_col.isNull())
        .alias("null_ids"),

    F.count_if(
        id_col.isNotNull() & (trimmed_id == "")
    ).alias("empty_ids"),

    F.count_if(
        id_col != trimmed_id
    ).alias("whitespace_issues"),

    F.max(F.length(id_col))
        .alias("max_id_length")
)

display(caregiver_profile)

# Preview identifiers without modifying them
display(
    caregiver_bronze
    .select("caregiver_id")
    .limit(10)
)


# COMMAND ----------


# ============================================================
# ICU SILVER — CAREGIVER
# STEP 2: SCHEMA-CONTRACT VALIDATION
# Target: cdr_mimiciv.silver_v2.caregiver
# ============================================================

from pyspark.sql import functions as F

# 1. Check source quality results
source_quality = caregiver_profile.first().asDict()

assert source_quality["total_rows"] == 17984
assert source_quality["unique_original_ids"] == 17984
assert source_quality["unique_trimmed_ids"] == 17984
assert source_quality["null_ids"] == 0
assert source_quality["empty_ids"] == 0
assert source_quality["whitespace_issues"] == 0

# Official MIMIC-IV source: VARCHAR(10) NOT NULL
assert source_quality["max_id_length"] <= 10

# 2. Preserve original IDs exactly
# No unnecessary CAST, TRIM or deduplication
caregiver_silver = (
    caregiver_bronze
    .select("caregiver_id")
    .withColumn(
        "_silver_processed_at",
        F.current_timestamp()
    )
)

# 3. Validate prepared Silver data
checks = caregiver_silver.agg(
    F.count("*").alias("silver_rows"),
    F.countDistinct("caregiver_id").alias("unique_ids"),
    F.count_if(
        F.col("caregiver_id").isNull()
    ).alias("null_ids"),
    F.count_if(
        F.col("_silver_processed_at").isNull()
    ).alias("missing_timestamps")
).first().asDict()

assert checks["silver_rows"] == 17984
assert checks["unique_ids"] == 17984
assert checks["null_ids"] == 0
assert checks["missing_timestamps"] == 0

# 4. Check output schema
actual_types = {
    f.name: f.dataType.simpleString()
    for f in caregiver_silver.schema.fields
}

assert actual_types == {
    "caregiver_id": "string",
    "_silver_processed_at": "timestamp"
}

# 5. Protect existing Silver tables
assert not spark.catalog.tableExists(TARGET), (
    f"STOP: Target already exists: {TARGET}"
)

print("=== CAREGIVER SILVER VALIDATION ===")
print(checks)
print("SCHEMA:", actual_types)
print("STATUS: READY FOR PUBLICATION")
print("No tables have been modified.")


# COMMAND ----------


# ============================================================
# ICU SILVER — CAREGIVER
# STEP 3: SAFE DELTA TABLE PUBLICATION
# ============================================================

TARGET = "cdr_mimiciv.silver_v2.caregiver"
EXPECTED_ROWS = 17984

# Revalidate before publishing
assert checks["silver_rows"] == EXPECTED_ROWS
assert checks["unique_ids"] == EXPECTED_ROWS
assert checks["null_ids"] == 0
assert checks["missing_timestamps"] == 0

# Prevent accidental overwrite
if spark.catalog.tableExists(TARGET):
    raise RuntimeError(
        f"STOP: {TARGET} already exists. "
        "No existing table will be overwritten."
    )

print("All validation gates passed.")
print("Publishing:", TARGET)

# Create new Delta table; fail if another user created it
(
    caregiver_silver.write
    .format("delta")
    .mode("error")
    .saveAsTable(TARGET)
)

print("SUCCESS: Silver caregiver table created.")


# COMMAND ----------


# ============================================================
# ICU SILVER — CAREGIVER
# STEP 4: FINAL PERSISTED-TABLE VERIFICATION
# READ-ONLY — NO DATA MODIFICATION
# ============================================================

from pyspark.sql import functions as F

SOURCE = "cdr_mimiciv.bronze.caregiver"
TARGET = "cdr_mimiciv.silver_v2.caregiver"

bronze = spark.table(SOURCE)
silver = spark.table(TARGET)

# 1. Verify published schema
actual_schema = {
    field.name: field.dataType.simpleString()
    for field in silver.schema.fields
}

expected_schema = {
    "caregiver_id": "string",
    "_silver_processed_at": "timestamp"
}

assert actual_schema == expected_schema, (
    f"Schema mismatch: {actual_schema}"
)

# 2. Verify record quality
audit = silver.agg(
    F.count("*").alias("silver_rows"),
    F.countDistinct("caregiver_id").alias("unique_ids"),
    F.count_if(
        F.col("caregiver_id").isNull()
    ).alias("null_ids"),
    F.count_if(
        F.col("_silver_processed_at").isNull()
    ).alias("missing_processing_timestamps")
).first().asDict()

# 3. Exact identifier comparison, both directions
# exceptAll detects missing or duplicated records
missing = (
    bronze.select("caregiver_id")
    .exceptAll(silver.select("caregiver_id"))
    .count()
)

unexpected = (
    silver.select("caregiver_id")
    .exceptAll(bronze.select("caregiver_id"))
    .count()
)

# 4. Final assertions
assert audit["silver_rows"] == 17984
assert audit["unique_ids"] == 17984
assert audit["null_ids"] == 0
assert audit["missing_processing_timestamps"] == 0
assert missing == 0
assert unexpected == 0

print("=== FINAL CAREGIVER SILVER AUDIT ===")
print(audit)
print("Missing Bronze records:", missing)
print("Unexpected Silver records:", unexpected)
print("Schema verified:", actual_schema)
print("FINAL STATUS: PASSED")
