# Databricks notebook source

# ============================================================
# ICU SILVER — D_ITEMS
# STEP 1: SOURCE PROFILING & QUALITY CONTRACT
# READ-ONLY
# ============================================================

from pyspark.sql import functions as F

SOURCE = "cdr_mimiciv.bronze.d_items"
TARGET = "cdr_mimiciv.silver_v2.d_items"

bronze_items = spark.table(SOURCE)

print("SOURCE:", SOURCE)
print("TARGET:", TARGET)
print("Target exists:", spark.catalog.tableExists(TARGET))

bronze_items.printSchema()

# Safe candidate conversion for the official item identifier
itemid_as_int = F.expr(
    "TRY_CAST(TRIM(CAST(itemid AS STRING)) AS INT)"
)

# Single aggregated quality profile
item_profile = bronze_items.agg(
    F.count("*").alias("total_rows"),

    F.countDistinct("itemid").alias("unique_source_ids"),

    F.countDistinct(itemid_as_int).alias("unique_integer_ids"),

    F.count_if(
        F.col("itemid").isNull()
    ).alias("null_itemids"),

    F.count_if(
        F.col("itemid").isNotNull()
        & itemid_as_int.isNull()
    ).alias("invalid_integer_ids"),

    F.count_if(
        F.col("label").isNull()
    ).alias("null_labels"),

    F.count_if(
        F.col("label").isNotNull()
        & (F.trim("label") == "")
    ).alias("empty_labels"),

    F.count_if(
        F.col("label").isNotNull()
        & (F.col("label") != F.trim("label"))
    ).alias("label_whitespace_issues"),

    F.count_if(
        F.col("linksto").isNull()
    ).alias("null_linksto")
)

display(item_profile)

# Inspect event-table references
display(
    bronze_items
    .groupBy("linksto")
    .count()
    .orderBy(F.desc("count"))
)

print("STEP 1 COMPLETE — No data modified.")


# COMMAND ----------


# ============================================================
# ICU SILVER — D_ITEMS
# STEP 2: TYPED TRANSFORMATION + QUALITY CONTRACT
# READ-ONLY — NO TABLE MODIFICATION
# ============================================================

from pyspark.sql import functions as F

# Typed expressions
itemid_int = F.expr(
    "TRY_CAST(TRIM(itemid) AS INT)"
)

low_value = F.expr(
    "TRY_CAST(TRIM(lownormalvalue) AS DOUBLE)"
)

high_value = F.expr(
    "TRY_CAST(TRIM(highnormalvalue) AS DOUBLE)"
)

# 1. Prepare Silver candidate
items_silver = bronze_items.select(
    itemid_int.alias("itemid"),
    F.col("label"),
    F.col("abbreviation"),
    F.col("linksto"),
    F.col("category"),
    F.col("unitname"),
    F.col("param_type"),
    low_value.alias("lownormalvalue"),
    high_value.alias("highnormalvalue")
)

# 2. Single-pass source quality audit
items_quality = bronze_items.agg(
    F.count("*").alias("source_rows"),

    F.min(itemid_int).alias("min_itemid"),

    F.count_if(
        F.col("lownormalvalue").isNotNull()
        & low_value.isNull()
    ).alias("invalid_low_values"),

    F.count_if(
        F.col("highnormalvalue").isNotNull()
        & high_value.isNull()
    ).alias("invalid_high_values"),

    F.count_if(
        low_value.isNotNull()
        & high_value.isNotNull()
        & (low_value > high_value)
    ).alias("inverted_reference_ranges"),

    F.count_if(
        F.col("abbreviation").isNotNull()
        & (F.trim("abbreviation") == "")
    ).alias("empty_abbreviations"),

    F.count_if(
        F.col("unitname").isNotNull()
        & (F.trim("unitname") == "")
    ).alias("empty_units")
)

# 3. Display the full audit
display(items_quality)

# 4. Inspect prepared schema
print("=== SILVER D_ITEMS SCHEMA ===")
items_silver.printSchema()

print("STEP 2 COMPLETE — NOT PUBLISHED")


# COMMAND ----------


# ============================================================
# ICU SILVER — D_ITEMS
# STEP 3: VALIDATION-GATED DELTA PUBLICATION
# ============================================================

from pyspark.sql import functions as F

TARGET = "cdr_mimiciv.silver_v2.d_items"
EXPECTED_ROWS = 4095

# 1. Retrieve source profiling and transformation checks
profile = item_profile.first().asDict()
quality = items_quality.first().asDict()

# 2. Validate row counts and identifier integrity
assert profile["total_rows"] == EXPECTED_ROWS
assert profile["unique_source_ids"] == EXPECTED_ROWS
assert profile["unique_integer_ids"] == EXPECTED_ROWS

# 3. Validate source quality
required_zero_checks = {
    "null_itemids": profile["null_itemids"],
    "invalid_integer_ids": profile["invalid_integer_ids"],
    "null_labels": profile["null_labels"],
    "empty_labels": profile["empty_labels"],
    "label_whitespace_issues": profile["label_whitespace_issues"],
    "null_linksto": profile["null_linksto"],
    "invalid_low_values": quality["invalid_low_values"],
    "invalid_high_values": quality["invalid_high_values"],
    "inverted_reference_ranges": quality["inverted_reference_ranges"],
    "empty_abbreviations": quality["empty_abbreviations"],
    "empty_units": quality["empty_units"]
}

failures = {
    name: value
    for name, value in required_zero_checks.items()
    if value != 0
}

assert not failures, f"QUALITY GATE FAILED: {failures}"
assert quality["source_rows"] == EXPECTED_ROWS
assert quality["min_itemid"] > 220000

# 4. Enforce expected Silver schema
expected_schema = {
    "itemid": "int",
    "label": "string",
    "abbreviation": "string",
    "linksto": "string",
    "category": "string",
    "unitname": "string",
    "param_type": "string",
    "lownormalvalue": "double",
    "highnormalvalue": "double"
}

actual_schema = {
    field.name: field.dataType.simpleString()
    for field in items_silver.schema.fields
}

assert actual_schema == expected_schema, (
    f"SCHEMA CONTRACT FAILED: {actual_schema}"
)

# 5. Prevent accidental overwrites
if spark.catalog.tableExists(TARGET):
    raise RuntimeError(
        f"STOP: {TARGET} already exists. No changes made."
    )

# 6. Add Silver processing metadata
items_final = items_silver.withColumn(
    "_silver_processed_at",
    F.current_timestamp()
)

print("All quality gates passed.")
print("Publishing:", TARGET)

# 7. Create Delta table without overwriting
(
    items_final.write
    .format("delta")
    .mode("error")
    .saveAsTable(TARGET)
)

print("SUCCESS: Silver d_items table created.")


# COMMAND ----------


# ============================================================
# ICU SILVER — D_ITEMS
# STEP 4: FINAL PERSISTED-TABLE AUDIT
# READ-ONLY — NO DATA MODIFICATION
# ============================================================

from pyspark.sql import functions as F

TARGET = "cdr_mimiciv.silver_v2.d_items"
EXPECTED_ROWS = 4095

saved_items = spark.table(TARGET)

# 1. Enforce saved-table schema
expected_schema = {
    "itemid": "int",
    "label": "string",
    "abbreviation": "string",
    "linksto": "string",
    "category": "string",
    "unitname": "string",
    "param_type": "string",
    "lownormalvalue": "double",
    "highnormalvalue": "double",
    "_silver_processed_at": "timestamp"
}

actual_schema = {
    field.name: field.dataType.simpleString()
    for field in saved_items.schema.fields
}

assert actual_schema == expected_schema, (
    f"Schema mismatch: {actual_schema}"
)

# 2. Audit published records
audit = saved_items.agg(
    F.count("*").alias("silver_rows"),
    F.countDistinct("itemid").alias("unique_itemids"),
    F.count_if(F.col("itemid").isNull())
        .alias("null_itemids"),
    F.count_if(F.col("_silver_processed_at").isNull())
        .alias("missing_processing_timestamps")
).first().asDict()

# 3. Compare the nine clinical/dictionary fields
# Both directions, including duplicate-row counts
cols = items_silver.columns

missing = (
    items_silver.select(*cols)
    .exceptAll(saved_items.select(*cols))
    .count()
)

unexpected = (
    saved_items.select(*cols)
    .exceptAll(items_silver.select(*cols))
    .count()
)

# 4. Enforce final quality conditions
assert audit["silver_rows"] == EXPECTED_ROWS
assert audit["unique_itemids"] == EXPECTED_ROWS
assert audit["null_itemids"] == 0
assert audit["missing_processing_timestamps"] == 0
assert missing == 0
assert unexpected == 0

print("=== FINAL D_ITEMS SILVER AUDIT ===")
print(audit)
print("Missing from saved:", missing)
print("Unexpected in saved:", unexpected)
print("Schema verification: PASSED")
print("FINAL STATUS: PASSED")
