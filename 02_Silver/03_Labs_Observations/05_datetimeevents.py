# Databricks notebook source

# ============================================================
# ICU SILVER — DATETIMEEVENTS
# STEP 1: SOURCE SCHEMA AND QUALITY PROFILING
# READ-ONLY — NO TABLE MODIFICATION
# ============================================================

from pyspark.sql import functions as F

SOURCE = "cdr_mimiciv.bronze.datetimeevents"
TARGET = "cdr_mimiciv.silver_v2.datetimeevents"

dt_bronze = spark.table(SOURCE)

print("SOURCE:", SOURCE)
print("TARGET:", TARGET)
print("Target exists:", spark.catalog.tableExists(TARGET))

# Inspect actual source types before conversions
dt_bronze.printSchema()

# Validate essential source fields
required = [
    "subject_id", "hadm_id", "stay_id",
    "itemid", "charttime", "value"
]

missing = set(required) - set(dt_bronze.columns)
assert not missing, f"Missing source columns: {missing}"

# Efficient single-pass quality profile
checks = [
    F.count("*").alias("total_rows")
]

for c in required:
    checks.append(
        F.count_if(F.col(c).isNull())
        .alias(f"null_{c}")
    )

profile = dt_bronze.agg(*checks)

display(profile)
display(dt_bronze.limit(5))

print("STEP 1 COMPLETE — No tables modified.")


# COMMAND ----------


# ============================================================
# ICU SILVER — DATETIMEEVENTS
# STEP 2: SCHEMA-DRIVEN TRANSFORMATION
# AND SINGLE-PASS QUALITY VALIDATION
# READ-ONLY — NO TABLE MODIFICATION
# ============================================================

from pyspark.sql import functions as F

SOURCE = "cdr_mimiciv.bronze.datetimeevents"
TARGET = "cdr_mimiciv.silver_v2.datetimeevents"

# Official MIMIC-IV source-aligned types
type_contract = {
    "subject_id": "INT",
    "hadm_id": "INT",
    "stay_id": "INT",
    "caregiver_id": "INT",
    "charttime": "TIMESTAMP",
    "storetime": "TIMESTAMP",
    "itemid": "INT",
    "value": "TIMESTAMP",
    "warning": "SMALLINT"
}

source_columns = list(type_contract) + ["valueuom"]

assert set(source_columns).issubset(
    set(dt_bronze.columns)
), "Source schema does not match expected ICU fields"

# 1. Prepare typed columns and retain originals for audit
prepared_dt = dt_bronze.select(
    *[
        F.col(c).alias(f"_raw_{c}")
        for c in type_contract
    ],
    *[
        F.expr(
            f"TRY_CAST(TRIM(CAST(`{c}` AS STRING)) AS {dtype})"
        ).alias(c)
        for c, dtype in type_contract.items()
    ],
    F.col("valueuom")
)

# 2. One aggregation across the prepared source
quality_expressions = [
    F.count("*").alias("total_rows")
]

# Detect values lost during conversion
for c in type_contract:
    quality_expressions.append(
        F.count_if(
            F.col(f"_raw_{c}").isNotNull()
            & F.col(c).isNull()
        ).alias(f"cast_fail_{c}")
    )

# Preserve and measure existing missing values
for c in source_columns:
    quality_expressions.append(
        F.count_if(
            F.col(c).isNull()
        ).alias(f"null_{c}")
    )

dt_quality = prepared_dt.agg(*quality_expressions)

display(dt_quality)

# 3. Final clinical source column projection
dt_silver = prepared_dt.select(
    "subject_id",
    "hadm_id",
    "stay_id",
    "caregiver_id",
    "charttime",
    "storetime",
    "itemid",
    "value",
    "valueuom",
    "warning"
)

print("=== DATETIMEEVENTS SILVER SCHEMA ===")
dt_silver.printSchema()

print("STEP 2 COMPLETE — NOT PUBLISHED")


# COMMAND ----------


# ============================================================
# ICU SILVER — DATETIMEEVENTS
# STEP 3: ADVANCED REFERENTIAL INTEGRITY
# READ-ONLY — NO TABLE MODIFICATION
# ============================================================

from pyspark.sql import functions as F

EXPECTED_ROWS = 9979761

# 1. Confirm all datatype conversions succeeded
quality = dt_quality.first().asDict()

assert quality["total_rows"] == EXPECTED_ROWS

cast_failures = {
    k: v for k, v in quality.items()
    if k.startswith("cast_fail_") and v != 0
}

assert not cast_failures, (
    f"Datatype conversion failures: {cast_failures}"
)

print("All datatype conversions verified.")
print("Source NULL counts:", {
    k: v for k, v in quality.items()
    if k.startswith("null_")
})

# 2. Read verified Silver reference tables
icu_ref = spark.table(
    "cdr_mimiciv.silver_v2.icustays"
).select(
    F.col("stay_id").alias("ref_stay_id"),
    F.col("subject_id").alias("ref_subject_id"),
    F.col("hadm_id").alias("ref_hadm_id")
)

item_ref = spark.table(
    "cdr_mimiciv.silver_v2.d_items"
).select(
    F.col("itemid").alias("ref_itemid")
)

caregiver_ref = spark.table(
    "cdr_mimiciv.silver_v2.caregiver"
).select(
    F.expr("TRY_CAST(caregiver_id AS INT)")
    .alias("ref_caregiver_id")
)

# 3. Efficient broadcast joins
linked_dt = (
    dt_silver.alias("d")
    .join(
        F.broadcast(icu_ref),
        F.col("d.stay_id") == F.col("ref_stay_id"),
        "left"
    )
    .join(
        F.broadcast(item_ref),
        F.col("d.itemid") == F.col("ref_itemid"),
        "left"
    )
    .join(
        F.broadcast(caregiver_ref),
        F.col("d.caregiver_id")
        == F.col("ref_caregiver_id"),
        "left"
    )
)

# 4. Single-pass relationship audit
dt_integrity = linked_dt.agg(
    F.count("*").alias("joined_rows"),

    F.count_if(
        F.col("ref_stay_id").isNull()
    ).alias("missing_icu_stays"),

    F.count_if(
        F.col("ref_itemid").isNull()
    ).alias("missing_dictionary_items"),

    F.count_if(
        F.col("d.caregiver_id").isNotNull()
        & F.col("ref_caregiver_id").isNull()
    ).alias("missing_caregivers"),

    F.count_if(
        F.col("ref_stay_id").isNotNull()
        & (
            (F.col("d.subject_id")
             != F.col("ref_subject_id"))
            |
            (F.col("d.hadm_id")
             != F.col("ref_hadm_id"))
        )
    ).alias("stay_identity_mismatches")
)

display(dt_integrity)

print("STEP 3 COMPLETE — NO TABLES MODIFIED")


# COMMAND ----------


# ============================================================
# ICU SILVER — DATETIMEEVENTS
# STEP 4: VALIDATION-GATED DELTA PUBLICATION
# ============================================================

from pyspark.sql import functions as F

TARGET = "cdr_mimiciv.silver_v2.datetimeevents"
EXPECTED_ROWS = 9979761

# 1. Retrieve completed validation results
quality = dt_quality.first().asDict()
integrity = dt_integrity.first().asDict()

assert quality["total_rows"] == EXPECTED_ROWS
assert integrity["joined_rows"] == EXPECTED_ROWS

# 2. Validate conversions
for name, value in quality.items():
    if name.startswith("cast_fail_"):
        assert value == 0, f"{name}: {value}"

# 3. Validate relationships
relationship_checks = [
    "missing_icu_stays",
    "missing_dictionary_items",
    "missing_caregivers",
    "stay_identity_mismatches"
]

for name in relationship_checks:
    assert integrity[name] == 0, (
        f"Integrity failure: {name} = {integrity[name]}"
    )

# 4. Validate exact output schema
expected_types = {
    "subject_id": "int",
    "hadm_id": "int",
    "stay_id": "int",
    "caregiver_id": "int",
    "charttime": "timestamp",
    "storetime": "timestamp",
    "itemid": "int",
    "value": "timestamp",
    "valueuom": "string",
    "warning": "smallint"
}

actual_types = {
    f.name: f.dataType.simpleString()
    for f in dt_silver.schema.fields
}

# Spark reports SMALLINT as short
expected_types["warning"] = "smallint"

assert actual_types == expected_types, (
    f"Schema mismatch: {actual_types}"
)

# 5. Prevent accidental overwrite
if spark.catalog.tableExists(TARGET):
    raise RuntimeError(
        f"STOP: {TARGET} already exists. No changes made."
    )

# 6. Add Silver processing timestamp
dt_final = dt_silver.withColumn(
    "_silver_processed_at",
    F.current_timestamp()
)

print("ALL VALIDATION GATES PASSED")
print("Publishing:", TARGET)

# 7. Publish safely without overwriting
(
    dt_final.write
    .format("delta")
    .mode("error")
    .saveAsTable(TARGET)
)

print("SUCCESS: datetimeevents Silver table created.")


# COMMAND ----------


# ============================================================
# ICU SILVER — DATETIMEEVENTS
# STEP 5: FINAL PERSISTED-TABLE AUDIT
# READ-ONLY — NO TABLE MODIFICATION
# ============================================================

from pyspark.sql import functions as F

TARGET = "cdr_mimiciv.silver_v2.datetimeevents"
EXPECTED_ROWS = 9979761

saved_dt = spark.table(TARGET)

# 1. Validate persisted schema
expected_schema = {
    "subject_id": "int",
    "hadm_id": "int",
    "stay_id": "int",
    "caregiver_id": "int",
    "charttime": "timestamp",
    "storetime": "timestamp",
    "itemid": "int",
    "value": "timestamp",
    "valueuom": "string",
    "warning": "smallint",
    "_silver_processed_at": "timestamp"
}

actual_schema = {
    f.name: f.dataType.simpleString()
    for f in saved_dt.schema.fields
}

assert actual_schema == expected_schema, (
    f"Schema mismatch: {actual_schema}"
)

# 2. Audit saved data
audit = saved_dt.agg(
    F.count("*").alias("silver_rows"),

    F.count_if(
        F.col("stay_id").isNull()
    ).alias("null_stay_ids"),

    F.count_if(
        F.col("itemid").isNull()
    ).alias("null_itemids"),

    F.count_if(
        F.col("_silver_processed_at").isNull()
    ).alias("missing_processing_timestamps")
).first().asDict()

# 3. Validate record counts
assert audit["silver_rows"] == EXPECTED_ROWS
assert audit["null_stay_ids"] == 0
assert audit["null_itemids"] == 0
assert audit["missing_processing_timestamps"] == 0

print("Row count and schema checks: PASSED")
print("Starting exact record reconciliation...")

# 4. Exact comparison of all 10 source columns
# exceptAll also detects duplicate-row differences
cols = dt_silver.columns

missing = (
    dt_silver.select(*cols)
    .exceptAll(saved_dt.select(*cols))
    .count()
)

unexpected = (
    saved_dt.select(*cols)
    .exceptAll(dt_silver.select(*cols))
    .count()
)

# 5. Final assertions
assert missing == 0, f"Missing records: {missing}"
assert unexpected == 0, f"Unexpected records: {unexpected}"

print("=== FINAL DATETIMEEVENTS AUDIT ===")
print(audit)
print("Missing from saved:", missing)
print("Unexpected in saved:", unexpected)
print("Schema verification: PASSED")
print("FINAL STATUS: PASSED")
