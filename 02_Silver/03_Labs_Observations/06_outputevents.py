# Databricks notebook source

# ============================================================
# ICU SILVER — OUTPUTEVENTS
# STEP 1: BRONZE SCHEMA & SOURCE QUALITY PROFILING
# READ-ONLY — NO TABLE MODIFICATION
# ============================================================

from pyspark.sql import functions as F

SOURCE = "cdr_mimiciv.bronze.outputevents"
TARGET = "cdr_mimiciv.silver_v2.outputevents"

out_bronze = spark.table(SOURCE)

print("=== OUTPUTEVENTS SOURCE ===")
print("SOURCE:", SOURCE)
print("TARGET:", TARGET)
print("Target exists:", spark.catalog.tableExists(TARGET))

# Inspect actual Bronze field types
out_bronze.printSchema()

# Validate expected source columns
required = [
    "subject_id", "hadm_id", "stay_id",
    "caregiver_id", "charttime", "storetime",
    "itemid", "value", "valueuom"
]

missing = set(required) - set(out_bronze.columns)
assert not missing, f"Missing columns: {missing}"

# Single-pass source quality profile
metrics = [F.count("*").alias("total_rows")]

for c in required:
    metrics.append(
        F.count_if(F.col(c).isNull())
        .alias(f"null_{c}")
    )

out_profile = out_bronze.agg(*metrics)

display(out_profile)

# Small sample for datatype and value inspection
display(
    out_bronze.select(*required).limit(5)
)

print("STEP 1 COMPLETE — NO DATA MODIFIED")


# COMMAND ----------


# ============================================================
# ICU SILVER — OUTPUTEVENTS
# STEP 2: TYPE-SAFE TRANSFORMATION AND QUALITY AUDIT
# READ-ONLY — NO TABLE MODIFICATION
# ============================================================

from pyspark.sql import functions as F

# Official MIMIC-IV source-aligned datatype contract
type_contract = {
    "subject_id": "INT",
    "hadm_id": "INT",
    "stay_id": "INT",
    "caregiver_id": "INT",
    "charttime": "TIMESTAMP",
    "storetime": "TIMESTAMP",
    "itemid": "INT",
    "value": "DOUBLE"
}

# 1. Validate source fields
required = list(type_contract) + ["valueuom"]

assert set(required).issubset(
    set(out_bronze.columns)
), "Missing expected source columns"

# 2. Prepare typed data while retaining original
# values temporarily for conversion verification
prepared_out = out_bronze.select(
    *[
        F.col(c).alias(f"_raw_{c}")
        for c in type_contract
    ],
    *[
        F.expr(
            f"TRY_CAST(TRIM(CAST(`{c}` AS STRING)) "
            f"AS {dtype})"
        ).alias(c)
        for c, dtype in type_contract.items()
    ],
    F.col("valueuom")
)

# 3. Single-pass quality aggregation
metrics = [
    F.count("*").alias("total_rows")
]

# Identify values lost during datatype conversion
for c in type_contract:
    metrics.append(
        F.count_if(
            F.col(f"_raw_{c}").isNotNull()
            & F.col(c).isNull()
        ).alias(f"cast_fail_{c}")
    )

# Count missing values after transformation
for c in required:
    metrics.append(
        F.count_if(
            F.col(c).isNull()
        ).alias(f"null_{c}")
    )

# Diagnostic flags only; do not remove records
metrics.extend([
    F.count_if(
        F.col("value") < 0
    ).alias("negative_measurements"),

    F.count_if(
        F.isnan("value")
    ).alias("nan_measurements"),

    F.count_if(
        F.col("storetime") < F.col("charttime")
    ).alias("storetime_before_charttime")
])

# 4. Perform quality audit
out_quality = prepared_out.agg(*metrics)

display(out_quality)

# 5. Project the official clinical columns
out_silver = prepared_out.select(
    "subject_id",
    "hadm_id",
    "stay_id",
    "caregiver_id",
    "charttime",
    "storetime",
    "itemid",
    "value",
    "valueuom"
)

# 6. Inspect final schema
print("=== OUTPUTEVENTS SILVER SCHEMA ===")
out_silver.printSchema()

print("STEP 2 COMPLETE — NOT PUBLISHED")


# COMMAND ----------


# ============================================================
# ICU SILVER — OUTPUTEVENTS
# STEP 3: REFERENTIAL INTEGRITY VALIDATION
# READ-ONLY — NO DATA MODIFICATION
# ============================================================

from pyspark.sql import functions as F

EXPECTED_ROWS = 5359395

# 1. Validate datatype conversions
quality = out_quality.first().asDict()

assert quality["total_rows"] == EXPECTED_ROWS

cast_failures = {
    k: v for k, v in quality.items()
    if k.startswith("cast_fail_") and v != 0
}

assert not cast_failures, (
    f"Datatype conversion failures: {cast_failures}"
)

print("Datatype conversions: PASSED")

print("Diagnostic findings:")
print("Negative measurements:",
      quality["negative_measurements"])
print("Earlier storetime:",
      quality["storetime_before_charttime"])

# 2. Reference tables
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
    F.col("itemid").alias("ref_itemid"),
    F.col("linksto").alias("ref_linksto")
)

caregiver_ref = spark.table(
    "cdr_mimiciv.silver_v2.caregiver"
).select(
    F.expr("TRY_CAST(caregiver_id AS INT)")
    .alias("ref_caregiver_id")
)

# 3. Join to validated reference tables
linked_out = (
    out_silver.alias("o")
    .join(
        F.broadcast(icu_ref),
        F.col("o.stay_id") == F.col("ref_stay_id"),
        "left"
    )
    .join(
        F.broadcast(item_ref),
        F.col("o.itemid") == F.col("ref_itemid"),
        "left"
    )
    .join(
        F.broadcast(caregiver_ref),
        F.col("o.caregiver_id")
        == F.col("ref_caregiver_id"),
        "left"
    )
)

# 4. Aggregate all relationship checks
out_integrity = linked_out.agg(
    F.count("*").alias("joined_rows"),

    F.count_if(
        F.col("ref_stay_id").isNull()
    ).alias("missing_icu_stays"),

    F.count_if(
        F.col("ref_itemid").isNull()
    ).alias("missing_dictionary_items"),

    F.count_if(
        F.col("o.caregiver_id").isNotNull()
        & F.col("ref_caregiver_id").isNull()
    ).alias("missing_caregivers"),

    F.count_if(
        F.col("ref_stay_id").isNotNull()
        & (
            (F.col("o.subject_id") != F.col("ref_subject_id"))
            |
            (F.col("o.hadm_id") != F.col("ref_hadm_id"))
        )
    ).alias("stay_identity_mismatches"),

    F.count_if(
        F.col("ref_itemid").isNotNull()
        & (F.col("ref_linksto") != "outputevents")
    ).alias("incorrect_item_mapping")
)

display(out_integrity)

print("STEP 3 COMPLETE — NO TABLES MODIFIED")


# COMMAND ----------


# ============================================================
# ICU SILVER — OUTPUTEVENTS
# STEP 4: ITEM-LEVEL DATA QUALITY INVESTIGATION
# READ-ONLY — NO TABLE MODIFICATION
# ============================================================

from pyspark.sql import functions as F

EXPECTED_ROWS = 5359395

# 1. Confirm previous quality and integrity checks
q = out_quality.first().asDict()
r = out_integrity.first().asDict()

assert q["total_rows"] == EXPECTED_ROWS
assert r["joined_rows"] == EXPECTED_ROWS

for key, value in q.items():
    if key.startswith("cast_fail_"):
        assert value == 0, f"{key}: {value}"

for key in [
    "missing_icu_stays",
    "missing_dictionary_items",
    "missing_caregivers",
    "stay_identity_mismatches",
    "incorrect_item_mapping"
]:
    assert r[key] == 0, f"{key}: {r[key]}"

print("Previous validation gates: PASSED")

# 2. Define anomaly conditions
negative = F.col("value") < 0
early = F.col("storetime") < F.col("charttime")

# 3. Aggregate by itemid without modifying records
item_anomalies = (
    out_silver
    .groupBy("itemid")
    .agg(
        F.count("*").alias("total_events"),

        F.count_if(negative).alias(
            "negative_values"
        ),

        F.count_if(early).alias(
            "early_storetime"
        ),

        F.min(
            F.when(negative, F.col("value"))
        ).alias("minimum_negative_value"),

        F.max(
            F.when(
                early,
                (
                    F.col("charttime").cast("long")
                    - F.col("storetime").cast("long")
                ) / F.lit(3600.0)
            )
        ).alias("max_hours_early")
    )
)

# 4. Enrich the small summary with the item dictionary
dictionary = (
    spark.table("cdr_mimiciv.silver_v2.d_items")
    .select("itemid", "label", "unitname")
)


anomaly_report = (
    item_anomalies
    .join(F.broadcast(dictionary), "itemid", "left")
    .filter(
        (F.col("negative_values") > 0)
        | (F.col("early_storetime") > 0)
    )
)


# 5. Show measurement categories with negatives
print("=== TOP NEGATIVE MEASUREMENT CATEGORIES ===")

display(
    anomaly_report
    .filter(F.col("negative_values") > 0)
    .orderBy(F.desc("negative_values"))
    .limit(15)
)

# 6. Show categories with earlier storetime
print("=== TOP TIMESTAMP-ORDER FINDINGS ===")

display(
    anomaly_report
    .filter(F.col("early_storetime") > 0)
    .orderBy(F.desc("early_storetime"))
    .limit(15)
)

print("STEP 4 COMPLETE — NO DATA MODIFIED")


# COMMAND ----------


# ============================================================
# ICU SILVER — OUTPUTEVENTS
# STEP 5: VALIDATION-GATED PUBLICATION
# Preserve clinical values and flag anomalies
# ============================================================

from pyspark.sql import functions as F

TARGET = "cdr_mimiciv.silver_v2.outputevents"
EXPECTED_ROWS = 5359395

# 1. Retrieve validation results
quality = out_quality.first().asDict()
integrity = out_integrity.first().asDict()

assert quality["total_rows"] == EXPECTED_ROWS
assert integrity["joined_rows"] == EXPECTED_ROWS

# 2. Confirm all conversions succeeded
for name, value in quality.items():
    if name.startswith("cast_fail_"):
        assert value == 0, f"{name}: {value}"

# 3. Confirm referential integrity
checks = [
    "missing_icu_stays",
    "missing_dictionary_items",
    "missing_caregivers",
    "stay_identity_mismatches",
    "incorrect_item_mapping"
]

for name in checks:
    assert integrity[name] == 0, (
        f"Integrity failure: {name} = {integrity[name]}"
    )

# 4. Verify intended Silver schema
expected_schema = {
    "subject_id": "int",
    "hadm_id": "int",
    "stay_id": "int",
    "caregiver_id": "int",
    "charttime": "timestamp",
    "storetime": "timestamp",
    "itemid": "int",
    "value": "double",
    "valueuom": "string"
}

actual_schema = {
    f.name: f.dataType.simpleString()
    for f in out_silver.schema.fields
}

assert actual_schema == expected_schema, (
    f"Schema mismatch: {actual_schema}"
)

# 5. Preserve values, flag questionable records
out_final = (
    out_silver
    .withColumn(
        "dq_negative_value",
        F.coalesce(
            F.col("value") < 0,
            F.lit(False)
        )
    )
    .withColumn(
        "dq_storetime_before_charttime",
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

# 6. Protect existing teammate work
if spark.catalog.tableExists(TARGET):
    raise RuntimeError(
        f"STOP: Target already exists: {TARGET}"
    )

print("ALL VALIDATION GATES PASSED")
print("Negative values retained:", quality["negative_measurements"])
print("Earlier storetimes retained:", quality["storetime_before_charttime"])
print("Publishing:", TARGET)

# 7. Create new Delta table; never overwrite
(
    out_final.write
    .format("delta")
    .mode("error")
    .saveAsTable(TARGET)
)

print("SUCCESS: outputevents Silver table created.")
print("STEP 5 COMPLETE — PUBLISHED")


# COMMAND ----------


# ============================================================
# ICU SILVER — OUTPUTEVENTS
# STEP 6: FINAL PERSISTED-TABLE AUDIT
# READ-ONLY — NO TABLE MODIFICATIONS
# ============================================================

from pyspark.sql import functions as F

TARGET = "cdr_mimiciv.silver_v2.outputevents"
EXPECTED_ROWS = 5359395

# 1. Load published Silver table
assert spark.catalog.tableExists(TARGET), (
    f"Missing target: {TARGET}"
)

saved_out = spark.table(TARGET)

# 2. Verify exact schema
expected_schema = {
    "subject_id": "int",
    "hadm_id": "int",
    "stay_id": "int",
    "caregiver_id": "int",
    "charttime": "timestamp",
    "storetime": "timestamp",
    "itemid": "int",
    "value": "double",
    "valueuom": "string",
    "dq_negative_value": "boolean",
    "dq_storetime_before_charttime": "boolean",
    "_silver_processed_at": "timestamp"
}

actual_schema = {
    f.name: f.dataType.simpleString()
    for f in saved_out.schema.fields
}

assert actual_schema == expected_schema, (
    f"Schema mismatch: {actual_schema}"
)

# 3. Aggregate saved-table quality metrics
audit = saved_out.agg(
    F.count("*").alias("silver_rows"),

    F.count_if(
        F.col("stay_id").isNull()
    ).alias("null_stay_ids"),

    F.count_if(
        F.col("itemid").isNull()
    ).alias("null_itemids"),

    F.count_if(
        F.col("_silver_processed_at").isNull()
    ).alias("missing_processing_timestamps"),

    F.count_if(
        F.col("dq_negative_value")
    ).alias("negative_flags"),

    F.count_if(
        F.col("dq_storetime_before_charttime")
    ).alias("timestamp_flags"),

    F.count_if(
        F.col("dq_negative_value").isNull()
        | F.col("dq_storetime_before_charttime").isNull()
    ).alias("null_quality_flags")
).first().asDict()

# 4. Verify counts and flags
assert audit["silver_rows"] == EXPECTED_ROWS
assert audit["null_stay_ids"] == 0
assert audit["null_itemids"] == 0
assert audit["missing_processing_timestamps"] == 0
assert audit["null_quality_flags"] == 0

assert audit["negative_flags"] == 604
assert audit["timestamp_flags"] == 757757

print("Schema, record counts and flags: PASSED")
print("Starting exact record reconciliation...")

# 5. Compare all original clinical fields
# Duplicate records are preserved in comparison
cols = out_silver.columns

missing = (
    out_silver.select(*cols)
    .exceptAll(saved_out.select(*cols))
    .count()
)

unexpected = (
    saved_out.select(*cols)
    .exceptAll(out_silver.select(*cols))
    .count()
)

assert missing == 0, f"Missing records: {missing}"
assert unexpected == 0, f"Unexpected records: {unexpected}"

# 6. Confirm the flags agree with saved values
flag_mismatches = saved_out.filter(
    (F.col("dq_negative_value")
     != (F.col("value") < 0))
    |
    (F.col("dq_storetime_before_charttime")
     != (F.col("storetime") < F.col("charttime")))
).count()

assert flag_mismatches == 0, (
    f"Quality flag mismatches: {flag_mismatches}"
)

print("=== FINAL OUTPUTEVENTS AUDIT ===")
print(audit)
print("Missing from saved:", missing)
print("Unexpected in saved:", unexpected)
print("Flag mismatches:", flag_mismatches)
print("Schema verification: PASSED")
print("FINAL STATUS: PASSED")
