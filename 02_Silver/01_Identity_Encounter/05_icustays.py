# Databricks notebook source

# ============================================================
# ICU SILVER LAYER — ICUSTAYS
# STEP 1: BRONZE SOURCE INSPECTION
# ============================================================

from pyspark.sql import functions as F

SOURCE = "cdr_mimiciv.bronze.icustays"
TARGET = "cdr_mimiciv.silver_v2.icustays"

# Load existing Bronze data
icu_bronze = spark.table(SOURCE)

# Inspect source structure
print("SOURCE:", SOURCE)
icu_bronze.printSchema()

# Check row count and ICU stay identifiers
profile = icu_bronze.agg(
    F.count("*").alias("total_rows"),
    F.countDistinct("stay_id").alias("unique_stay_ids"),
    F.sum(
        F.when(F.col("stay_id").isNull(), 1).otherwise(0)
    ).alias("null_stay_ids")
)

display(profile)

# Preview actual ICU stay records
display(icu_bronze.limit(5))


# COMMAND ----------


# ============================================================
# STEP 2: ICU DATA TYPE AND QUALITY VALIDATION
# Read-only: no tables are modified
# ============================================================

quality_check = spark.sql("""
SELECT
    COUNT(*) AS total_rows,

    COUNT_IF(
        subject_id IS NOT NULL
        AND TRY_CAST(TRIM(subject_id) AS BIGINT) IS NULL
    ) AS invalid_subject_ids,

    COUNT_IF(
        hadm_id IS NOT NULL
        AND TRY_CAST(TRIM(hadm_id) AS BIGINT) IS NULL
    ) AS invalid_hadm_ids,

    COUNT_IF(
        stay_id IS NOT NULL
        AND TRY_CAST(TRIM(stay_id) AS BIGINT) IS NULL
    ) AS invalid_stay_ids,

    COUNT_IF(
        intime IS NOT NULL
        AND TRY_CAST(TRIM(intime) AS TIMESTAMP) IS NULL
    ) AS invalid_intime,

    COUNT_IF(
        outtime IS NOT NULL
        AND TRY_CAST(TRIM(outtime) AS TIMESTAMP) IS NULL
    ) AS invalid_outtime,

    COUNT_IF(
        los IS NOT NULL
        AND TRY_CAST(TRIM(los) AS DOUBLE) IS NULL
    ) AS invalid_los,

    COUNT_IF(
        TRY_CAST(intime AS TIMESTAMP) >
        TRY_CAST(outtime AS TIMESTAMP)
    ) AS reversed_timestamps,

    COUNT_IF(
        TRY_CAST(los AS DOUBLE) < 0
    ) AS negative_los

FROM cdr_mimiciv.bronze.icustays
""")

display(quality_check)


# COMMAND ----------


# ============================================================
# ICU SILVER — ICUSTAYS
# STEP 3: TYPED TRANSFORMATION + VALIDATION CONTRACT
# ============================================================

from pyspark.sql import functions as F

# Official MIMIC source-aligned datatypes
cast_types = {
    "subject_id": "INT",
    "hadm_id": "INT",
    "stay_id": "INT",
    "intime": "TIMESTAMP",
    "outtime": "TIMESTAMP",
    "los": "DOUBLE"
}

# Retain original values temporarily for audit comparisons
prepared = icu_bronze.select(
    *[
        F.col(c).alias(f"_raw_{c}")
        for c in cast_types
    ],
    *[
        F.expr(
            f"TRY_CAST(TRIM(`{c}`) AS {dtype})"
        ).alias(c)
        for c, dtype in cast_types.items()
    ],
    F.col("first_careunit"),
    F.col("last_careunit")
)

# One grouped validation pass
checks = [
    F.count("*").alias("total_rows"),
    F.countDistinct("stay_id").alias("unique_stays"),
    F.count_if(F.col("stay_id").isNull())
        .alias("null_stays")
]

# Detect any non-NULL source value lost during conversion
for c in cast_types:
    checks.append(
        F.count_if(
            F.col(f"_raw_{c}").isNotNull()
            & F.col(c).isNull()
        ).alias(f"cast_fail_{c}")
    )

# Check timeline consistency
checks.extend([
    F.count_if(
        F.col("intime") > F.col("outtime")
    ).alias("reversed_times"),

    F.count_if(
        F.col("los") < 0
    ).alias("negative_los"),

    # Compare recorded LOS against actual timestamp duration
    # Flag differences exceeding 1 minute
    F.count_if(
        F.col("los").isNotNull()
        & F.col("intime").isNotNull()
        & F.col("outtime").isNotNull()
        & (
            F.abs(
                F.col("los")
                - F.expr(
                    "timestampdiff(SECOND, intime, outtime)"
                ) / F.lit(86400.0)
            ) > F.lit(1.0 / 1440.0)
        )
    ).alias("los_mismatch_over_1min")
])

validation = prepared.agg(*checks)

display(validation)

# Final Silver column projection — no write yet
icu_silver = prepared.select(
    "subject_id",
    "hadm_id",
    "stay_id",
    "first_careunit",
    "last_careunit",
    "intime",
    "outtime",
    "los"
)

print("Silver candidate prepared — NOT published")
icu_silver.printSchema()


# COMMAND ----------


# ============================================================
# STEP 4: CROSS-TABLE REFERENTIAL VALIDATION
# Read-only — no tables modified
# ============================================================

from pyspark.sql import functions as F

# Reference tables already published by the team
patients_ref = (
    spark.table("cdr_mimiciv.silver_v2.patients")
    .select(
        F.col("subject_id").alias("p_subject_id")
    )
    .distinct()
)

admissions_ref = (
    spark.table("cdr_mimiciv.silver_v2.admissions")
    .select(
        F.col("hadm_id").alias("a_hadm_id"),
        F.col("subject_id").alias("a_subject_id")
    )
    .distinct()
)

# Check patient and hospital admission relationships
linked = (
    icu_silver.alias("i")
    .join(
        patients_ref.alias("p"),
        F.col("i.subject_id") == F.col("p.p_subject_id"),
        "left"
    )
    .join(
        admissions_ref.alias("a"),
        F.col("i.hadm_id") == F.col("a.a_hadm_id"),
        "left"
    )
)

integrity = linked.agg(
    F.count("*").alias("joined_rows"),

    F.count_if(
        F.col("i.subject_id").isNull()
    ).alias("null_subject_ids"),

    F.count_if(
        F.col("i.hadm_id").isNull()
    ).alias("null_hadm_ids"),

    F.count_if(
        F.col("i.subject_id").isNotNull() &
        F.col("p.p_subject_id").isNull()
    ).alias("missing_patients"),

    F.count_if(
        F.col("i.hadm_id").isNotNull() &
        F.col("a.a_hadm_id").isNull()
    ).alias("missing_admissions"),

    F.count_if(
        F.col("a.a_hadm_id").isNotNull() &
        (~F.col("i.subject_id").eqNullSafe(
            F.col("a.a_subject_id")
        ))
    ).alias("patient_admission_mismatches")
)

display(integrity)


# COMMAND ----------


# ============================================================
# STEP 5: VALIDATION-GATED SILVER PUBLICATION
# Target: cdr_mimiciv.silver_v2.icustays
# ============================================================

from pyspark.sql import functions as F

TARGET = "cdr_mimiciv.silver_v2.icustays"
EXPECTED_ROWS = 94458

# 1. Retrieve previously defined validation results
quality = validation.first().asDict()
links = integrity.first().asDict()

# 2. Mandatory quality conditions
assert quality["total_rows"] == EXPECTED_ROWS
assert quality["unique_stays"] == EXPECTED_ROWS
assert quality["null_stays"] == 0

for name, value in quality.items():
    if name.startswith("cast_fail_"):
        assert value == 0, f"{name}: {value}"

for name in [
    "reversed_times",
    "negative_los",
    "los_mismatch_over_1min"
]:
    assert quality[name] == 0, f"{name}: {quality[name]}"

# 3. Referential integrity conditions
assert links["joined_rows"] == EXPECTED_ROWS

for name in [
    "null_subject_ids",
    "null_hadm_ids",
    "missing_patients",
    "missing_admissions",
    "patient_admission_mismatches"
]:
    assert links[name] == 0, f"{name}: {links[name]}"

# 4. Protect existing tables
if spark.catalog.tableExists(TARGET):
    raise RuntimeError(
        f"STOP: {TARGET} already exists. No changes made."
    )

# 5. Add processing timestamp
icu_final = icu_silver.withColumn(
    "_silver_processed_at",
    F.current_timestamp()
)

print("All validation gates passed.")
print("Publishing:", TARGET)

# 6. Create new Delta table (never overwrite)
(
    icu_final.write
    .format("delta")
    .mode("error")
    .saveAsTable(TARGET)
)

print("SUCCESS: Silver ICU stays table created.")


# COMMAND ----------


# ============================================================
# STEP 6: FINAL SILVER TABLE VERIFICATION
# Read-only — no changes to any tables
# ============================================================

from pyspark.sql import functions as F

TARGET = "cdr_mimiciv.silver_v2.icustays"

saved = spark.table(TARGET)

# 1. Validate published schema
expected_types = {
    "subject_id": "int",
    "hadm_id": "int",
    "stay_id": "int",
    "first_careunit": "string",
    "last_careunit": "string",
    "intime": "timestamp",
    "outtime": "timestamp",
    "los": "double",
    "_silver_processed_at": "timestamp"
}

actual_types = {
    field.name: field.dataType.simpleString()
    for field in saved.schema.fields
}

assert actual_types == expected_types, (
    f"Schema mismatch: {actual_types}"
)

# 2. Check saved records and processing metadata
result = saved.agg(
    F.count("*").alias("silver_rows"),
    F.countDistinct("stay_id").alias("unique_stays"),
    F.count_if(F.col("stay_id").isNull())
        .alias("null_stays"),
    F.count_if(F.col("_silver_processed_at").isNull())
        .alias("missing_processing_timestamps")
).first().asDict()

# 3. Compare all 8 clinical columns in both directions
# exceptAll preserves duplicate-row semantics
cols = icu_silver.columns

missing_from_saved = (
    icu_silver.select(*cols)
    .exceptAll(saved.select(*cols))
    .count()
)

unexpected_in_saved = (
    saved.select(*cols)
    .exceptAll(icu_silver.select(*cols))
    .count()
)

# 4. Final quality gate
assert result["silver_rows"] == 94458
assert result["unique_stays"] == 94458
assert result["null_stays"] == 0
assert result["missing_processing_timestamps"] == 0
assert missing_from_saved == 0
assert unexpected_in_saved == 0

print("=== FINAL ICUSTAYS AUDIT ===")
print(result)
print("Missing from saved:", missing_from_saved)
print("Unexpected in saved:", unexpected_in_saved)
print("FINAL STATUS: PASSED")
