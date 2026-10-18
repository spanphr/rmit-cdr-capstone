# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Transfers
# MAGIC
# MAGIC ## Source
# MAGIC `cdr_mimiciv.bronze.transfers`
# MAGIC
# MAGIC ## Target
# MAGIC `cdr_mimiciv.silver_v2.transfers`
# MAGIC
# MAGIC ## Grain
# MAGIC One row represents one patient's stay in one physical location during a hospital
# MAGIC encounter, identified by `transfer_id`.
# MAGIC
# MAGIC ## Key semantics (official MIMIC-IV hosp documentation)
# MAGIC - `subject_id` identifies the patient and links to `patients`.
# MAGIC - `hadm_id` identifies the hospital stay and links to `admissions`. It is nullable.
# MAGIC - `transfer_id` is unique to a patient physical location. `icustays.stay_id` is
# MAGIC   derived from the `transfer_id` of the first ICU location of a stay.
# MAGIC - `eventtype` describes the event: emergency department stay, admission to the
# MAGIC   hospital, intra-hospital transfer, or discharge from the hospital.
# MAGIC - `intime` / `outtime` are when the patient entered and left the location.
# MAGIC
# MAGIC ## Silver objective
# MAGIC Create a reusable, typed patient-location entity while preserving source semantics
# MAGIC and making a small number of questionable records visible without altering or
# MAGIC removing them.
# MAGIC
# MAGIC ## Engineering principles
# MAGIC - Preserve source clinical meaning and every source row.
# MAGIC - Preserve valid source nulls; do not fill them.
# MAGIC - Do not deduplicate, correct timestamps or normalise coded text.
# MAGIC - Use flags for questionable records rather than guessing corrections.
# MAGIC - Apply only deterministic reusable transformations.
# MAGIC - Defer durations and analytical features to Gold.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "cdr_mimiciv"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver_v2"

SOURCE_COLUMNS = [
    "subject_id",
    "hadm_id",
    "transfer_id",
    "eventtype",
    "careunit",
    "intime",
    "outtime"
]

# COMMAND ----------

transfers_bronze = spark.table(f"{BRONZE}.transfers")

transfers_bronze.printSchema()
display(transfers_bronze.limit(20))

# COMMAND ----------

# establish the grain
display(
    transfers_bronze.select(
        F.count("*").alias("total_rows"),
        F.countDistinct("transfer_id").alias("unique_transfer_ids"),
        F.sum(
            F.when(F.col("transfer_id").isNull(), 1).otherwise(0)
        ).alias("null_transfer_ids")
    )
)

# COMMAND ----------

# explicitly check duplicates: by key, and across all source fields
duplicate_transfer_ids = (
    transfers_bronze
    .groupBy("transfer_id")
    .count()
    .filter(F.col("count") > 1)
)

exact_duplicate_rows = (
    transfers_bronze
    .groupBy(*SOURCE_COLUMNS)
    .count()
    .filter(F.col("count") > 1)
)

print("Duplicate transfer_ids:", duplicate_transfer_ids.count())
print("Exact duplicate clinical rows (groups):", exact_duplicate_rows.count())

# COMMAND ----------

# profile missingness
display(
    transfers_bronze.select(
        *[
            F.sum(
                F.when(F.col(c).isNull(), 1).otherwise(0)
            ).alias(c)
            for c in SOURCE_COLUMNS
        ]
    )
)

# COMMAND ----------

# inspect categorical values: eventtype
display(
    transfers_bronze
    .groupBy("eventtype")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

# inspect categorical values: careunit (including case variants of the unknown placeholder)
display(
    transfers_bronze.select(
        F.countDistinct("careunit").alias("distinct_careunit"),
        F.countDistinct(F.lower(F.trim("careunit"))).alias("distinct_careunit_normalised")
    )
)

display(
    transfers_bronze
    .filter(F.lower(F.col("careunit")) == "unknown")
    .groupBy("careunit", "eventtype")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

# missingness and timestamp order by eventtype
display(
    transfers_bronze
    .groupBy("eventtype")
    .agg(
        F.count("*").alias("rows"),
        F.sum(F.when(F.col("hadm_id").isNull(), 1).otherwise(0)).alias("null_hadm_id"),
        F.sum(F.when(F.col("careunit").isNull(), 1).otherwise(0)).alias("null_careunit"),
        F.sum(F.when(F.col("intime").isNull(), 1).otherwise(0)).alias("null_intime"),
        F.sum(F.when(F.col("outtime").isNull(), 1).otherwise(0)).alias("null_outtime"),
        F.sum(
            F.when(F.col("outtime") < F.col("intime"), 1).otherwise(0)
        ).alias("outtime_before_intime"),
        F.sum(
            F.when(F.col("outtime") == F.col("intime"), 1).otherwise(0)
        ).alias("outtime_equals_intime")
    )
    .orderBy(F.desc("rows"))
)

# COMMAND ----------

# validate relationships to the already-published Silver v2 reference tables
admissions_ref = spark.table(f"{SILVER}.admissions").select("hadm_id", "subject_id")
patients_ref = spark.table(f"{SILVER}.patients").select("subject_id")

print("admissions rows / distinct hadm_id:",
      admissions_ref.count(), admissions_ref.select("hadm_id").distinct().count())

orphan_hadm = (
    transfers_bronze
    .filter(F.col("hadm_id").isNotNull())
    .join(admissions_ref.select("hadm_id"), "hadm_id", "left_anti")
    .count()
)

subject_mismatch = (
    transfers_bronze.alias("t")
    .join(admissions_ref.alias("a"), F.col("t.hadm_id") == F.col("a.hadm_id"), "inner")
    .filter(F.col("t.subject_id") != F.col("a.subject_id"))
)

orphan_subject = transfers_bronze.join(patients_ref, "subject_id", "left_anti").count()

print("hadm_id not found in silver_v2.admissions:", orphan_hadm)
print("subject_id not found in silver_v2.patients:", orphan_subject)
print("subject_id differs from silver_v2.admissions for the same hadm_id:", subject_mismatch.count())
display(subject_mismatch.select("t.transfer_id", "t.eventtype", "t.subject_id", "t.hadm_id",
                                F.col("a.subject_id").alias("admissions_subject_id")))

# COMMAND ----------

# observational check only: relationship to silver_v2.icustays
# (findings are recorded in 90_Silver_Cross_Table_Validation, not as row-level flags)
icustays_ref = spark.table(f"{SILVER}.icustays").select(
    "stay_id", "subject_id", "hadm_id", "first_careunit", "intime"
)

icu_link = (
    icustays_ref.alias("i")
    .join(transfers_bronze.alias("t"), F.col("i.stay_id") == F.col("t.transfer_id"), "left")
)

display(
    icu_link.select(
        F.count("*").alias("icustays_rows"),
        F.sum(F.when(F.col("t.transfer_id").isNotNull(), 1).otherwise(0)).alias("stay_id_found_as_transfer_id"),
        F.sum(F.when(F.col("t.transfer_id").isNotNull() & ~F.col("t.hadm_id").eqNullSafe(F.col("i.hadm_id")), 1).otherwise(0)).alias("hadm_id_disagree"),
        F.sum(F.when(F.col("t.transfer_id").isNotNull() & ~F.col("t.subject_id").eqNullSafe(F.col("i.subject_id")), 1).otherwise(0)).alias("subject_id_disagree"),
        F.sum(F.when(F.col("t.transfer_id").isNotNull() & ~F.col("t.intime").eqNullSafe(F.col("i.intime")), 1).otherwise(0)).alias("intime_disagree"),
        F.sum(F.when(F.col("t.transfer_id").isNotNull() & ~F.col("t.careunit").eqNullSafe(F.col("i.first_careunit")), 1).otherwise(0)).alias("first_careunit_disagree")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Profiling Findings
# MAGIC
# MAGIC Profiling of the Bronze `transfers` table (2,413,581 rows) found:
# MAGIC
# MAGIC | Check | Result | Silver treatment |
# MAGIC |---|---:|---|
# MAGIC | Rows / distinct `transfer_id` | 2,413,581 / 2,413,581 | Grain confirmed; no deduplication needed |
# MAGIC | Exact duplicate rows | 0 | None |
# MAGIC | Duplicate `transfer_id` values | 0 | None |
# MAGIC | Null `hadm_id` | 408,977 (all `ED` rows) | Structural; preserve, no flag |
# MAGIC | Null `outtime` on `discharge` | 546,020 | Structural; preserve, no flag |
# MAGIC | Null `outtime` on other events | 103 (94 `admit`, 9 `ED`) | Preserve and flag |
# MAGIC | `outtime` before `intime` | 8 (all `ED`, no `hadm_id`) | Preserve and flag |
# MAGIC | `outtime` equals `intime` | 82 | Preserve; not invalid, no flag |
# MAGIC | `discharge` rows with an `outtime` | 4 | Preserve; no flag |
# MAGIC | `hadm_id` not in `silver_v2.admissions` | 0 | None |
# MAGIC | `subject_id` differs from `silver_v2.admissions` for the same `hadm_id` | 1 | Preserve and flag |
# MAGIC | `subject_id` not in `silver_v2.patients` | 0 | None |
# MAGIC | `eventtype` values | `ED`, `discharge`, `admit`, `transfer` | See note below |
# MAGIC | `careunit` | 46 distinct values; 45 after lower-casing (`UNKNOWN` vs `Unknown`) | Preserve exactly |
# MAGIC
# MAGIC **Documentation note.** The MIMIC documentation lists the emergency department value
# MAGIC as lowercase `ed`; the data contains uppercase `ED`. All four event types are present,
# MAGIC there are no nulls and no other values, so no unexpected-value flag is justified.
# MAGIC The source casing is preserved.
# MAGIC
# MAGIC ## Engineering decisions
# MAGIC
# MAGIC 1. **All 2,413,581 source rows are preserved.** No row is removed, deduplicated or
# MAGIC    corrected.
# MAGIC 2. **Source fields are preserved unchanged:** `subject_id`, `hadm_id`, `transfer_id`,
# MAGIC    `eventtype`, `careunit`, `intime`, `outtime`. `ED`, `UNKNOWN` and `Unknown` are
# MAGIC    not normalised. NULLs are not filled. Timestamps are not corrected.
# MAGIC 3. **No duration field and no analytical features** are added; Gold can derive them.
# MAGIC 4. **Three data-quality flags are added**, each justified by profiling:
# MAGIC    - `dq_invalid_transfer_time` — `outtime` is earlier than `intime`.
# MAGIC    - `dq_missing_outtime` — `outtime` is NULL and `eventtype` is not `discharge`.
# MAGIC      The structural NULL `outtime` on discharge events is deliberately not flagged.
# MAGIC    - `dq_hadm_subject_mismatch` — a non-null `hadm_id` matches an admission in
# MAGIC      `silver_v2.admissions` but belongs to a different `subject_id`.
# MAGIC 5. **Not flagged, by decision:** duplicates, orphan `hadm_id`, null `hadm_id`,
# MAGIC    unexpected `eventtype`, care-unit mismatches, admission-window checks, and any
# MAGIC    ICU/`icustays` mismatch.
# MAGIC 6. **Cross-table ICU findings are recorded, not flagged.** Every `icustays.stay_id`
# MAGIC    (94,458) exists as a `transfer_id`, and `hadm_id`, `subject_id` and `intime` agree
# MAGIC    for all of them. `icustays.first_careunit` differs from the transfer's `careunit`
# MAGIC    for 7,956 stays. This is carried to `90_Silver_Cross_Table_Validation`.
# MAGIC
# MAGIC These flags indicate records that require caution; they do **not automatically mean
# MAGIC that the source record is clinically invalid**. Decisions about excluding anomalous
# MAGIC records from research cohorts are deferred to Gold or the analytical layer.

# COMMAND ----------

# the admissions reference supplies the subject_id used only to evaluate the mismatch flag
admissions_keys = admissions_ref.select(
    "hadm_id",
    F.col("subject_id").alias("_admissions_subject_id")
).distinct()

transfers_silver = (
    transfers_bronze
    .select(*SOURCE_COLUMNS)
    .join(admissions_keys, on="hadm_id", how="left")

    # Flag: location exit occurs before location entry
    .withColumn(
        "dq_invalid_transfer_time",
        F.when(
            F.col("intime").isNotNull()
            & F.col("outtime").isNotNull()
            & (F.col("outtime") < F.col("intime")),
            True
        ).otherwise(False)
    )

    # Flag: no exit time on an event that is not a discharge
    # (NULL outtime on discharge events is structural and is not flagged)
    .withColumn(
        "dq_missing_outtime",
        F.when(
            F.col("outtime").isNull()
            & (F.col("eventtype") != "discharge"),
            True
        ).otherwise(False)
    )

    # Flag: hadm_id belongs to a different patient in silver_v2.admissions
    .withColumn(
        "dq_hadm_subject_mismatch",
        F.when(
            F.col("hadm_id").isNotNull()
            & F.col("_admissions_subject_id").isNotNull()
            & (F.col("subject_id") != F.col("_admissions_subject_id")),
            True
        ).otherwise(False)
    )

    # Technical Silver processing timestamp
    .withColumn("_silver_processed_at", F.current_timestamp())

    .select(
        *SOURCE_COLUMNS,
        "dq_invalid_transfer_time",
        "dq_missing_outtime",
        "dq_hadm_subject_mismatch",
        "_silver_processed_at"
    )
)

transfers_silver.printSchema()
display(transfers_silver.limit(20))

# COMMAND ----------

# pre-publish validation contract (counts established by Bronze profiling)
EXPECTED_ROWS = 2413581
EXPECTED_FLAGS = {
    "dq_invalid_transfer_time": 8,
    "dq_missing_outtime": 103,
    "dq_hadm_subject_mismatch": 1
}

bronze_rows = transfers_bronze.count()
silver_rows = transfers_silver.count()

flag_counts = transfers_silver.select(
    *[
        F.sum(F.when(F.col(c), 1).otherwise(0)).alias(c)
        for c in EXPECTED_FLAGS
    ]
).first().asDict()

print("Bronze rows:", bronze_rows)
print("Silver rows:", silver_rows)
print("Flag counts:", flag_counts)

assert bronze_rows == EXPECTED_ROWS, "Bronze row count changed"
assert silver_rows == bronze_rows, "Silver transformation changed the row count"
assert flag_counts == EXPECTED_FLAGS, "DQ flag counts differ from the profiled expectation"

# COMMAND ----------

(
    transfers_silver.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{SILVER}.transfers")
)

print(f"Created: {SILVER}.transfers")

# COMMAND ----------

# post-publish validation
transfers_published = spark.table(f"{SILVER}.transfers")

published_rows = transfers_published.count()
published_ids = transfers_published.select("transfer_id").distinct().count()

# source rows lost / added: compare every source field in both directions
lost_from_silver = transfers_bronze.select(*SOURCE_COLUMNS).exceptAll(
    transfers_published.select(*SOURCE_COLUMNS)
).count()
added_in_silver = transfers_published.select(*SOURCE_COLUMNS).exceptAll(
    transfers_bronze.select(*SOURCE_COLUMNS)
).count()

published_flags = transfers_published.select(
    *[
        F.sum(F.when(F.col(c), 1).otherwise(0)).alias(c)
        for c in EXPECTED_FLAGS
    ]
).first().asDict()

expected_schema = {
    "subject_id": "int",
    "hadm_id": "int",
    "transfer_id": "int",
    "eventtype": "string",
    "careunit": "string",
    "intime": "timestamp",
    "outtime": "timestamp",
    "dq_invalid_transfer_time": "boolean",
    "dq_missing_outtime": "boolean",
    "dq_hadm_subject_mismatch": "boolean",
    "_silver_processed_at": "timestamp"
}
published_schema = {f.name: f.dataType.simpleString() for f in transfers_published.schema.fields}

print("Published rows:", published_rows)
print("Distinct transfer_id:", published_ids)
print("Source rows lost:", lost_from_silver)
print("Rows not in source:", added_in_silver)
print("Flag counts:", published_flags)
print("Schema matches contract:", published_schema == expected_schema)

assert published_rows == EXPECTED_ROWS
assert published_ids == EXPECTED_ROWS
assert lost_from_silver == 0 and added_in_silver == 0
assert published_flags == EXPECTED_FLAGS
assert list(published_schema.items()) == list(expected_schema.items())

transfers_published.printSchema()
