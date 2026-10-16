# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Admissions
# MAGIC
# MAGIC ## Source
# MAGIC `cdr_mimiciv.bronze.admissions`
# MAGIC
# MAGIC ## Target
# MAGIC `cdr_mimiciv.silver_v2.admissions`
# MAGIC
# MAGIC ## Grain
# MAGIC One row represents one unique hospital admission identified by `hadm_id`.
# MAGIC
# MAGIC ## Silver objective
# MAGIC Create a reusable and clinically interpretable hospital encounter entity while
# MAGIC preserving source semantics and identifying questionable temporal relationships
# MAGIC without silently removing records.
# MAGIC
# MAGIC ## Engineering principles
# MAGIC - Preserve source clinical meaning.
# MAGIC - Preserve valid source nulls.
# MAGIC - Do not automatically remove unusual EHR records.
# MAGIC - Standardise datatypes and text representation.
# MAGIC - Validate encounter identifiers and temporal relationships.
# MAGIC - Use flags for questionable records rather than guessing corrections.
# MAGIC - Apply only deterministic reusable transformations.
# MAGIC - Defer cohort definitions and analytical classifications to Gold.%md
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "cdr_mimiciv"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver_v2"

# COMMAND ----------

admissions_bronze = spark.table(f"{BRONZE}.admissions")

display(admissions_bronze.limit(20))

# COMMAND ----------

# establish the grain
display(
    admissions_bronze.select(
        F.count("*").alias("total_rows"),
        F.countDistinct("hadm_id").alias("unique_hadm_ids"),
        F.countDistinct("subject_id").alias("unique_subject_ids"),
        F.sum(
            F.when(F.col("hadm_id").isNull(), 1).otherwise(0)
        ).alias("null_hadm_ids"),
        F.sum(
            F.when(F.col("subject_id").isNull(), 1).otherwise(0)
        ).alias("null_subject_ids")
    )
)

# COMMAND ----------

# explicitly check duplicate admissions
duplicate_admissions = (
    admissions_bronze
    .groupBy("hadm_id")
    .count()
    .filter(F.col("count") > 1)
)

print("Duplicate hadm_ids:", duplicate_admissions.count())

# COMMAND ----------

# profile missingness
clinical_columns = [
    "subject_id",
    "hadm_id",
    "admittime",
    "dischtime",
    "deathtime",
    "admission_type",
    "admit_provider_id",
    "admission_location",
    "discharge_location",
    "insurance",
    "language",
    "marital_status",
    "race",
    "edregtime",
    "edouttime",
    "hospital_expire_flag"
]

display(
    admissions_bronze.select(
        *[
            F.sum(
                F.when(F.col(c).isNull(), 1).otherwise(0)
            ).alias(c)
            for c in clinical_columns
        ]
    )
)

# COMMAND ----------

# inspect categorical values
display(
    admissions_bronze
    .groupBy("admission_type")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

# now test temporal quality
temporal_profile = admissions_bronze.select(

    F.sum(
        F.when(
            F.col("admittime") > F.col("dischtime"),
            1
        ).otherwise(0)
    ).alias("admit_after_discharge"),

    F.sum(
        F.when(
            F.col("edregtime").isNotNull() &
            F.col("edouttime").isNotNull() &
            (F.col("edregtime") > F.col("edouttime")),
            1
        ).otherwise(0)
    ).alias("edreg_after_edout"),

    F.sum(
        F.when(
            F.col("deathtime").isNotNull() &
            (F.col("deathtime") < F.col("admittime")),
            1
        ).otherwise(0)
    ).alias("death_before_admission"),

    F.sum(
        F.when(
            F.col("deathtime").isNotNull() &
            (F.col("deathtime") > F.col("dischtime")),
            1
        ).otherwise(0)
    ).alias("death_after_discharge"),

    F.sum(
        F.when(
            (F.col("hospital_expire_flag") == 1) &
            F.col("deathtime").isNull(),
            1
        ).otherwise(0)
    ).alias("expired_but_no_deathtime")

)

display(temporal_profile)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Temporal Data Quality Findings
# MAGIC
# MAGIC Temporal profiling of the Bronze `admissions` table identified a small number of
# MAGIC records with unusual timestamp relationships:
# MAGIC
# MAGIC | Check | Records | Silver treatment |
# MAGIC |---|---:|---|
# MAGIC | Admission time occurs after discharge time | 175 | Preserve the record and flag it |
# MAGIC | ED registration time occurs after ED discharge time | 3 | Preserve the record and flag it |
# MAGIC | Recorded death time occurs before admission time | 26 | Preserve the record and flag it |
# MAGIC | Recorded death time occurs after discharge time | 745 | Preserve the record and flag it |
# MAGIC | `hospital_expire_flag = 1` but `deathtime` is missing | 11 | Preserve the record and flag the inconsistency |
# MAGIC
# MAGIC ### Engineering decision
# MAGIC
# MAGIC These records will **not be deleted or automatically corrected in the Silver layer**.
# MAGIC
# MAGIC MIMIC-IV represents real-world EHR data and can contain unusual timestamp
# MAGIC relationships caused by clinical workflows, administrative processes,
# MAGIC documentation timing, de-identification, or source-system inconsistencies.
# MAGIC
# MAGIC The purpose of Silver is to make these issues visible without changing the
# MAGIC clinical meaning of the source data. Therefore, the original timestamps are
# MAGIC preserved and explicit data-quality flags are added.
# MAGIC
# MAGIC This follows a non-destructive data-quality approach:
# MAGIC
# MAGIC `preserve source value → identify anomaly → add DQ flag → allow downstream users to decide`
# MAGIC
# MAGIC No timestamp will be guessed, replaced, swapped, or imputed.
# MAGIC
# MAGIC ### Silver quality fields
# MAGIC
# MAGIC The following reusable quality indicators will be created:
# MAGIC
# MAGIC - `dq_invalid_admission_time` — discharge occurs before admission.
# MAGIC - `dq_invalid_ed_time` — ED discharge occurs before ED registration.
# MAGIC - `dq_death_before_admission` — recorded death occurs before hospital admission.
# MAGIC - `dq_death_after_discharge` — recorded death occurs after hospital discharge.
# MAGIC - `dq_expire_deathtime_mismatch` — the hospital mortality flag and recorded
# MAGIC   death time are inconsistent.
# MAGIC
# MAGIC These flags indicate records that require caution; they do **not automatically
# MAGIC mean that the source record is clinically invalid**.
# MAGIC
# MAGIC Project-specific decisions about excluding anomalous records from research
# MAGIC cohorts will be deferred to the Gold or analytical layer.

# COMMAND ----------

from pyspark.sql import functions as F

admissions_silver = (
    admissions_bronze

    # Flag: admission occurs after discharge
    .withColumn(
        "dq_invalid_admission_time",
        F.when(
            F.col("admittime").isNotNull()
            & F.col("dischtime").isNotNull()
            & (F.col("admittime") > F.col("dischtime")),
            True
        ).otherwise(False)
    )

    # Flag: ED registration occurs after ED discharge
    .withColumn(
        "dq_invalid_ed_time",
        F.when(
            F.col("edregtime").isNotNull()
            & F.col("edouttime").isNotNull()
            & (F.col("edregtime") > F.col("edouttime")),
            True
        ).otherwise(False)
    )

    # Flag: recorded death occurs before hospital admission
    .withColumn(
        "dq_death_before_admission",
        F.when(
            F.col("deathtime").isNotNull()
            & F.col("admittime").isNotNull()
            & (F.col("deathtime") < F.col("admittime")),
            True
        ).otherwise(False)
    )

    # Flag: recorded death occurs after discharge
    .withColumn(
        "dq_death_after_discharge",
        F.when(
            F.col("deathtime").isNotNull()
            & F.col("dischtime").isNotNull()
            & (F.col("deathtime") > F.col("dischtime")),
            True
        ).otherwise(False)
    )

    # Flag: hospital_expire_flag and deathtime disagree
    .withColumn(
        "dq_expire_deathtime_mismatch",
        F.when(
            (F.col("hospital_expire_flag") == 1)
            & F.col("deathtime").isNull(),
            True
        )
        .when(
            (F.col("hospital_expire_flag") == 0)
            & F.col("deathtime").isNotNull(),
            True
        )
        .otherwise(False)
    )

    # Technical Silver processing timestamp
    .withColumn(
        "_silver_processed_at",
        F.current_timestamp()
    )
)

# COMMAND ----------

display(admissions_silver.limit(20))

# COMMAND ----------

display(
    admissions_silver.select(
        F.sum(
            F.when(F.col("dq_invalid_admission_time"), 1).otherwise(0)
        ).alias("dq_invalid_admission_time"),

        F.sum(
            F.when(F.col("dq_invalid_ed_time"), 1).otherwise(0)
        ).alias("dq_invalid_ed_time"),

        F.sum(
            F.when(F.col("dq_death_before_admission"), 1).otherwise(0)
        ).alias("dq_death_before_admission"),

        F.sum(
            F.when(F.col("dq_death_after_discharge"), 1).otherwise(0)
        ).alias("dq_death_after_discharge"),

        F.sum(
            F.when(F.col("dq_expire_deathtime_mismatch"), 1).otherwise(0)
        ).alias("dq_expire_deathtime_mismatch")
    )
)

# COMMAND ----------

print("Bronze rows:", admissions_bronze.count())
print("Silver rows:", admissions_silver.count())

# COMMAND ----------

from pyspark.sql import functions as F

admissions_silver = (
    admissions_bronze

    # Keep only the actual admissions fields
    .select(
        "subject_id",
        "hadm_id",
        "admittime",
        "dischtime",
        "deathtime",
        "admission_type",
        "admit_provider_id",
        "admission_location",
        "discharge_location",
        "insurance",
        "language",
        "marital_status",
        "race",
        "edregtime",
        "edouttime",
        "hospital_expire_flag"
    )

    # Admission after discharge
    .withColumn(
        "dq_invalid_admission_time",
        F.when(
            F.col("admittime").isNotNull()
            & F.col("dischtime").isNotNull()
            & (F.col("admittime") > F.col("dischtime")),
            True
        ).otherwise(False)
    )

    # ED registration after ED discharge
    .withColumn(
        "dq_invalid_ed_time",
        F.when(
            F.col("edregtime").isNotNull()
            & F.col("edouttime").isNotNull()
            & (F.col("edregtime") > F.col("edouttime")),
            True
        ).otherwise(False)
    )

    # Death before admission
    .withColumn(
        "dq_death_before_admission",
        F.when(
            F.col("deathtime").isNotNull()
            & F.col("admittime").isNotNull()
            & (F.col("deathtime") < F.col("admittime")),
            True
        ).otherwise(False)
    )

    # Death after discharge
    .withColumn(
        "dq_death_after_discharge",
        F.when(
            F.col("deathtime").isNotNull()
            & F.col("dischtime").isNotNull()
            & (F.col("deathtime") > F.col("dischtime")),
            True
        ).otherwise(False)
    )

    # Mortality flag / deathtime disagreement
    .withColumn(
        "dq_expire_deathtime_mismatch",
        F.when(
            (F.col("hospital_expire_flag") == 1)
            & F.col("deathtime").isNull(),
            True
        )
        .when(
            (F.col("hospital_expire_flag") == 0)
            & F.col("deathtime").isNotNull(),
            True
        )
        .otherwise(False)
    )

    # Genuine Silver processing metadata
    .withColumn(
        "_silver_processed_at",
        F.current_timestamp()
    )
)

# COMMAND ----------

display(admissions_silver.limit(20))

# COMMAND ----------

(
    admissions_silver.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{SILVER}.admissions")
)

print(f"Created: {SILVER}.admissions")

# COMMAND ----------

# MAGIC %md
# MAGIC null marital status to unknown
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM cdr_mimiciv.bronze.admissions
# MAGIC WHERE marital_status IS NULL