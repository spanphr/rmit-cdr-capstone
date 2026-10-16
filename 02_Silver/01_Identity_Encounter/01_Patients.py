# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Patients
# MAGIC
# MAGIC ## Source
# MAGIC `cdr_mimiciv.bronze.patients`
# MAGIC
# MAGIC ## Target
# MAGIC `cdr_mimiciv.silver.patients`
# MAGIC
# MAGIC ## Grain
# MAGIC One row represents one unique patient identified by `subject_id`.
# MAGIC
# MAGIC ## Silver objective
# MAGIC Create a reusable and clinically interpretable patient entity while preserving
# MAGIC MIMIC-IV de-identification semantics.
# MAGIC
# MAGIC ## Engineering principles
# MAGIC - Preserve source clinical meaning.
# MAGIC - Do not impute missing clinical values.
# MAGIC - Do not remove records unless a validated rule requires it.
# MAGIC - Preserve MIMIC-IV age de-identification.
# MAGIC - Apply only deterministic, reusable Silver transformations.
# MAGIC - Defer analytical groupings such as age bands to Gold.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "cdr_mimiciv"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver_v2"

# COMMAND ----------

patients_bronze = spark.table(f"{BRONZE}.patients")

display(patients_bronze.limit(20))

# COMMAND ----------

patient_profile = patients_bronze.select(
    F.count("*").alias("total_rows"),
    F.countDistinct("subject_id").alias("unique_subject_ids"),
    F.sum(
        F.when(F.col("subject_id").isNull(), 1).otherwise(0)
    ).alias("null_subject_ids")
)

display(patient_profile)

# COMMAND ----------

duplicate_patients = (
    patients_bronze
    .groupBy("subject_id")
    .count()
    .filter(F.col("count") > 1)
)

print("Duplicate subject_ids:", duplicate_patients.count())

# COMMAND ----------

# check missing values
display(
    patients_bronze.select(
        *[
            F.sum(
                F.when(F.col(c).isNull(), 1).otherwise(0)
            ).alias(c)
            for c in [
                "subject_id",
                "gender",
                "anchor_age",
                "anchor_year",
                "anchor_year_group",
                "dod"
            ]
        ]
    )
)

# COMMAND ----------

# inspect rescued data
rescued_count = (
    patients_bronze
    .filter(F.col("_rescued_data").isNotNull())
    .count()
)

print("Rows with rescued data:", rescued_count)

# COMMAND ----------

display(
    patients_bronze
    .groupBy("gender")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

display(
    patients_bronze.select(
        F.min("anchor_age").alias("min_anchor_age"),
        F.max("anchor_age").alias("max_anchor_age"),
        F.sum(
            F.when(F.col("anchor_age") == 91, 1).otherwise(0)
        ).alias("age_91_count")
    )
)

# COMMAND ----------

patients_silver = (
    patients_bronze
    .select(
        F.col("subject_id")
            .cast("int")
            .alias("subject_id"),

        F.upper(
            F.trim(F.col("gender"))
        ).alias("gender"),

        F.col("anchor_age")
            .cast("int")
            .alias("anchor_age"),

        F.col("anchor_year")
            .cast("int")
            .alias("anchor_year"),

        F.trim(
            F.col("anchor_year_group")
        ).alias("anchor_year_group"),

        F.col("dod")
            .cast("date")
            .alias("dod")
    )

    # Preserve the meaning of MIMIC's age de-identification
    .withColumn(
        "age_deidentified_90_plus",
        F.col("anchor_age") == 91
    )

    # Real technical processing timestamp
    .withColumn(
        "_silver_processed_at",
        F.current_timestamp()
    )
)

# COMMAND ----------

# validate Silver before writing it
display(
    patients_silver.select(
        F.count("*").alias("silver_rows"),
        F.countDistinct("subject_id").alias("unique_subject_ids"),
        F.sum(
            F.when(F.col("subject_id").isNull(), 1).otherwise(0)
        ).alias("null_subject_ids")
    )
)

# COMMAND ----------

# publish it
(
    patients_silver.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{SILVER}.patients")
)

print(f"Created: {SILVER}.patients")

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

print("Old Silver:", spark.table("cdr_mimiciv.silver.patients").count())
print("Silver V2:", spark.table("cdr_mimiciv.silver_v2.patients").count())