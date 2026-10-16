# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — ICU Ingredient Events (`ingredientevents`)
# MAGIC
# MAGIC **Source:** `cdr_mimiciv.bronze.ingredientevents`  
# MAGIC **Target:** `cdr_mimiciv.silver_v2.ingredientevents`
# MAGIC
# MAGIC **Objective:** Prepare ICU ingredient-level administration records for the Silver layer while preserving patient and ICU stay identifiers, ingredient identifiers, medication and fluid information, quantities, rates, units, event timestamps and Bronze ingestion metadata.
# MAGIC
# MAGIC **Grain:** One source ingredient event record. Multiple ingredient records may belong to the same ICU input or medication administration.
# MAGIC
# MAGIC **Silver rules:** Preserve every Bronze record, source column and clinical value. Standardize data types only when necessary and validated. Do not delete, filter, deduplicate, impute, aggregate or automatically correct clinical information.
# MAGIC
# MAGIC **Data quality:** Inspect missing values, exact duplicates, identifiers, ingredient amounts, rates, timestamps and Bronze–Silver record counts. Document anomalies without modifying or removing records.
# MAGIC
# MAGIC **Publication:** Create a new Silver Delta table only after validation. Never overwrite an existing table.
# MAGIC
# MAGIC **Data quality findings:** Bronze and Silver contain 14,253,480
# MAGIC records with matching schemas. Fifteen records have end timestamps
# MAGIC earlier than start timestamps; these source anomalies are preserved
# MAGIC without correction. All checked timestamps and clinical quantities
# MAGIC are parseable. Duplicate and identifier NULL findings are documented
# MAGIC after validation.

# COMMAND ----------

# Import PySpark functions
from pyspark.sql import functions as F

# COMMAND ----------

# Configure ingredientevents source and target
SOURCE_TABLE = "cdr_mimiciv.bronze.ingredientevents"
TARGET_TABLE = "cdr_mimiciv.silver_v2.ingredientevents"

print("Source:", SOURCE_TABLE)
print("Target:", TARGET_TABLE)

# COMMAND ----------

# Load all ingredient events without modifying source records
bronze_df = spark.table(SOURCE_TABLE)

bronze_df.printSchema()

bronze_count = bronze_df.count()

print("Bronze rows:", bronze_count)
display(bronze_df.limit(20))

# COMMAND ----------

# Inspect every Bronze column, data type and nullability
schema_df = spark.createDataFrame(
    [
        (
            field.name,
            field.dataType.simpleString(),
            field.nullable
        )
        for field in bronze_df.schema.fields
    ],
    ["column_name", "bronze_type", "nullable"]
)

display(schema_df)

# COMMAND ----------

# Preserve all 14,253,480 source records and their original data types
silver_df = bronze_df

print("Silver schema:")
silver_df.printSchema()

# COMMAND ----------

# Count exact duplicate groups without removing any records
duplicate_df = (
    bronze_df
    .groupBy(*bronze_df.columns)
    .count()
    .filter(F.col("count") > 1)
)

duplicate_count = duplicate_df.count()

print("Exact duplicate groups:", duplicate_count)
display(duplicate_df.limit(20))

# COMMAND ----------

# Summarize duplicate occurrences while preserving every source record
duplicate_summary_df = duplicate_df.select(
    F.count("*").alias("duplicate_groups"),
    F.coalesce(F.sum("count"), F.lit(0)).alias("records_in_duplicate_groups"),
    F.coalesce(
        F.sum(F.col("count") - 1),
        F.lit(0)
    ).alias("additional_duplicate_occurrences"),
    F.coalesce(F.max("count"), F.lit(0)).alias("maximum_group_size")
)

display(duplicate_summary_df)

# COMMAND ----------

# Check missing identifiers without excluding incomplete clinical records
identifier_columns = [
    "subject_id",
    "hadm_id",
    "stay_id",
    "itemid",
    "orderid",
    "linkorderid"
]

identifier_quality_df = bronze_df.select(
    *[
        F.sum(F.col(c).isNull().cast("long")).alias(f"null_{c}")
        for c in identifier_columns
    ]
)

identifier_nulls = identifier_quality_df.first().asDict()

display(identifier_quality_df)

# COMMAND ----------

# Inspect missing amounts, rates and units without filling or changing values
clinical_columns = [
    "amount",
    "amountuom",
    "rate",
    "rateuom",
    "originalamount",
    "originalrate"
]

clinical_quality_df = bronze_df.select(
    *[
        F.sum(F.col(c).isNull().cast("long")).alias(f"null_{c}")
        for c in clinical_columns
    ]
)

display(clinical_quality_df)

# COMMAND ----------

# Inspect timestamp strings without casting or correcting source values
timestamp_columns = ["starttime", "endtime", "storetime"]

timestamp_quality_df = bronze_df.select(
    *[
        F.sum(F.col(c).isNull().cast("long")).alias(f"null_{c}")
        for c in timestamp_columns
    ]
)

display(timestamp_quality_df)

# COMMAND ----------

# Parse timestamps for read-only comparison; do not change source columns
timestamp_check_df = bronze_df.select(
    F.try_to_timestamp(F.col("starttime")).alias("parsed_starttime"),
    F.try_to_timestamp(F.col("endtime")).alias("parsed_endtime")
)

timestamp_summary = timestamp_check_df.agg(
    F.sum(
        (
            F.col("parsed_starttime").isNotNull() &
            F.col("parsed_endtime").isNotNull() &
            (F.col("parsed_endtime") < F.col("parsed_starttime"))
        ).cast("long")
    ).alias("reversed_intervals"),
    F.sum(F.col("parsed_starttime").isNull().cast("long")).alias("unparseable_or_null_starttime"),
    F.sum(F.col("parsed_endtime").isNull().cast("long")).alias("unparseable_or_null_endtime")
)

display(timestamp_summary)

# COMMAND ----------

# Check numeric parsing without converting stored clinical quantities
numeric_columns = [
    "amount",
    "rate",
    "originalamount",
    "originalrate"
]

numeric_quality_df = bronze_df.select(
    *[
        F.sum(
            (
                F.col(c).isNotNull() &
                F.expr(f"try_cast(`{c}` AS DOUBLE)").isNull()
            ).cast("long")
        ).alias(f"unparseable_{c}")
        for c in numeric_columns
    ]
)

display(numeric_quality_df)

# COMMAND ----------

# Confirm that all source records and the complete schema are preserved
silver_count = silver_df.count()

print("Bronze rows:", bronze_count)
print("Silver rows:", silver_count)

assert bronze_count == silver_count, (
    "Bronze–Silver row-count mismatch"
)

assert bronze_df.schema == silver_df.schema, (
    "Bronze–Silver schema mismatch"
)

print("Bronze–Silver preservation checks passed.")

# COMMAND ----------

# Check publication safety without modifying any existing table
target_exists = spark.catalog.tableExists(TARGET_TABLE)

print("Target table already exists:", target_exists)
print("Target:", TARGET_TABLE)

# COMMAND ----------

# Compare Bronze and Silver timestamp anomalies without changing records
def count_reversed_intervals(df):
    return df.select(
        F.try_to_timestamp(F.col("starttime")).alias("parsed_starttime"),
        F.try_to_timestamp(F.col("endtime")).alias("parsed_endtime")
    ).filter(
        F.col("parsed_starttime").isNotNull() &
        F.col("parsed_endtime").isNotNull() &
        (F.col("parsed_endtime") < F.col("parsed_starttime"))
    ).count()

bronze_reversed = count_reversed_intervals(bronze_df)
silver_reversed = count_reversed_intervals(silver_df)

print("Bronze reversed intervals:", bronze_reversed)
print("Silver reversed intervals:", silver_reversed)

assert bronze_reversed == silver_reversed, (
    "Bronze–Silver timestamp anomaly counts differ"
)


# COMMAND ----------

# Display all identifier NULL counts, including columns hidden in the screenshot
print("Exact duplicate groups:", duplicate_count)

for column_name, null_count in identifier_nulls.items():
    print(f"{column_name}: {null_count}")

print("Bronze reversed intervals:", bronze_reversed)
print("Silver reversed intervals:", silver_reversed)

# COMMAND ----------

# Publish ingredientevents as a new Delta table without changing records
assert bronze_count == silver_count, (
    "Bronze–Silver row-count mismatch"
)

assert bronze_df.schema == silver_df.schema, (
    "Bronze–Silver schema mismatch"
)

assert duplicate_count == 0, (
    "Unexpected exact duplicate groups; review before publication"
)

assert all(v == 0 for v in identifier_nulls.values()), (
    "Identifier NULL counts require review before publication"
)

assert bronze_reversed == silver_reversed == 15, (
    "Timestamp anomaly counts changed; review before publication"
)

assert not spark.catalog.tableExists(TARGET_TABLE), (
    f"Target already exists; publication stopped: {TARGET_TABLE}"
)

silver_df.write.format("delta").saveAsTable(TARGET_TABLE)

print("Published:", TARGET_TABLE)
print("Source timestamp anomalies preserved:", silver_reversed)

# COMMAND ----------

# Verify the published record count and schema against Bronze
published_df = spark.table(TARGET_TABLE)
published_count = published_df.count()

print("Bronze rows:", bronze_count)
print("Silver rows:", silver_count)
print("Published rows:", published_count)

assert published_count == bronze_count, (
    "Published row-count mismatch"
)

assert published_df.schema == silver_df.schema, (
    "Published schema mismatch"
)

print("Silver ingredientevents table verified successfully.")

# COMMAND ----------

